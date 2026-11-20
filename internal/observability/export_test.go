package observability

import (
	"context"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type receiver struct {
	collector.UnimplementedTraceServiceServer
	requests chan *collector.ExportTraceServiceRequest
}

func (r *receiver) Export(_ context.Context, in *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	r.requests <- in
	return &collector.ExportTraceServiceResponse{}, nil
}

func TestSetupExportsOTLPOverGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &receiver{requests: make(chan *collector.ExportTraceServiceRequest, 1)}
	server := grpc.NewServer()
	collector.RegisterTraceServiceServer(server, r)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://"+lis.Addr().String())
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	shutdown, err := Setup(ctx, "notification-worker")
	if err != nil {
		t.Fatal(err)
	}
	_, end := Delivery(ctx, "", "N42")
	end()
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-r.requests:
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					if span.Name == "notification.process" {
						return
					}
				}
			}
		}
		t.Fatal("collector received data without the worker span")
	case <-ctx.Done():
		t.Fatal("collector did not receive trace", ctx.Err())
	}
}
