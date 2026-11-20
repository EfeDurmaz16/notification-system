package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContinuesAcrossDurableQueue(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})

	var stored string
	api := HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stored = Traceparent(r.Context())
	}))
	const parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	req := httptest.NewRequest(http.MethodPost, "/v1/notifications", nil)
	req.Header.Set("Traceparent", parent)
	res := httptest.NewRecorder()
	api.ServeHTTP(res, req)
	if stored == "" || res.Header().Get("Traceparent") != stored {
		t.Fatalf("lost API traceparent %q", stored)
	}
	ctx, endDelivery := Delivery(context.Background(), stored, "N42")
	out, endProvider := ProviderRequest(httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil))
	endProvider()
	endDelivery()

	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("got %d spans; want API, provider, delivery", len(spans))
	}
	apiSpan, providerSpan, deliverySpan := spans[0], spans[1], spans[2]
	if deliverySpan.Parent().SpanID() != apiSpan.SpanContext().SpanID() ||
		providerSpan.Parent().SpanID() != deliverySpan.SpanContext().SpanID() {
		t.Fatal("API -> delivery -> provider parent chain is broken")
	}
	received := propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(out.Header))
	remote := trace.SpanContextFromContext(received)
	if remote.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" ||
		remote.SpanID() != providerSpan.SpanContext().SpanID() {
		t.Fatal("trace context missing at remote provider")
	}
}
