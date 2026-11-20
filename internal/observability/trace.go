// Package observability traces the lifecycle of a notification across asynchronous
// boundaries. Payloads, recipient addresses and provider URLs are never span attributes.
package observability

import (
	"context"
	"net/http"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const scope = "notification-system"

// Setup installs process-wide tracing during startup. With no endpoint the service
// still creates valid W3C trace IDs usable for log correlation.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

// HTTP propagates remote trace context without wrapping the response writer;
// streaming handlers retain their flush and hijack capabilities.
func HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer(scope).Start(ctx, "notification.api", trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method)))
		defer span.End()
		w.Header().Set("Traceparent", Traceparent(ctx))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Traceparent serializes only the W3C traceparent to persist alongside the job.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// Delivery restores the context stored by the API while preserving the worker's
// cancellation and deadlines for this attempt.
func Delivery(ctx context.Context, parent, id string) (context.Context, func()) {
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": parent})
	ctx, span := otel.Tracer(scope).Start(ctx, "notification.process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("notification.id", id)))
	return ctx, func() { span.End() }
}

// ProviderRequest injects the provider HTTP span into an independent request header map.
func ProviderRequest(r *http.Request) (*http.Request, func()) {
	ctx, span := otel.Tracer(scope).Start(r.Context(), "notification.provider", trace.WithSpanKind(trace.SpanKindClient))
	r = r.Clone(ctx)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(r.Header))
	return r, func() { span.End() }
}
