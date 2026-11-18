package tracedemo

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Providers struct{ API, Checks, Sender, Reports *sdktrace.TracerProvider }

func Provider(name string, exporter sdktrace.SpanExporter, sampler sdktrace.Sampler, synchronous bool) *sdktrace.TracerProvider {
	options := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sampler), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", name)))}
	if synchronous {
		options = append(options, sdktrace.WithSyncer(exporter))
	} else {
		options = append(options, sdktrace.WithBatcher(exporter))
	}
	return sdktrace.NewTracerProvider(options...)
}
func (p Providers) Shutdown(ctx context.Context) error {
	for _, provider := range []*sdktrace.TracerProvider{p.API, p.Checks, p.Sender, p.Reports} {
		if err := provider.Shutdown(ctx); err != nil {
			return err
		}
	}
	return nil
}
func logSpan(ctx context.Context, service, operation, auditID, eventID, outcome string, started time.Time) {
	sc := trace.SpanContextFromContext(ctx)
	slog.Info("audit_operation", "service", service, "operation", operation, "correlation_id", "corr-"+auditID, "audit_id", auditID, "event_id", eventID, "outcome", outcome, "duration_ms", float64(time.Since(started))/float64(time.Millisecond), "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
}
