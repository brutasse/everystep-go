// Package traces creates OpenTelemetry spans for workflow runs.
//
// everystep only creates spans on the global tracer "everystep"; the host
// owns the TracerProvider and its exporters. Without one installed the
// global tracer is a no-op, so spans are simply absent.
//
// Span status is always set explicitly via MarkOK and MarkError, never
// inferred from an unwound span. Because OTel propagation is
// context.Context-based, branch goroutines inherit the fork span through
// the child context with no capture/attach dance.
package traces

import (
	"context"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracer     = otel.Tracer("everystep")
	noopTracer = trace.NewNoopTracerProvider().Tracer("everystep")
)

// WorkflowSpan starts the span for one run.
func WorkflowSpan(ctx context.Context, name string, runID uuid.UUID) (context.Context, trace.Span) {
	return tracer.Start(ctx, name,
		trace.WithAttributes(attribute.String("everystep.workflow.id", runID.String())))
}

// StepSpan starts the span for one executed step. Replayed (served) steps
// get no span.
func StepSpan(ctx context.Context, name, stepID string, active bool) (context.Context, trace.Span) {
	t := tracer
	if !active {
		t = noopTracer
	}
	return t.Start(ctx, name,
		trace.WithAttributes(attribute.String("everystep.step.id", stepID)))
}

// ForkSpan starts the span for one parallel fork.
func ForkSpan(ctx context.Context, forkID string, active bool) (context.Context, trace.Span) {
	t := tracer
	if !active {
		t = noopTracer
	}
	return t.Start(ctx, "parallel",
		trace.WithAttributes(attribute.String("everystep.parallel.id", forkID)))
}

// MarkOK sets the span status to OK explicitly.
func MarkOK(span trace.Span) {
	span.SetStatus(codes.Ok, "")
}

// MarkError records the exception on the span and sets the status to
// ERROR.
func MarkError(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
