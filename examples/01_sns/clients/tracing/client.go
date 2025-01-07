package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	otelTrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const OffEndpoint = "off"

func InitGrpcTracer(ctx context.Context, endpoint, serviceName string) (func(), error) {
	if strings.ToLower(endpoint) == OffEndpoint {
		otel.SetTracerProvider(trace.NewTracerProvider()) // No options mean no tracing/exporting
		return func() {}, nil
	}

	conn, err := grpc.NewClient(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("otlptracegrpc.New: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)))
	if err != nil {
		return nil, fmt.Errorf("resource.New: %w", err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	)
	if tp == nil {
		return nil, fmt.Errorf("trace.NewTracerProvider returned nil")
	}

	otel.SetTracerProvider(tp)

	return func() {
		if err := tp.Shutdown(ctx); err != nil {
			slog.Error("tp.Shutdown", "error", err)
		}
	}, nil
}

func StartSpan(ctx context.Context, tracerName, spanName string) (context.Context, otelTrace.Span, func(err error)) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, spanName)

	// Return a function to finish the span
	finishFunc := func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}

	return ctx, span, finishFunc
}
