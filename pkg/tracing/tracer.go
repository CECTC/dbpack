/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	olteResource "go.opentelemetry.io/otel/sdk/resource"
	traceSDK "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/third_party/parser/ast"
)

const (
	serviceName = "dbpack"
)

type TracingExporter string

const (
	ConsoleExporter TracingExporter = "console"
	JaegerExporter  TracingExporter = "jaeger"
	ZipkinExporter  TracingExporter = "zipkin"
	OltpExporter    TracingExporter = "oltp"
)

type TracerController struct {
	provider *traceSDK.TracerProvider
}

func createJaegerExporter(endpoint string) (traceSDK.SpanExporter, error) {
	return jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
}

// NewTracer create tracer controller, currently only support jaeger.
func NewTracer(version string, jaegerEndpoint string) (*TracerController, error) {
	resource, err := olteResource.Merge(
		olteResource.Default(),
		olteResource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(version),
		),
	)
	if err != nil {
		return nil, err
	}

	exporter, err := createJaegerExporter(jaegerEndpoint)
	if err != nil {
		return nil, err
	}

	provider := traceSDK.NewTracerProvider(
		traceSDK.WithBatcher(exporter),
		traceSDK.WithResource(resource),
	)

	otel.SetTracerProvider(provider)

	tracerCtl := &TracerController{provider: provider}
	return tracerCtl, nil
}

func (p TracerController) Shutdown(ctx context.Context) error {
	return p.provider.Shutdown(ctx)
}

func GetTraceSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	return otel.Tracer(serviceName).Start(ctx, spanName)
}

func BuildContextFromSQLHint(ctx context.Context, stmt ast.Node) context.Context {
	var traceParent string
	var flag bool
	switch node := stmt.(type) {
	case *ast.SelectStmt:
		flag, traceParent = misc.HasTraceParentHint(node.TableHints)
	case *ast.InsertStmt:
		flag, traceParent = misc.HasTraceParentHint(node.TableHints)
	case *ast.UpdateStmt:
		flag, traceParent = misc.HasTraceParentHint(node.TableHints)
	case *ast.DeleteStmt:
		flag, traceParent = misc.HasTraceParentHint(node.TableHints)
	}

	if flag {
		tc := propagation.TraceContext{}
		carrier := propagation.MapCarrier{}
		carrier.Set(TraceParentHeader, traceParent)
		return tc.Extract(ctx, carrier)
	}
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

func RecordErrorSpan(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
