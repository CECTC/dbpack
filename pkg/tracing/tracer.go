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
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSDK "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	trace "go.opentelemetry.io/otel/trace"
)

const (
	serviceName = "dbpack"
)

type ExporterType string

const (
	ConsoleExporter ExporterType = "console"
	JaegerExporter  ExporterType = "jaeger"
	ZipkinExporter  ExporterType = "zipkin"
	OltpExporter    ExporterType = "oltp"
)

type TracerController struct {
	tracingResource *resource.Resource
	file            *os.File
	tracingProvider *traceSDK.TracerProvider
	exporterType    string
}

func NewTracer(version string, exporterType string) (*TracerController, error) {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
			semconv.ServiceVersionKey.String(version),
			attribute.String("environment", "demo"),
		),
	)
	if err != nil {
		return nil, err
	}
	tracerCtl := &TracerController{tracingResource: r}
	err = tracerCtl.setupExporter(ExporterType(exporterType))
	if err != nil {
		return nil, err
	}
	return tracerCtl, nil
}

func (p TracerController) Shutdown(ctx context.Context) error {
	return p.tracingProvider.Shutdown(ctx)
}

func (p TracerController) setupExporter(exporterType ExporterType) error {
	var exporter traceSDK.SpanExporter
	var err error
	switch exporterType {
	case ConsoleExporter:
		exporter, err = stdouttrace.New(
			// Use human readable output.
			stdouttrace.WithPrettyPrint(),
			// Do not print timestamps for the demo.
			stdouttrace.WithoutTimestamps(),
		)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown opentelemetry exporter %s", exporterType)
	}
	p.tracingProvider = traceSDK.NewTracerProvider(
		traceSDK.WithBatcher(exporter),
		traceSDK.WithResource(p.tracingResource),
	)
	otel.SetTracerProvider(p.tracingProvider)
	return nil
}

func GetTraceSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	return otel.Tracer(serviceName).Start(ctx, spanName)
}

func RecordErrorSpan(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
