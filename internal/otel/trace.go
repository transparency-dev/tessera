// Copyright 2024 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otel

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TraceErr executes logic that returns only an error and traces it, recording any errors on the span.
func TraceErr(ctx context.Context, name string, tracer trace.Tracer, fn func(context.Context, trace.Span) error) error {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()

	err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// Trace executes logic that returns (T, error) and traces it, recording any errors on the span.
func Trace[T any](ctx context.Context, name string, tracer trace.Tracer, fn func(context.Context, trace.Span) (T, error)) (T, error) {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()

	res, err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return res, err
}

// Trace2 executes logic that returns (T1, T2, error) and traces it, recording any errors on the span.
func Trace2[T1, T2 any](ctx context.Context, name string, tracer trace.Tracer, fn func(context.Context, trace.Span) (T1, T2, error)) (T1, T2, error) {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()

	res1, res2, err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return res1, res2, err
}

// Trace3 executes logic that returns (T1, T2, T3, error) and traces it, recording any errors on the span.
func Trace3[T1, T2, T3 any](ctx context.Context, name string, tracer trace.Tracer, fn func(context.Context, trace.Span) (T1, T2, T3, error)) (T1, T2, T3, error) {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()

	res1, res2, res3, err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return res1, res2, res3, err
}

// Trace4 executes logic that returns (T1, T2, T3, T4, error) and traces it, recording any errors on the span.
func Trace4[T1, T2, T3, T4 any](ctx context.Context, name string, tracer trace.Tracer, fn func(context.Context, trace.Span) (T1, T2, T3, T4, error)) (T1, T2, T3, T4, error) {
	ctx, span := tracer.Start(ctx, name)
	defer span.End()

	res1, res2, res3, res4, err := fn(ctx, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return res1, res2, res3, res4, err
}
