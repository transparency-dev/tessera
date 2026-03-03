// Copyright 2026 The Tessera authors. All Rights Reserved.
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

// Package logger provides structured logging utilities.
package logger

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// GCPContextHandler is an slog.Handler that extracts OpenTelemetry tracing
// information from the context and adds it to the log record in the format
// expected by GCP Cloud Logging, allowing logs to be correlated with traces.
type GCPContextHandler struct {
	slog.Handler
	projectID string
}

// NewGCPContextHandler wraps the provided slog.Handler. It injects GCP Cloud Logging
// compatible trace fields extracted from the context if a valid span is present.
func NewGCPContextHandler(h slog.Handler, projectID string) *GCPContextHandler {
	if projectID == "" {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	return &GCPContextHandler{Handler: h, projectID: projectID}
}

// Handle adds the trace ID, span ID, and sampled flag to the record attributes.
func (h *GCPContextHandler) Handle(ctx context.Context, r slog.Record) error {
	span := trace.SpanContextFromContext(ctx)
	if span.IsValid() {
		// GCP Cloud Logging expects the trace ID to be formatted as:
		// projects/[PROJECT_ID]/traces/[TRACE_ID]
		// https://docs.cloud.google.com/logging/docs/structured-logging#structured_logging_special_fields
		tracePath := span.TraceID().String()
		if h.projectID != "" {
			tracePath = "projects/" + h.projectID + "/traces/" + tracePath
		}

		r.AddAttrs(
			slog.String("logging.googleapis.com/trace", tracePath),
			slog.String("logging.googleapis.com/spanId", span.SpanID().String()),
			slog.Bool("logging.googleapis.com/trace_sampled", span.IsSampled()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs returns a new handler with the given attributes, preserving the GCP handling.
func (h *GCPContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &GCPContextHandler{Handler: h.Handler.WithAttrs(attrs), projectID: h.projectID}
}

// WithGroup returns a new handler with the given group name, preserving the GCP handling.
func (h *GCPContextHandler) WithGroup(name string) slog.Handler {
	return &GCPContextHandler{Handler: h.Handler.WithGroup(name), projectID: h.projectID}
}
