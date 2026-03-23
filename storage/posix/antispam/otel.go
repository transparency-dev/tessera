// Copyright 2025 The Tessera authors. All Rights Reserved.
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

package badger

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const name = "github.com/transparency-dev/tessera/storage/posix/antispam"

var (
	tracer = otel.Tracer(name)
	meter  = otel.Meter(name)
)

var (
	followFromKey = attribute.Key("tessera.followFrom")
	pushbackKey   = attribute.Key("tessera.pushback")
	gcStatusKey   = attribute.Key("status")
	hitKey        = attribute.Key("hit")
)

var (
	gcCounter                 metric.Int64Counter
	gcConsecutiveSuccessCount metric.Int64Histogram
	gcDuration                metric.Float64Histogram
	lookupCounter             metric.Int64Counter
	lookupDuration            metric.Float64Histogram
	followTxnEntriesCounter   metric.Int64Histogram
	followTxnDuration         metric.Float64Histogram

	// Histogram buckets for operations (latency in ms)
	histogramBuckets = []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 4000, 5000, 6000, 8000, 10000}

	// GC buckets for longer running operations.
	gcDurationBuckets = []float64{10, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 30000, 60000}
)

func generateBatchBuckets(max int) []float64 {
	bases := []float64{1, 2, 5}
	buckets := []float64{}
	for multiplier := 1.0; ; multiplier *= 10 {
		for _, b := range bases {
			v := b * multiplier
			if v >= float64(max) {
				buckets = append(buckets, float64(max))
				return buckets
			}
			buckets = append(buckets, v)
		}
	}
}

func init() {
	var err error

	gcCounter, err = meter.Int64Counter(
		"tessera.antispam.badger.gc.count",
		metric.WithDescription("Number of BadgerDB Garbage Collection runs"),
		metric.WithUnit("{run}"))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.gc.count metric", slog.Any("error", err))
		os.Exit(1)
	}

	gcConsecutiveSuccessCount, err = meter.Int64Histogram(
		"tessera.antispam.badger.gc.consecutive_success_count",
		metric.WithDescription("Number of consecutive successful GC runs"),
		metric.WithUnit("{run}"),
		metric.WithExplicitBucketBoundaries(generateBatchBuckets(500)...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.gc.consecutive_success_count metric", slog.Any("error", err))
		os.Exit(1)
	}

	gcDuration, err = meter.Float64Histogram(
		"tessera.antispam.badger.gc.duration",
		metric.WithDescription("Duration of BadgerDB Garbage Collection runs"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(gcDurationBuckets...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.gc.duration metric", slog.Any("error", err))
		os.Exit(1)
	}

	lookupCounter, err = meter.Int64Counter(
		"tessera.antispam.badger.index.count",
		metric.WithDescription("Number of BadgerDB identity lookups"),
		metric.WithUnit("{lookup}"))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.index.count metric", slog.Any("error", err))
		os.Exit(1)
	}

	lookupDuration, err = meter.Float64Histogram(
		"tessera.antispam.badger.index.duration",
		metric.WithDescription("Duration of BadgerDB identity lookups"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(histogramBuckets...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.index.duration metric", slog.Any("error", err))
		os.Exit(1)
	}

	followTxnEntriesCounter, err = meter.Int64Histogram(
		"tessera.antispam.badger.follow_txn.entries.count",
		metric.WithDescription("Number of entries processed by BadgerDB antispam Follow transactions"),
		metric.WithUnit("{entry}"),
		metric.WithExplicitBucketBoundaries(generateBatchBuckets(int(DefaultMaxBatchSize))...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.follow_txn.entries.count metric", slog.Any("error", err))
		os.Exit(1)
	}

	followTxnDuration, err = meter.Float64Histogram(
		"tessera.antispam.badger.FollowTxn.duration",
		metric.WithDescription("Duration of BadgerDB Follow transactions"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(histogramBuckets...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create tessera.antispam.badger.FollowTxn.duration metric", slog.Any("error", err))
		os.Exit(1)
	}
}
