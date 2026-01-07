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

package gcp

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/klog/v2"
)

const name = "github.com/transparency-dev/tessera/storage/gcp"

var (
	meter  = otel.Meter(name)
	tracer = otel.Tracer(name)
)

var (
	errorTypeKey  = attribute.Key("error.type")
	treeSizeKey   = attribute.Key("tessera.treeSize")
	fromSizeKey   = attribute.Key("tessera.fromSize")
	numEntriesKey = attribute.Key("tessera.numEntries")
	objectPathKey = attribute.Key("tessera.objectPath")
	opNameKey     = attribute.Key("op_name")

	publishCount metric.Int64Counter
	opsHistogram metric.Int64Histogram

	// Custom histogram buckets as we're interested in low-millis upto low-seconds.
	histogramBuckets = []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 4000, 5000, 6000, 8000, 10000}
)

func init() {
	var err error

	opsHistogram, err = meter.Int64Histogram(
		"tessera.appender.ops.duration",
		metric.WithDescription("Duration of calls to storage operations"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(histogramBuckets...))
	if err != nil {
		klog.Exitf("Failed to create opsHistogram metric: %v", err)
	}

	publishCount, err = meter.Int64Counter(
		"tessera.appender.checkpoint.publication.counter",
		metric.WithDescription("Number of checkpoint publication attempts by result"),
		metric.WithUnit("{call}"))
	if err != nil {
		klog.Exitf("Failed to create checkpoint publication counter metric: %v", err)
	}
}
