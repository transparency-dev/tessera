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

package storage

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const name = "github.com/transparency-dev/tessera/storage"

var (
	tracer = otel.Tracer(name)
	meter  = otel.Meter(name)

	// Custom histogram buckets for batch sizes
	batchSizeHistogramBuckets = []float64{0, 64, 256, 1024, 4096, 8192, 16384, 32768}

	// Custom histogram buckets as we're interested in low-millis upto low-seconds.
	latencyHistogramBuckets = []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 4000, 5000, 6000, 8000, 10000}
)

var (
	fromSizeKey   = attribute.Key("tessera.fromSize")
	numEntriesKey = attribute.Key("tessera.numEntries")

	treeSizeKey = attribute.Key("tessera.treeSize")
	indexKey    = attribute.Key("tessera.index")
	levelKey    = attribute.Key("tessera.level")
)
