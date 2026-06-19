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

package loadtest

import (
	"context"
	"errors"
	"sync"
)

var ErrRetry = errors.New("retry")

// NewRoundRobinReader creates a new LogReader which will spread read requests over the passed-in LogReaders.
func NewRoundRobinReader(r []LogReader) LogReader {
	return &roundRobinReader{r: r}
}

// NewRoundRobinWriter creates a new LeafWriter which will spread write requests over the passed-in LeafWriters.
func NewRoundRobinWriter(w []LeafWriter) LeafWriter {
	return (&roundRobinLeafWriter{ws: w}).Write
}

// roundRobinReader ensures that read requests are sent to all configured readers
// using a round-robin strategy.
type roundRobinReader struct {
	sync.Mutex
	idx int
	r   []LogReader
}

func (rr *roundRobinReader) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	r := rr.next()
	return r.ReadCheckpoint(ctx)
}

func (rr *roundRobinReader) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	r := rr.next()
	return r.ReadTile(ctx, l, i, p)
}

func (rr *roundRobinReader) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	r := rr.next()
	return r.ReadEntryBundle(ctx, i, p)
}

func (rr *roundRobinReader) next() LogReader {
	rr.Lock()
	defer rr.Unlock()

	r := rr.r[rr.idx]
	rr.idx = (rr.idx + 1) % len(rr.r)

	return r
}

// roundRobinLeafWriter ensures that write requests are sent to all configured
// LeafWriters using a round-robin strategy.
type roundRobinLeafWriter struct {
	sync.Mutex
	idx int
	ws  []LeafWriter
}

func (rr *roundRobinLeafWriter) Write(ctx context.Context, newLeaf []byte) (uint64, error) {
	w := rr.next()
	return w(ctx, newLeaf)
}

func (rr *roundRobinLeafWriter) next() LeafWriter {
	rr.Lock()
	defer rr.Unlock()

	w := rr.ws[rr.idx]
	rr.idx = (rr.idx + 1) % len(rr.ws)

	return w
}
