// Copyright 2026 Google LLC. All Rights Reserved.
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

// Package client_test is for client tests which have deps which would otherwise cause an import cycle.
package client_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/testonly"
	"golang.org/x/sync/errgroup"
)

func TestProofBuilderIsThreadsafe(t *testing.T) {
	ctx := t.Context()

	// First, create a test log large enough to have a bunch of tiles/entrybundles.
	treeSize := uint64(2468)

	l, done := testonly.NewTestLog(t, tessera.NewAppendOptions().WithBatching(1024, 100*time.Millisecond).WithCheckpointInterval(100*time.Millisecond))
	defer func() {
		_ = done(ctx)
	}()

	var f tessera.IndexFuture
	t.Logf("Growing log to %d entries", treeSize)
	for i := range treeSize {
		f = l.Appender.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "Entry %d", i)))
	}
	t.Logf("Awaiting integration")
	if _, _, err := tessera.NewPublicationAwaiter(ctx, l.LogReader.ReadCheckpoint, 100*time.Millisecond).Await(ctx, f); err != nil {
		t.Fatalf("Failed while awaiting: %v", err)
	}

	// Create a single ProofBuilder which we'll use below, shared between a number of concurrent goroutines.
	pb, err := client.NewProofBuilder(ctx, treeSize, l.LogReader.ReadTile)
	if err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}

	// Finally, spin up a bunch of goroutines asking for consistency and inclusion proofs from the same single ProofBuilder instance.
	t.Logf("Exercising proof builder")
	concurrency := 512

	var wg errgroup.Group
	for range concurrency {
		wg.Go(func() error {
			leafIndex := uint64(1 + rand.IntN(int(treeSize-1)))
			if _, err := pb.InclusionProof(ctx, leafIndex); err != nil {
				return fmt.Errorf("failure calling InclusionProof(%d): %v", leafIndex, err)
			}

			smaller := uint64(1 + rand.IntN(int(treeSize-1)))
			if _, err := pb.ConsistencyProof(ctx, smaller, treeSize); err != nil {
				return fmt.Errorf("failure calling ConsistencyProof(%d, %d): %v", smaller, treeSize, err)
			}
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		t.Errorf("Concurrent request failed: %v", err)
	}
}
