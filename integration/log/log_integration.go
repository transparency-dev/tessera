// Copyright 2026 The Tessera Authors. All Rights Reserved.
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

// Package log contains integration tests for a log.
package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/sync/errgroup"
)

type LogIntegrationTestConfig struct {
	LogReadTile        client.TileFetcherFunc
	LogReadCP          client.CheckpointFetcherFunc
	LogReadEntryBundle client.EntryBundleFetcherFunc
	WriteLogURL        string
	LogVerifier        note.Verifier
	HTTPClient         *http.Client

	EntrySize uint64
}

// LogIntegrationTest performs the live log integration test.
func LogIntegrationTest(t *testing.T, cfg LogIntegrationTestConfig) {
	t.Helper()
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	var entryIndexMap sync.Map

	// Step 1 - Get checkpoint initial size for increment validation.
	lst, err := client.NewLogStateTracker(ctx, cfg.LogReadTile, nil, cfg.LogVerifier, cfg.LogVerifier.Name(), client.UnilateralConsensus(cfg.LogReadCP))
	if err != nil {
		t.Fatalf("client.NewLogStateTracker: %v", err)
	}
	checkpointInitSize := lst.Latest().Size

	// Step 2 - Add entries and get new checkpoints. The entry data comes from the int loop ranging from 0 to the test entry size - 1.
	addEntriesURL, err := url.JoinPath(cfg.WriteLogURL, "add")
	if err != nil {
		t.Errorf("url.JoinPath: %v", err)
	}
	entryWriter := entryWriter{
		hc:     cfg.HTTPClient,
		addURL: addEntriesURL,
	}
	var miMu sync.Mutex
	var maxIndex uint64
	errG := errgroup.Group{}
	for i := range cfg.EntrySize {
		errG.Go(func() error {
			index, err := entryWriter.add(ctx, fmt.Appendf(nil, "%d", i))
			if err != nil {
				return fmt.Errorf("entryWriter.add(%d): %v", i, err)
			}
			entryIndexMap.Store(i, index)
			miMu.Lock()
			defer miMu.Unlock()
			if maxIndex < index {
				maxIndex = index
			}
			return nil
		})
	}
	if err := errG.Wait(); err != nil {
		t.Fatalf("addEntry: %v", err)
	}
	// All entries are queued. Wait for a checkpoint committing to maxIndex.
	for size := lst.Latest().Size; size <= maxIndex; {
		if _, _, _, err := lst.Update(ctx); err != nil {
			t.Errorf("lst.Update: %v", err)
		}
		size = lst.Latest().Size
		time.Sleep(50 * time.Millisecond)
	}

	gotIncrease := lst.Latest().Size - checkpointInitSize
	if gotIncrease < cfg.EntrySize {
		t.Logf("checkpoint size increase (%d) is < %d, entries may have been deduplicated.", gotIncrease, cfg.EntrySize)
	}

	// Step 3 - Loop through the entry data index map to verify leaves and inclusion proofs.
	entryIndexMap.Range(func(k, v any) bool {
		data := k.(uint64)
		index := v.(uint64)

		// Step 4.1 - Get entry bundles to read back what was written, check leaves are correct.
		entryBundle, err := client.GetEntryBundle(ctx, cfg.LogReadEntryBundle, index/layout.EntryBundleWidth, lst.Latest().Size)
		if err != nil {
			t.Fatalf("client.GetEntryBundle: %v", err)
		}

		got, want := entryBundle.Entries[index%layout.EntryBundleWidth], fmt.Appendf(nil, "%d", data)
		if !bytes.Equal(got, want) {
			t.Errorf("Entry bundle (index: %d) got %v want %v", index, got, want)
		}

		// Step 4.2 - Test inclusion proofs.
		pb, err := client.NewProofBuilder(ctx, lst.Latest().Size, cfg.LogReadTile)
		if err != nil {
			t.Errorf("client.NewProofBuilder: %v", err)
		}
		ip, err := pb.InclusionProof(ctx, index)
		if err != nil {
			t.Errorf("pb.InclusionProof: %v", err)
		}
		leafHash := rfc6962.DefaultHasher.HashLeaf(fmt.Append(nil, data))
		if err := proof.VerifyInclusion(rfc6962.DefaultHasher, index, lst.Latest().Size, leafHash, ip, lst.Latest().Hash); err != nil {
			t.Errorf("proof.VerifyInclusion: %v", err)
		}

		return true
	})
}

type entryWriter struct {
	hc     *http.Client
	addURL string
}

func (w *entryWriter) add(ctx context.Context, entry []byte) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.addURL, bytes.NewReader(entry))
	if err != nil {
		return 0, err
	}
	resp, err := w.hc.Do(req)
	if err != nil {
		return 0, err
	}
	body, err := io.ReadAll(resp.Body)
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.WarnContext(ctx, "resp.Body.Close", slog.Any("error", err))
		}
	}()
	if err != nil {
		return 0, fmt.Errorf("failed to read response from %s: %w", w.addURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("code: %s, path: %s, body: %s", resp.Status, w.addURL, strings.TrimSpace(string(body)))
	}
	index, err := strconv.ParseUint(string(body), 10, 64)
	if err != nil {
		return 0, err
	}

	return index, nil
}
