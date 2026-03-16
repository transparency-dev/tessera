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

// Package integration_test contains some integration tests which are intended to
// serve as a way of checking that example binary works as intended,
// as well as providing a simple example of how to run and use it.
package integration_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"log/slog"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/sync/errgroup"
)

var (
	runIntegrationTest = flag.Bool("run_integration_test", false, "If true, the integration tests in this package will not be skipped")
	logURL             = flag.String("log_url", "http://localhost:2024", "Log storage read root URL, e.g. https://log.server/and/path/")
	writeLogURL        = flag.String("write_log_url", "http://localhost:2024", "Log storage write root URL, e.g. https://log.server/and/path/")
	logPublicKey       = flag.String("log_public_key", "", "The log's public key value for checkpoint note verification")
	testEntrySize      = flag.Int("test_entry_size", 1024, "The number of entries to be tested in the live log integration")

	noteVerifier note.Verifier

	logReadBaseURL     *url.URL
	logReadCP          client.CheckpointFetcherFunc
	logReadTile        client.TileFetcherFunc
	logReadEntryBundle client.EntryBundleFetcherFunc

	hc = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
		},
		Timeout: 60 * time.Second,
	}
)

func TestMain(m *testing.M) {
	flag.Parse()

	if !*runIntegrationTest {
		slog.WarnContext(context.Background(), "example binary integration tests are skipped")
		return
	}

	var err error
	noteVerifier, err = note.NewVerifier(*logPublicKey)
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create new verifier", slog.Any("error", err))
		os.Exit(1)
	}

	logReadBaseURL, err = url.Parse(*logURL)
	if err != nil {
		slog.ErrorContext(context.Background(), "failed to parse logURL", slog.Any("error", err))
		os.Exit(1)
	}

	switch logReadBaseURL.Scheme {
	case "http", "https":
		hf, err := client.NewHTTPFetcher(logReadBaseURL, nil)
		if err != nil {
			slog.ErrorContext(context.Background(), "NewHTTPFetcher", slog.Any("error", err))
			os.Exit(1)
		}
		logReadCP = hf.ReadCheckpoint
		logReadTile = hf.ReadTile
		logReadEntryBundle = hf.ReadEntryBundle
	case "file":
		ff := client.FileFetcher{Root: logReadBaseURL.Path}
		logReadCP = ff.ReadCheckpoint
		logReadTile = ff.ReadTile
		logReadEntryBundle = ff.ReadEntryBundle
	default:
		slog.ErrorContext(context.Background(), "unsupported url scheme", slog.String("scheme", logReadBaseURL.Scheme))
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestLiveLogIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var entryIndexMap sync.Map

	// Step 1 - Get checkpoint initial size for increment validation.
	lst, err := client.NewLogStateTracker(ctx, logReadTile, nil, noteVerifier, noteVerifier.Name(), client.UnilateralConsensus(logReadCP))
	if err != nil {
		t.Fatalf("client.NewLogStateTracker: %v", err)
	}
	checkpointInitSize := lst.Latest().Size

	// Step 2 - Add entries and get new checkpoints. The entry data comes from the int loop ranging from 0 to the test entry size - 1.
	addEntriesURL, err := url.JoinPath(*writeLogURL, "add")
	if err != nil {
		t.Errorf("url.JoinPath: %v", err)
	}
	entryWriter := entryWriter{
		addURL: addEntriesURL,
	}
	var miMu sync.Mutex
	var maxIndex uint64
	errG := errgroup.Group{}
	for i := range *testEntrySize {
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
	if gotIncrease < uint64(*testEntrySize) {
		t.Logf("checkpoint size increase (%d) is < %d, entries may have been deduplicated.", gotIncrease, *testEntrySize)
	}

	// Step 3 - Loop through the entry data index map to verify leaves and inclusion proofs.
	entryIndexMap.Range(func(k, v any) bool {
		data := k.(int)
		index := v.(uint64)

		// Step 4.1 - Get entry bundles to read back what was written, check leaves are correct.
		entryBundle, err := client.GetEntryBundle(ctx, logReadEntryBundle, index/layout.EntryBundleWidth, lst.Latest().Size)
		if err != nil {
			t.Fatalf("client.GetEntryBundle: %v", err)
		}

		got, want := entryBundle.Entries[index%layout.EntryBundleWidth], fmt.Appendf(nil, "%d", data)
		if !bytes.Equal(got, want) {
			t.Errorf("Entry bundle (index: %d) got %v want %v", index, got, want)
		}

		// Step 4.2 - Test inclusion proofs.
		pb, err := client.NewProofBuilder(ctx, lst.Latest().Size, logReadTile)
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
	addURL string
}

func (w *entryWriter) add(ctx context.Context, entry []byte) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.addURL, bytes.NewReader(entry))
	if err != nil {
		return 0, err
	}
	resp, err := hc.Do(req)
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
