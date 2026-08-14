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

// Package integration contains some integration tests which are intended to
// serve as a way of checking that example binary works as intended,
// as well as providing a simple example of how to run and use it.
package log

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"log/slog"

	fNote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera/client"

	"golang.org/x/mod/sumdb/note"
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
	noteVerifier, err = fNote.NewVerifier(*logPublicKey)
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
	LogIntegrationTest(t, LogIntegrationTestConfig{
		LogReadTile:        logReadTile,
		LogReadCP:          logReadCP,
		LogReadEntryBundle: logReadEntryBundle,
		WriteLogURL:        *writeLogURL,
		LogVerifier:        noteVerifier,
		HTTPClient:         hc,
		EntrySize:          uint64(*testEntrySize),
	})
}
