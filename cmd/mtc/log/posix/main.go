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

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	mtc "github.com/transparency-dev/tessera/cmd/mtc/log"
	"github.com/transparency-dev/tessera/storage/posix"
)

var (
	slogLevel = flag.Int("slog_level", 0, "The cut-off threshold for structured logging.")

	listenAddr        = flag.String("listen_addr", ":8080", "The address to listen on for HTTP requests.")
	readHeaderTimeout = flag.Duration("read_header_timeout", 5*time.Second, "The maximum duration for reading request headers.")
	maxHeaderBytes    = flag.Int("max_header_bytes", http.DefaultMaxHeaderBytes, "Maximum number of bytes the server will read parsing request headers.")
	writeTimeout      = flag.Duration("write_timeout", 0, "The maximum duration before timing out writes of the response.")

	storageDir                = flag.String("storage_dir", "", "Path to root of log storage.")
	privKeyFile               = flag.String("private_key", "", "Location of private key file. If unset, uses the contents of the LOG_PRIVATE_KEY environment variable.")
	checkpointInterval        = flag.Duration("checkpoint_interval", 1500*time.Millisecond, "Interval between publishing checkpoints when the log has grown")
	batchMaxSize              = flag.Uint("batch_max_size", tessera.DefaultBatchMaxSize, "Maximum number of entries to process in a single sequencing batch.")
	batchMaxAge               = flag.Duration("batch_max_age", tessera.DefaultBatchMaxAge, "Maximum age of entries in a single sequencing batch.")
	pushbackMaxOutstanding    = flag.Uint("pushback_max_outstanding", tessera.DefaultPushbackMaxOutstanding, "Maximum number of in-flight add requests - i.e. the number of entries with sequence numbers assigned, but which are not yet integrated into the log.")
	garbageCollectionInterval = flag.Duration("garbage_collection_interval", 10*time.Second, "Interval between scans to remove obsolete partial tiles and entry bundles. Set to 0 to disable.")

	clientHTTPTimeout        = flag.Duration("client_http_timeout", 5*time.Second, "Timeout for outgoing HTTP requests")
	clientHTTPMaxIdle        = flag.Int("client_http_max_idle", 20, "Maximum number of idle HTTP connections for outgoing requests.")
	clientHTTPMaxIdlePerHost = flag.Int("client_http_max_idle_per_host", 10, "Maximum number of idle HTTP connections per host for outgoing requests.")
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))
	ctx := context.Background()

	appender, shutdown := newAppenderFromFlags(ctx)
	mtcLog := mtc.NewMTCLog(ctx, appender)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /add-tbs", func(w http.ResponseWriter, r *http.Request) {
		// TODO: parse request
		// TODO write response
		if _, _, err := mtcLog.AddTBS(ctx, mtc.TBSCertificateLogEntry{}); err != nil {
			slog.ErrorContext(ctx, "Failed to add entry to MTC log", slog.Any("error", err))
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	})
	mux.HandleFunc("GET /proof-to-landmark", func(w http.ResponseWriter, r *http.Request) {
		// TODO parse request
		// TODO write response
		if _, err := mtcLog.ProofToLandmark(ctx, 0); err != nil {
			slog.ErrorContext(ctx, "Failed to fetch inclusion proof to landmark", slog.Any("error", err))
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	})

	server := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: *readHeaderTimeout,
		WriteTimeout:      *writeTimeout,
		MaxHeaderBytes:    *maxHeaderBytes,
	}

	slog.InfoContext(ctx, "Starting Tessera MTC log service", slog.String("addr", *listenAddr))
	if err := server.ListenAndServe(); err != nil {
		if err := shutdown(ctx); err != nil {
			slog.ErrorContext(ctx, "Failed to cleanly shutdown after ListenAndServe", slog.Any("error", err))
			os.Exit(1)
		}
		slog.ErrorContext(ctx, "ListenAndServe failed", slog.Any("error", err))
		os.Exit(1)
	}
}

// Read log private key from file or environment variable
func mustCreateSigner() note.SubtreeSigner {
	var privKey string
	var err error
	if len(*privKeyFile) > 0 {
		privKey, err = getKeyFile(*privKeyFile)
		if err != nil {
			slog.ErrorContext(context.Background(), "Unable to get private key", slog.Any("error", err))
			os.Exit(1)
		}
	} else {
		privKey = os.Getenv("LOG_PRIVATE_KEY")
		if len(privKey) == 0 {
			slog.ErrorContext(context.Background(), "Supply private key file path using --private_key or set LOG_PRIVATE_KEY environment variable")
			os.Exit(1)
		}
	}
	s, err := note.NewMLDSASigner(privKey)
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to instantiate signer", slog.Any("error", err))
		os.Exit(1)
	}
	return s
}

func getKeyFile(path string) (string, error) {
	k, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read key file: %w", err)
	}
	return string(k), nil
}

func newAppenderFromFlags(ctx context.Context) (*tessera.Appender, func(ctx context.Context) error) {
	if *storageDir == "" {
		slog.ErrorContext(ctx, "flag --storage_dir is required")
		os.Exit(1)
	}

	signer := mustCreateSigner()

	// TODO: Add WithOrigin option.
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithCheckpointInterval(*checkpointInterval).
		WithBatching(*batchMaxSize, *batchMaxAge).
		WithPushback(*pushbackMaxOutstanding).
		WithGarbageCollectionInterval(*garbageCollectionInterval)

	cfg := posix.Config{
		Path: *storageDir,
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        *clientHTTPMaxIdle,
				MaxIdleConnsPerHost: *clientHTTPMaxIdlePerHost,
				DisableKeepAlives:   false,
			},
			Timeout: *clientHTTPTimeout,
		},
	}

	driver, err := posix.New(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "failed to initialize POSIX Tessera storage", slog.Any("error", err))
		os.Exit(1)
	}
	appender, shutdown, _, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		slog.ErrorContext(ctx, "failed to initialize Tessera appender", slog.Any("error", err))
		os.Exit(1)
	}
	return appender, shutdown
}
