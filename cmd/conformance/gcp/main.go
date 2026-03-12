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

// gcp is a simple personality allowing to run conformance/compliance/performance tests and showing how to use the Tessera GCP storage implmentation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"log/slog"
	"os"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/internal/logger"
	"github.com/transparency-dev/tessera/storage/gcp"
	gcp_as "github.com/transparency-dev/tessera/storage/gcp/antispam"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var (
	bucket             = flag.String("bucket", "", "Bucket to use for storing log")
	listen             = flag.String("listen", ":2024", "Address:port to listen on")
	spanner            = flag.String("spanner", "", "Spanner resource URI ('projects/.../...')")
	signer             = flag.String("signer", "", "Note signer to use to sign checkpoints")
	persistentAntispam = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable GCP-based persistent antispam storage")
	traceFraction      = flag.Float64("trace_fraction", 0.01, "Fraction of open-telemetry span traces to sample")
	projectID          = flag.String("project", "", "GCP Project ID for Cloud Logging traces (optional)")
	additionalSigners  = []string{}
	slogLevel          = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
)

func init() {
	flag.Func("additional_signer", "Additional note signer for checkpoints, may be specified multiple times", func(s string) error {
		additionalSigners = append(additionalSigners, s)
		return nil
	})
}

func main() {
	flag.Parse()
	ctx := context.Background()

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})
	slog.SetDefault(slog.New(logger.NewGCPContextHandler(handler, *projectID)))

	shutdownOTel := initOTel(ctx, *traceFraction)
	defer shutdownOTel(ctx)

	s, a := signerFromFlags()

	// Create our Tessera storage backend:
	gcpCfg := storageConfigFromFlags()
	driver, err := gcp.New(ctx, gcpCfg)
	if err != nil {
		slog.Error("Failed to create new GCP storage", slog.Any("error", err))
		os.Exit(1)
	}

	var antispam tessera.Antispam
	// Persistent antispam is currently experimental, so there's no terraform or documentation yet!
	if *persistentAntispam {
		asOpts := gcp_as.AntispamOpts{} // Use defaults
		antispam, err = gcp_as.NewAntispam(ctx, fmt.Sprintf("%s-antispam", *spanner), asOpts)
		if err != nil {
			slog.Error("Failed to create new GCP antispam storage", slog.Any("error", err))
			os.Exit(1)
		}
	}

	appender, shutdown, _, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s, a...).
		WithCheckpointInterval(10*time.Second).
		WithBatching(512, 300*time.Millisecond).
		WithPushback(10*4096).
		WithAntispam(tessera.DefaultAntispamInMemorySize, antispam))
	if err != nil {
		slog.Error("Failed to append", slog.Any("error", err))
		os.Exit(1)
	}

	// Expose a HTTP handler for the conformance test writes.
	// This should accept arbitrary bytes POSTed to /add, and return an ascii
	// decimal representation of the index assigned to the entry.
	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		f := appender.Add(r.Context(), tessera.NewEntry(b))
		idx, err := f()
		if err != nil {
			if errors.Is(err, tessera.ErrPushback) {
				w.Header().Add("Retry-After", "1")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		// Write out the assigned index
		_, _ = fmt.Fprintf(w, "%d", idx.Index)
	})

	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:              *listen,
		Handler:           h2c.NewHandler(http.DefaultServeMux, h2s),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := http2.ConfigureServer(h1s, h2s); err != nil {
		slog.Error("http2.ConfigureServer", slog.Any("error", err))
		os.Exit(1)
	}

	if err := h1s.ListenAndServe(); err != nil {
		if err := shutdown(ctx); err != nil {
			slog.Error("Failed to shutdown", slog.Any("error", err))
			os.Exit(1)
		}
		slog.Error("ListenAndServe", slog.Any("error", err))
		os.Exit(1)
	}
}

// storageConfigFromFlags returns a gcp.Config struct populated with values
// provided via flags.
func storageConfigFromFlags() gcp.Config {
	if *bucket == "" {
		slog.Error("--bucket must be set")
		os.Exit(1)
	}
	if *spanner == "" {
		slog.Error("--spanner must be set")
		os.Exit(1)
	}
	return gcp.Config{
		Bucket:  *bucket,
		Spanner: *spanner,
	}
}

func signerFromFlags() (note.Signer, []note.Signer) {
	s, err := note.NewSigner(*signer)
	if err != nil {
		slog.Error("Failed to create new signer", slog.Any("error", err))
		os.Exit(1)
	}

	var a []note.Signer
	for _, as := range additionalSigners {
		s, err := note.NewSigner(as)
		if err != nil {
			slog.Error("Failed to create additional signer", slog.Any("error", err))
			os.Exit(1)
		}
		a = append(a, s)
	}

	return s, a
}
