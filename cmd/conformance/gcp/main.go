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

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/gcp"
	gcp_as "github.com/transparency-dev/tessera/storage/gcp/antispam"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"k8s.io/klog/v2"
)

var (
	bucket             = flag.String("bucket", "", "Bucket to use for storing log")
	listen             = flag.String("listen", ":2024", "Address:port to listen on")
	spanner            = flag.String("spanner", "", "Spanner resource URI ('projects/.../...')")
	signer             = flag.String("signer", "", "Note signer to use to sign checkpoints")
	persistentAntispam = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable GCP-based persistent antispam storage")
	traceFraction      = flag.Float64("trace_fraction", 0.01, "Fraction of open-telemetry span traces to sample")
	additionalSigners  = []string{}
)

func init() {
	flag.Func("additional_signer", "Additional note signer for checkpoints, may be specified multiple times", func(s string) error {
		additionalSigners = append(additionalSigners, s)
		return nil
	})
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	shutdownOTel := initOTel(ctx, *traceFraction)
	defer shutdownOTel(ctx)

	s, a := signerFromFlags()

	// Create our Tessera storage backend:
	gcpCfg := storageConfigFromFlags()
	driver, err := gcp.New(ctx, gcpCfg)
	if err != nil {
		klog.Exitf("Failed to create new GCP storage: %v", err)
	}

	var antispam tessera.Antispam
	// Persistent antispam is currently experimental, so there's no terraform or documentation yet!
	if *persistentAntispam {
		asOpts := gcp_as.AntispamOpts{} // Use defaults
		antispam, err = gcp_as.NewAntispam(ctx, fmt.Sprintf("%s-antispam", *spanner), asOpts)
		if err != nil {
			klog.Exitf("Failed to create new GCP antispam storage: %v", err)
		}
	}

	appender, shutdown, _, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s, a...).
		WithCheckpointInterval(10*time.Second).
		WithBatching(512, 300*time.Millisecond).
		WithPushback(10*4096).
		WithAntispam(tessera.DefaultAntispamInMemorySize, antispam))
	if err != nil {
		klog.Exit(err)
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
		Addr:    *listen,
		Handler: h2c.NewHandler(http.DefaultServeMux, h2s),
	}
	if err := http2.ConfigureServer(h1s, h2s); err != nil {
		klog.Exitf("http2.ConfigureServer: %v", err)
	}

	if err := h1s.ListenAndServe(); err != nil {
		if err := shutdown(ctx); err != nil {
			klog.Exit(err)
		}
		klog.Exitf("ListenAndServe: %v", err)
	}
}

// storageConfigFromFlags returns a gcp.Config struct populated with values
// provided via flags.
func storageConfigFromFlags() gcp.Config {
	if *bucket == "" {
		klog.Exit("--bucket must be set")
	}
	if *spanner == "" {
		klog.Exit("--spanner must be set")
	}
	return gcp.Config{
		Bucket:  *bucket,
		Spanner: *spanner,
	}
}

func signerFromFlags() (note.Signer, []note.Signer) {
	s, err := note.NewSigner(*signer)
	if err != nil {
		klog.Exitf("Failed to create new signer: %v", err)
	}

	var a []note.Signer
	for _, as := range additionalSigners {
		s, err := note.NewSigner(as)
		if err != nil {
			klog.Exitf("Failed to create additional signer: %v", err)
		}
		a = append(a, s)
	}

	return s, a
}
