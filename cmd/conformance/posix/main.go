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

// posix runs a web server that allows new entries to be POSTed to
// a tlog-tiles log stored on a posix filesystem. It allows to run
// conformance/compliance/performance tests and showing how to use
// the Tessera POSIX storage implementation.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/posix"
	badger_as "github.com/transparency-dev/tessera/storage/posix/antispam"
	"k8s.io/klog/v2"
)

var (
	storageDir                = flag.String("storage_dir", "", "Root directory to store log data.")
	listen                    = flag.String("listen", ":2025", "Address:port to listen on")
	privKeyFile               = flag.String("private_key", "", "Location of private key file. If unset, uses the contents of the LOG_PRIVATE_KEY environment variable.")
	persistentAntispam        = flag.Bool("antispam", false, "EXPERIMENTAL: Set to true to enable Badger-based persistent antispam storage")
	additionalPrivateKeyFiles = []string{}
)

func init() {
	flag.Func("additional_private_key", "Location of additional private key, may be specified multiple times", func(s string) error {
		additionalPrivateKeyFiles = append(additionalPrivateKeyFiles, s)
		return nil
	})
}

func addCacheHeaders(value string, fs http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Cache-Control", value)
		fs.ServeHTTP(w, r)
	}
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	// Gather the info needed for reading/writing checkpoints
	s, a := getSignersOrDie()

	// Create the Tessera POSIX storage, using the directory from the --storage_dir flag
	driver, err := posix.New(ctx, posix.Config{Path: *storageDir})
	if err != nil {
		klog.Exitf("Failed to construct storage: %v", err)
	}
	var antispam tessera.Antispam
	// Persistent antispam is currently experimental, so there's no terraform or documentation yet!
	if *persistentAntispam {
		asOpts := badger_as.AntispamOpts{}
		antispam, err = badger_as.NewAntispam(ctx, filepath.Join(*storageDir, ".state", "antispam"), asOpts)
		if err != nil {
			klog.Exitf("Failed to create new Badger antispam storage: %v", err)
		}
	}

	appender, shutdown, _, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s, a...).
		WithCheckpointInterval(time.Second).
		WithCheckpointRepublishInterval(time.Minute).
		WithBatching(256, time.Second).
		WithAntispam(tessera.DefaultAntispamInMemorySize, antispam))
	if err != nil {
		klog.Exit(err)
	}

	// Define a handler for /add that accepts POST requests and adds the POST body to the log
	http.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		idx, err := appender.Add(r.Context(), tessera.NewEntry(b))()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err := fmt.Fprintf(w, "%d", idx.Index); err != nil {
			klog.Errorf("/add: %v", err)
			return
		}
	})
	// Proxy all GET requests to the filesystem as a lightweight file server.
	// This makes it easier to test this implementation from another machine.
	fs := http.FileServer(http.Dir(*storageDir))
	http.Handle("GET /checkpoint", addCacheHeaders("no-cache", fs))
	http.Handle("GET /tile/", addCacheHeaders("max-age=31536000, immutable", fs))
	http.Handle("GET /entries/", fs)

	// TODO(mhutchinson): Change the listen flag to just a port, or fix up this address formatting
	klog.Infof("Environment variables useful for accessing this log:\n"+
		"export WRITE_URL=http://localhost%s/ \n"+
		"export READ_URL=http://localhost%s/ \n", *listen, *listen)
	// Run the HTTP server with the single handler and block until this is terminated
	h2s := &http2.Server{}
	h1s := &http.Server{
		Addr:              *listen,
		Handler:           h2c.NewHandler(http.DefaultServeMux, h2s),
		ReadHeaderTimeout: 5 * time.Second,
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

func getSignersOrDie() (note.Signer, []note.Signer) {
	s := getSignerOrDie()
	a := []note.Signer{}
	for _, p := range additionalPrivateKeyFiles {
		kr, err := getKeyFile(p)
		if err != nil {
			klog.Exitf("Unable to get additional private key from %q: %v", p, err)
		}
		k, err := note.NewSigner(kr)
		if err != nil {
			klog.Exitf("Failed to instantiate signer from %q: %v", p, err)
		}
		a = append(a, k)
	}
	return s, a
}

// Read log private key from file or environment variable
func getSignerOrDie() note.Signer {
	var privKey string
	var err error
	if len(*privKeyFile) > 0 {
		privKey, err = getKeyFile(*privKeyFile)
		if err != nil {
			klog.Exitf("Unable to get private key: %q", err)
		}
	} else {
		privKey = os.Getenv("LOG_PRIVATE_KEY")
		if len(privKey) == 0 {
			klog.Exit("Supply private key file path using --private_key or set LOG_PRIVATE_KEY environment variable")
		}
	}
	s, err := note.NewSigner(privKey)
	if err != nil {
		klog.Exitf("Failed to instantiate signer: %q", err)
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
