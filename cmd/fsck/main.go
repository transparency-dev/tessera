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

// fsck is a command-line tool for checking the integrity of a tlog-tiles based log.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"time"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/cmd/fsck/tui"
	"github.com/transparency-dev/tessera/fsck"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/time/rate"
	"k8s.io/klog/v2"
)

var (
	storageURL  = flag.String("storage_url", "", "Base tlog-tiles URL")
	bearerToken = flag.String("bearer_token", "", "The bearer token for authorizing HTTP requests to the storage URL, if needed")
	N           = flag.Uint("N", 1, "The number of workers to use when fetching/comparing resources")
	origin      = flag.String("origin", "", "Origin of the log to check, if unset, will use the name of the provided public key")
	pubKey      = flag.String("public_key", "", "Path to a file containing the log's public key")
	qps         = flag.Float64("qps", 0, "Max QPS to send to the target log. Set to zero for unlimited")
	ui          = flag.Bool("ui", true, "Set to true to use a TUI to display progress, or false for logging")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	logURL, err := url.Parse(*storageURL)
	if err != nil {
		klog.Exitf("Invalid --storage_url %q: %v", *storageURL, err)
	}
	var src fsck.Fetcher

	if logURL.Scheme == "file" {
		src = &client.FileFetcher{
			Root: logURL.Path,
		}
	} else {
		httpSrc, err := client.NewHTTPFetcher(logURL, nil)
		if err != nil {
			klog.Exitf("Failed to create HTTP fetcher: %v", err)
		}
		if *bearerToken != "" {
			httpSrc.SetAuthorizationHeader(fmt.Sprintf("Bearer %s", *bearerToken))
		}
		src = httpSrc
	}
	if *qps > 0 {
		src = &rateLimitedSrc{
			rl:       rate.NewLimiter(rate.Limit(*qps), 10),
			delegate: src,
		}
	}
	v := verifierFromFlags()
	if *origin == "" {
		*origin = v.Name()
	}
	f := fsck.New(*origin, v, src, defaultMerkleLeafHasher, fsck.Opts{N: *N})

	go func() {
		if err := f.Check(ctx); err != nil {
			klog.Errorf("fsck failed: %v", err)
		}
		klog.V(1).Infof("Completed ranges:\n%s", f.Status())
		cancel()
	}()

	if *ui {
		tui.RunApp(ctx, f)
		// User exited the UI, cancel the context to signal to everything else.
		cancel()
	} else {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				klog.V(1).Infof("Ranges:\n%s", f.Status())
			}
		}
	}
}

// defaultMerkleLeafHasher parses a C2SP tlog-tile bundle and returns the Merkle leaf hashes of each entry it contains.
func defaultMerkleLeafHasher(bundle []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bundle); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	r := make([][]byte, 0, len(eb.Entries))
	for _, e := range eb.Entries {
		h := rfc6962.DefaultHasher.HashLeaf(e)
		r = append(r, h[:])
	}
	return r, nil
}

func verifierFromFlags() note.Verifier {
	if *pubKey == "" {
		klog.Exit("Must provide the --public_key flag")
	}
	b, err := os.ReadFile(*pubKey)
	if err != nil {
		klog.Exitf("Failed to read verifier from %q: %v", *pubKey, err)
	}
	v, err := f_note.NewVerifier(string(b))
	if err != nil {
		klog.Exitf("Invalid verifier in %q: %v", *pubKey, err)
	}
	return v
}

type rateLimitedSrc struct {
	rl       *rate.Limiter
	delegate fsck.Fetcher
}

func (r *rateLimitedSrc) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	if err := r.rl.Wait(ctx); err != nil {
		return nil, err
	}
	return r.delegate.ReadCheckpoint(ctx)
}

func (r *rateLimitedSrc) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	if err := r.rl.Wait(ctx); err != nil {
		return nil, err
	}
	return r.delegate.ReadTile(ctx, l, i, p)
}

func (r *rateLimitedSrc) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	if err := r.rl.Wait(ctx); err != nil {
		return nil, err
	}
	return r.delegate.ReadEntryBundle(ctx, i, p)
}
