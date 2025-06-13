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

// hammer is a tool to load test a Tessera log.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/hammer/loadtest"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/net/http2"

	"k8s.io/klog/v2"
)

func init() {
	flag.Var(&logURL, "log_url", "Log storage root URL (can be specified multiple times), e.g. https://log.server/and/path/")
	flag.Var(&writeLogURL, "write_log_url", "Root URL for writing to a log (can be specified multiple times), e.g. https://log.server/and/path/ (optional, defaults to log_url)")
}

var (
	logURL      multiStringFlag
	writeLogURL multiStringFlag

	logPubKey = flag.String("log_public_key", os.Getenv("TILES_LOG_PUBLIC_KEY"), "Public key for the log. This is defaulted to the environment variable TILES_LOG_PUBLIC_KEY")

	maxReadOpsPerSecond = flag.Int("max_read_ops", 20, "The maximum number of read operations per second")
	numReadersRandom    = flag.Int("num_readers_random", 4, "The number of readers looking for random leaves")
	numReadersFull      = flag.Int("num_readers_full", 4, "The number of readers downloading the whole log")

	maxWriteOpsPerSecond = flag.Int("max_write_ops", 0, "The maximum number of write operations per second")
	numWriters           = flag.Int("num_writers", 0, "The number of independent write tasks to run")

	leafMinSize = flag.Int("leaf_min_size", 0, "Minimum size in bytes of individual leaves")
	dupChance   = flag.Float64("dup_chance", 0.1, "The probability of a generated leaf being a duplicate of a previous value")

	leafWriteGoal = flag.Int64("leaf_write_goal", 0, "Exit after writing this number of leaves, or 0 to keep going indefinitely")
	maxRunTime    = flag.Duration("max_runtime", 0, "Fail after this amount of time has passed, or 0 to keep going indefinitely")

	showUI = flag.Bool("show_ui", true, "Set to false to disable the text-based UI")

	bearerToken      = flag.String("bearer_token", "", "The bearer token for auth. For GCP this is the result of `gcloud auth print-access-token`")
	bearerTokenWrite = flag.String("bearer_token_write", "", "The bearer token for auth to write. For GCP this is the result of `gcloud auth print-identity-token`. If unset will default to --bearer_token.")

	httpTimeout = flag.Duration("http_timeout", 30*time.Second, "Timeout for HTTP requests")
	forceHTTP2  = flag.Bool("force_http2", false, "Use HTTP/2 connections *only*")

	hc *http.Client
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	hc = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        *numWriters + *numReadersFull + *numReadersRandom,
			MaxIdleConnsPerHost: *numWriters + *numReadersFull + *numReadersRandom,
			DisableKeepAlives:   false,
		},
		Timeout: *httpTimeout,
	}
	if *forceHTTP2 {
		hc.Transport = &http2.Transport{
			// So http2.Transport doesn't complain the URL scheme isn't 'https'
			AllowHTTP: true,
			// Pretend we are dialing a TLS endpoint. (Note, we ignore the passed tls.Config)
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}
	}

	// If bearerTokenWrite is unset, default it to whatever bearerToken has (which may too be unset).
	if *bearerTokenWrite == "" {
		*bearerTokenWrite = *bearerToken
	}

	ctx, cancel := context.WithCancel(context.Background())

	logSigV, err := note.NewVerifier(*logPubKey)
	if err != nil {
		klog.Exitf("failed to create verifier: %v", err)
	}

	r := mustCreateReaders(logURL)
	if len(writeLogURL) == 0 {
		writeLogURL = logURL
	}
	w := mustCreateWriters(writeLogURL)

	var cpRaw []byte
	cons := client.UnilateralConsensus(r.ReadCheckpoint)
	tracker, err := client.NewLogStateTracker(ctx, r.ReadTile, cpRaw, logSigV, logSigV.Name(), cons)
	if err != nil {
		klog.Exitf("Failed to create LogStateTracker: %v", err)
	}
	// Fetch initial state of log
	_, _, _, err = tracker.Update(ctx)
	if err != nil {
		klog.Exitf("Failed to get initial state of the log: %v", err)
	}

	ha := loadtest.NewHammerAnalyser(func() uint64 { return tracker.Latest().Size })
	ha.Run(ctx)

	gen := newLeafGenerator(tracker.Latest().Size, *leafMinSize, *dupChance)
	opts := loadtest.HammerOpts{
		MaxReadOpsPerSecond:  *maxReadOpsPerSecond,
		MaxWriteOpsPerSecond: *maxWriteOpsPerSecond,
		NumReadersRandom:     *numReadersRandom,
		NumReadersFull:       *numReadersFull,
		NumWriters:           *numWriters,
	}
	hammer := loadtest.NewHammer(tracker, r.ReadEntryBundle, w, gen, ha.SeqLeafChan, ha.ErrChan, opts)

	exitCode := 0
	if *leafWriteGoal > 0 {
		go func() {
			startTime := time.Now()
			goal := tracker.Latest().Size + uint64(*leafWriteGoal)
			klog.Infof("Will exit once tree size is at least %d", goal)
			tick := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if tracker.Latest().Size >= goal {
						elapsed := time.Since(startTime)
						klog.Infof("Reached tree size goal of %d after %s; exiting", goal, elapsed)
						cancel()
						return
					}
				}
			}
		}()
	}
	if *maxRunTime > 0 {
		go func() {
			klog.Infof("Will fail after %s", *maxRunTime)
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(*maxRunTime):
					klog.Infof("Max runtime reached; exiting")
					exitCode = 1
					cancel()
					return
				}
			}
		}()
	}
	hammer.Run(ctx)

	if *showUI {
		c := loadtest.NewController(hammer, ha)
		c.Run(ctx)
	} else {
		<-ctx.Done()
	}
	os.Exit(exitCode)
}

// newLeafGenerator returns a function that generates values to append to a log.
// The leaves are constructed to be at least minLeafSize bytes long.
// The generator can be used by concurrent threads.
//
// dupChance provides the probability that a new leaf will be a duplicate of a previous entry.
// Leaves will be unique if dupChance is 0, and if set to 1 then all values will be duplicates.
// startSize should be set to the initial size of the log so that repeated runs of the
// hammer can start seeding leaves to avoid duplicates with previous runs.
func newLeafGenerator(startSize uint64, minLeafSize int, dupChance float64) func() []byte {
	// genLeaf MUST be determinstic given n
	genLeaf := func(n uint64) []byte {
		// Make a slice with half the number of requested bytes since we'll
		// hex-encode them below which gets us back up to the full amount.
		filler := make([]byte, minLeafSize/2)
		source := rand.New(rand.NewPCG(0, n))
		for i := range filler {
			// This throws away a lot of the generated data. An exercise to a future
			// coder is to fill in multiple bytes at a time.
			filler[i] = byte(source.Int())
		}
		return fmt.Appendf(nil, "%x %d", filler, n)
	}

	sizeLocked := startSize
	var mu sync.Mutex
	return func() []byte {
		mu.Lock()
		thisSize := sizeLocked

		if thisSize > 0 && rand.Float64() <= dupChance {
			thisSize = rand.Uint64N(thisSize)
		} else {
			sizeLocked++
		}
		mu.Unlock()

		// Do this outside of the protected block so that writers don't block on leaf generation (especially for larger leaves).
		return genLeaf(thisSize)
	}
}

func mustCreateReaders(us []string) loadtest.LogReader {
	r := []loadtest.LogReader{}
	for _, u := range us {
		if !strings.HasSuffix(u, "/") {
			u += "/"
		}
		rURL, err := url.Parse(u)
		if err != nil {
			klog.Exitf("Invalid log reader URL %q: %v", u, err)
		}

		switch rURL.Scheme {
		case "http", "https":
			c, err := client.NewHTTPFetcher(rURL, hc)
			if err != nil {
				klog.Exitf("Failed to create HTTP fetcher for %q: %v", u, err)
			}
			if *bearerToken != "" {
				c.SetAuthorizationHeader(fmt.Sprintf("Bearer %s", *bearerToken))
			}
			r = append(r, c)
		case "file":
			r = append(r, client.FileFetcher{Root: rURL.Path})
		default:
			klog.Exitf("Unsupported scheme %s on log URL", rURL.Scheme)
		}
	}
	return loadtest.NewRoundRobinReader(r)
}

func mustCreateWriters(us []string) loadtest.LeafWriter {
	w := []loadtest.LeafWriter{}
	for _, u := range us {
		if !strings.HasSuffix(u, "/") {
			u += "/"
		}
		u += "add"
		wURL, err := url.Parse(u)
		if err != nil {
			klog.Exitf("Invalid log writer URL %q: %v", u, err)
		}
		w = append(w, httpWriter(wURL, hc, *bearerTokenWrite))
	}
	return loadtest.NewRoundRobinWriter(w)
}

func httpWriter(u *url.URL, hc *http.Client, bearerToken string) loadtest.LeafWriter {
	cTrace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { klog.Infof("connection established %#v", info) },
	}
	return func(ctx context.Context, newLeaf []byte) (uint64, error) {
		req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(newLeaf))
		if err != nil {
			return 0, fmt.Errorf("failed to create request: %v", err)
		}
		if bearerToken != "" {
			req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", bearerToken))
		}
		reqCtx := req.Context()
		if klog.V(2).Enabled() {
			reqCtx = httptrace.WithClientTrace(req.Context(), cTrace)
		}
		resp, err := hc.Do(req.WithContext(reqCtx))
		if err != nil {
			return 0, fmt.Errorf("failed to write leaf: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return 0, fmt.Errorf("failed to read body: %v", err)
		}
		switch resp.StatusCode {
		case http.StatusOK:
			if resp.Request.Method != http.MethodPost {
				return 0, fmt.Errorf("write leaf was redirected to %s", resp.Request.URL)
			}
			// Continue below
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout, http.StatusTooManyRequests:
			// These status codes may indicate a delay before retrying, so handle that here:
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), time.Second))

			return 0, fmt.Errorf("log not available. Status code: %d. Body: %q %w", resp.StatusCode, body, loadtest.ErrRetry)
		default:
			return 0, fmt.Errorf("write leaf was not OK. Status code: %d. Body: %q", resp.StatusCode, body)
		}
		parts := bytes.Split(body, []byte("\n"))
		index, err := strconv.ParseUint(string(parts[0]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("write leaf failed to parse response: %v", body)
		}
		return index, nil
	}
}

func retryDelay(retryAfter string, defaultDur time.Duration) time.Duration {
	if retryAfter == "" {
		return defaultDur
	}
	d, err := time.Parse(http.TimeFormat, retryAfter)
	if err == nil {
		return time.Until(d)
	}
	s, err := strconv.Atoi(retryAfter)
	if err == nil {
		return time.Duration(s) * time.Second
	}
	return defaultDur
}

// multiStringFlag allows a flag to be specified multiple times on the command
// line, and stores all of these values.
type multiStringFlag []string

func (ms *multiStringFlag) String() string {
	return strings.Join(*ms, ",")
}

func (ms *multiStringFlag) Set(w string) error {
	*ms = append(*ms, w)
	return nil
}
