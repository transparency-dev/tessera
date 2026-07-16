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
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/client/mirror"
	"github.com/transparency-dev/tessera/internal/mirror/hammer/loadtest"
	"github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/net/http2"

	sdbNote "golang.org/x/mod/sumdb/note"

	"log/slog"
)

func init() {
	flag.Var(&mirrorURL, "mirror_url", "Root URL for writing to a mirror (can be specified multiple times), e.g. https://log.server/and/path/")
}

var (
	mirrorURL multiStringFlag

	storageDir = flag.String("storage_dir", "", "Root directory to store log data")
	logPrivKey = flag.String("log_private_key", "", "Location of private key file")
	sourceLogURL        = flag.String("source_log_url", "", "URL of the log to mirror, or empty if using a locally created log.")
	sourceLogPubKeyPath = flag.String("source_log_public_key_path", "", "Path to the public key of the log to mirror. Required if --source_log_url is set.")

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
	slogLevel   = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")

	hc *http.Client
)

type logReader interface {
	ReadCheckpoint(ctx context.Context) ([]byte, error)
	ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error)
	ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error)
}

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))

	hc = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        *numWriters,
			MaxIdleConnsPerHost: *numWriters,
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

	appender, logReader, shutdown, verifier := logFromFlags(ctx)
	defer func() {
		if err := shutdown(ctx); err != nil {
			slog.ErrorContext(ctx, "Failed to shutdown appender", slog.Any("error", err))
		}
	}()

	cons := client.UnilateralConsensus(logReader.ReadCheckpoint)
	tracker, err := client.NewLogStateTracker(ctx, logReader.ReadTile, nil, verifier, verifier.Name(), cons)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create LogStateTracker", slog.Any("error", err))
		os.Exit(1)
	}
	// Fetch initial state of log
	_, _, _, err = tracker.Update(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to get initial state of the log", slog.Any("error", err))
		os.Exit(1)
	}

	// TODO(roger2hk): Move this into the internal/mirror/hammer/loadtest package.
	// Sync log data to mirror servers.
	for _, urlStr := range mirrorURL {
		mURL, err := url.Parse(urlStr)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to parse write mirror URL", slog.String("url", urlStr), slog.Any("error", err))
			os.Exit(1)
		}
		mOpts := mirror.NewOptions().
			WithMirrorURL(mURL).
			WithHTTPClient(hc).
			WithLogOrigin(verifier.Name()).
			WithTileFetcher(logReader.ReadTile).
			WithBundleFetcher(logReader.ReadEntryBundle).
			WithMirrorCheckpointFetcher(func(ctx context.Context) ([]byte, error) {
				return nil, nil
			})
		mc, err := mirror.NewClient(ctx, mOpts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror client", slog.String("url", urlStr), slog.Any("error", err))
			os.Exit(1)
		}

		go runMirrorSync(ctx, tracker, mc, mURL)
	}

	var hammer *loadtest.Hammer
	var ha *loadtest.HammerAnalyser
	if appender != nil {
		hammer, ha = newHammer(ctx, appender, logReader, tracker)
		ha.Run(ctx)
		hammer.Run(ctx)
	}

	exitCode := 0
	if *leafWriteGoal > 0 {
		go func() {
			startTime := time.Now()
			goal := tracker.Latest().Size + uint64(*leafWriteGoal)
			slog.InfoContext(ctx, "Waiting for target tree size", slog.Uint64("size", goal))
			tick := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if tracker.Latest().Size >= goal {
						elapsed := time.Since(startTime)
						slog.InfoContext(ctx, "Reached tree size goal; exiting", slog.Uint64("size", goal), slog.Duration("elapsed", elapsed))
						cancel()
						return
					}
				}
			}
		}()
	}
	if *maxRunTime > 0 {
		go func() {
			slog.InfoContext(ctx, "Configured max runtime", slog.Duration("duration", *maxRunTime))
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(*maxRunTime):
					slog.InfoContext(ctx, "Max runtime reached; exiting")
					exitCode = 1
					cancel()
					return
				}
			}
		}()
	}

	if *showUI {
		c := loadtest.NewController(hammer, ha, tracker)
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

// Read log private key from file.
func getSignerOrDie(ctx context.Context) note.Signer {
	var privKey string
	var err error

	if len(*logPrivKey) == 0 {
		slog.ErrorContext(ctx, "Supply log private key file path using --log_private_key")
		os.Exit(1)
	}

	privKey, err = getKeyFile(*logPrivKey)
	if err != nil {
		slog.ErrorContext(ctx, "Unable to get private key", slog.Any("error", err))
		os.Exit(1)
	}

	s, err := note.NewSignerForCosignatureV1(privKey)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to instantiate signer", slog.Any("error", err))
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

// runMirrorSync syncs the source log to a mirror server.
func runMirrorSync(ctx context.Context, tracker *client.LogStateTracker, mClient *mirror.Client, mURL *url.URL) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var lastSyncedSize uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			latest := tracker.Latest()
			if latest.Size == lastSyncedSize {
				continue
			}
			slog.InfoContext(ctx, "Syncing source log to mirror", slog.Uint64("size", latest.Size), slog.String("mirror", mURL.String()))
			latestRaw := tracker.LatestRaw()
			cosigs, err := mClient.Sync(ctx, latestRaw, latest.Size)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to sync to mirror", slog.String("mirror", mURL.String()), slog.Any("error", err))
				continue
			}
			slog.InfoContext(ctx, "Successfully synced to mirror", slog.Uint64("size", latest.Size), slog.String("mirror", mURL.String()), slog.Int("cosigs_len", len(cosigs)))
			lastSyncedSize = latest.Size
		}
	}
}

func logFromFlags(ctx context.Context) (*tessera.Appender, logReader, func(context.Context) error, sdbNote.Verifier) {
	if *sourceLogURL != "" {
		if *sourceLogPubKeyPath == "" {
			slog.ErrorContext(ctx, "Source log URL provided but no public key path.")
			os.Exit(1)
		}
		lr, shutdown, v, err := newRemoteLog(ctx, *sourceLogURL, *sourceLogPubKeyPath)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create remote log", slog.Any("error", err))
			os.Exit(1)
		}
		return nil, lr, shutdown, v
	}

	if *storageDir == "" {
		slog.ErrorContext(ctx, "No storage directory provided.")
		os.Exit(1)
	}
	app, lr, shutdown, v, err := newLocalLog(ctx, *storageDir, getSignerOrDie(ctx))
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create local log", slog.Any("error", err))
		os.Exit(1)
	}
	return app, lr, shutdown, v
}

// newLocalLog creates a new local POSIX log which can be used by a hammer to hammer the mirror.
//
// Returns an Appender for adding leaves to the log, a LogReader that reads from the local log,
func newLocalLog(ctx context.Context, storageDir string, s note.Signer) (*tessera.Appender, logReader, func(context.Context) error, sdbNote.Verifier, error) {
	// Create the Tessera POSIX storage, using the provided directory.
	driver, err := posix.New(ctx, posix.Config{Path: storageDir})
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to construct storage: %w", err)
	}

	appender, shutdown, logReader, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(s).
		WithCheckpointInterval(time.Second).
		WithCheckpointRepublishInterval(time.Minute).
		WithBatching(tessera.DefaultBatchMaxSize, time.Second))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return appender, logReader, shutdown, s.Verifier(), nil
}

// newRemoteLog creates a logReader that reads from a remote log, and a shutdown func.
func newRemoteLog(_ context.Context, logURL string, pubKeyPath string) (logReader, func(context.Context) error, sdbNote.Verifier, error) {
	pubKRaw, err := getKeyFile(pubKeyPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read public key %q: %v", pubKeyPath, err)
	}

	pubKey, err := note.NewVerifier(pubKRaw)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse public key %q: %v", pubKeyPath, err)
	}

	var lr logReader
	u, err := url.Parse(logURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse log URL %q: %v", logURL, err)
	}
	switch u.Scheme {
	case "http", "https":
		lr, err = client.NewHTTPFetcher(u, hc)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to create HTTP fetcher: %v", err)
		}
	default:
		lr = client.FileFetcher{Root: logURL}
	}

	return lr, func(context.Context) error { return nil }, pubKey, nil
}

func newHammer(_ context.Context, appender *tessera.Appender, logReader logReader, tracker *client.LogStateTracker) (*loadtest.Hammer, *loadtest.HammerAnalyser) {
	leafWriter := func(ctx context.Context, newLeaf []byte) (uint64, error) {
		idx, err := appender.Add(ctx, tessera.NewEntry(newLeaf))()
		if err != nil {
			return 0, fmt.Errorf("failed to add leaf: %v", err)
		}
		return idx.Index, nil
	}
	ha := loadtest.NewHammerAnalyser(func() uint64 { return tracker.Latest().Size })

	gen := newLeafGenerator(tracker.Latest().Size, *leafMinSize, *dupChance)
	opts := loadtest.HammerOpts{
		MaxReadOpsPerSecond:  0,
		MaxWriteOpsPerSecond: *maxWriteOpsPerSecond,
		NumReadersRandom:     0,
		NumReadersFull:       0,
		NumWriters:           *numWriters,
	}
	hammer := loadtest.NewHammer(tracker, logReader.ReadEntryBundle, leafWriter, gen, ha.SeqLeafChan, ha.ErrChan, opts)

	return hammer, ha
}
