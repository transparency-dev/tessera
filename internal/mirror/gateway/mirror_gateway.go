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

package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/client/mirror"
	"golang.org/x/mod/sumdb/note"
)

// WitnessGroup defines the subset of tessera.WitnessGroup methods needed by the gateway.
type WitnessGroup interface {
	WitnessEndpoints() map[string][]note.Verifier
}

// LogReader defines the subset of tessera.LogReader methods needed by the gateway.
type LogReader interface {
	ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error)
	ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error)
}

// goal represents a desired state for a mirror.
// It contains a target checkpoint (and its size broken out for simplicity), and a
// callback which must be called once the goal is attained, or an error occurred.
type goal struct {
	cpSize uint64
	cp     []byte
	done   func([]byte, error)
}

// mirrorTarget represents a tlog-mirror service which we'll attempt to update.
type mirrorTarget struct {
	url    *url.URL
	client *mirror.Client

	goals chan goal
}

// Gateway manages the process of keeping mirrors up-to-date.
type Gateway struct {
	httpClient *http.Client
	lr         LogReader
	targets    []*mirrorTarget
}

// NewGateway creates a new Gateway that will keep mirrors up-to-date.
func NewGateway(ctx context.Context, httpClient *http.Client, mirrors WitnessGroup, lr LogReader, logOrigin string) *Gateway {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	g := &Gateway{
		httpClient: httpClient,
		lr:         lr,
	}

	endpoints := mirrors.WitnessEndpoints()
	for u := range endpoints {
		parsedURL, err := url.Parse(u)
		if err != nil {
			slog.ErrorContext(ctx, "Invalid mirror URL", slog.String("url", u), slog.Any("error", err))
			continue
		}

		tileFetcher := func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
			return lr.ReadTile(ctx, level, index, p)
		}
		bundleFetcher := func(ctx context.Context, index uint64, p uint8) ([]byte, error) {
			return lr.ReadEntryBundle(ctx, index, p)
		}
		mirrorCheckpointFetcher := func(ctx context.Context) ([]byte, error) {
			checkpointURL, err := parsedURL.Parse("checkpoint")
			if err != nil {
				return nil, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, checkpointURL.String(), nil)
			if err != nil {
				return nil, err
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				return nil, err
			}
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode == http.StatusNotFound {
				return nil, os.ErrNotExist
			}
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("failed to fetch checkpoint from mirror: status %d", resp.StatusCode)
			}
			return io.ReadAll(resp.Body)
		}

		mOpts := mirror.NewOptions().
			WithMirrorURL(parsedURL).
			WithHTTPClient(httpClient).
			WithLogOrigin(logOrigin).
			WithTileFetcher(tileFetcher).
			WithBundleFetcher(bundleFetcher).
			WithMirrorCheckpointFetcher(mirrorCheckpointFetcher)

		c, err := mirror.NewClient(ctx, mOpts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror client", slog.String("url", u), slog.Any("error", err))
			continue
		}

		target := &mirrorTarget{
			url:    parsedURL,
			client: c,
			goals:  make(chan goal, 1),
		}
		g.targets = append(g.targets, target)

		// Start the worker goroutine.
		go g.runWorker(ctx, target)
	}

	return g
}

// CosignCheckpoint updates the goals for all mirrors and returns a channel on which it will send
// cosignatures as they are successfully fetched from the mirrors.
// The channel is closed once all mirrors' signatures have been sent or the context is canceled.
func (g *Gateway) CosignCheckpoint(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
	if len(g.targets) == 0 {
		return nil
	}

	out := make(chan []byte, len(g.targets))
	wg := sync.WaitGroup{}

	// Send goals to each of the target workers, but don't block if they're
	// already busy.
	for _, target := range g.targets {
		newGoal := goal{
			cp:     cp,
			cpSize: cpSize,
			done: func(sig []byte, err error) {
				defer wg.Done()
				if err != nil {
					slog.ErrorContext(ctx, "Mirror sync failed", slog.String("url", target.url.String()), slog.Any("error", err))
					return
				}
				slog.InfoContext(ctx, "Mirror sync succeeded", slog.String("url", target.url.String()), slog.Uint64("size", cpSize))
				out <- sig
			},
		}

		for done := false; !done; {
			// Add the goal to the target worker. If there's already a goal in the channel, then we'll try to replace it since the current cosign request
			// supercedes it. This is racy, but that's fine since we're only trying to replace a pending request - it's fine if the worker has already picked up
			// the old goal.
			select {
			case <-ctx.Done():
				done = true
			case target.goals <- newGoal:
				// The goal was sent, we're done
				wg.Add(1)
				done = true
			default:
				// No space in the goals channel, try to supercede the goal currently in there.
				select {
				case oldGoal := <-target.goals:
					// Ok, we've removed the superceded one, let's signal that it's done:
					oldGoal.done(nil, fmt.Errorf("superseded by newer goal for size %d", cpSize))
					// Then let the loop retry the send.
				default:
					// Channel became empty in the meantime, let the loop retry the send.
				}
			}
		}
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// runWorker runs the main loop of a mirror worker: it picks up goals from the goals channel
// and attempts to satisfy them.
//
// It will block on the goals channel until a goal is received, or the context is
// cancelled.
func (g *Gateway) runWorker(ctx context.Context, target *mirrorTarget) {
	slog.InfoContext(ctx, "Starting mirror worker", slog.String("url", target.url.String()))
	defer slog.InfoContext(ctx, "Stopping mirror worker", slog.String("url", target.url.String()))

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-target.goals:
			if !ok {
				// Channel closed, stop.
				return
			}

			// Loop until the goal is met, or we timeout.
			done := false
			interval := time.Millisecond
			for !done {
				select {
				case <-ctx.Done():
					return
				case <-time.After(interval):
					interval = time.Second
				}
				// In a func for context defer.
				func() {
					// TODO(al): Make this configurable? Should be plenty of time for normal operation, and we'll retry anyway if we do timeout.
					cctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
					defer cancel()

					slog.DebugContext(cctx, "Syncing mirror", slog.String("url", target.url.String()), slog.Uint64("goal", job.cpSize))
					sigs, err := target.client.Sync(cctx, job.cp, job.cpSize)
					if err != nil {
						slog.WarnContext(ctx, "Mirror sync attempt failed, retrying", slog.String("url", target.url.String()), slog.Any("error", err))
						return
					}
					done = true
					job.done(sigs, nil)
				}()
			}
		}
	}
}
