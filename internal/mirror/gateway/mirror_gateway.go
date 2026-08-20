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

// Package gateway contains a mirror gateway implementation which knows how to keep a pool of mirrors
// up to date with a given log.
package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/client/mirror"
)

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

// Options represents the configuration for a Gateway.
type Options struct {
	// HTTPClient is the HTTP client to use for all HTTP operations, if nil uses the DefaultHTTPClient.
	HTTPClient *http.Client
	// Mirrors defines the pool of mirrors to update.
	Mirrors []*url.URL
	// LogReader provides access to the main log.
	LogReader LogReader
}

// NewGateway creates a new Gateway that will keep mirrors up-to-date.
//
// The provided context should be cancelled once the gateway is no-longer needed such that resources
// associated with it are released, in particular this will cause all worker goroutines to terminate.
func NewGateway(ctx context.Context, opts Options) (*Gateway, error) {
	if opts.HTTPClient == nil {
		slog.WarnContext(ctx, "MirrorGateway: No HTTP client configured, using DefaultHTTPClient")
		opts.HTTPClient = http.DefaultClient
	}
	if opts.LogReader == nil {
		return nil, fmt.Errorf("log reader is required")
	}

	gw := &Gateway{
		httpClient: opts.HTTPClient,
		lr:         opts.LogReader,
	}

	endpoints := opts.Mirrors
	for _, u := range endpoints {
		mirrorFetcher, err := client.NewHTTPFetcher(u, opts.HTTPClient)
		if err != nil {
			return nil, fmt.Errorf("invalid mirror URL %v: %v", u, err)
		}

		mOpts := mirror.NewOptions().
			WithMirrorURL(u).
			WithHTTPClient(opts.HTTPClient).
			WithTileFetcher(opts.LogReader.ReadTile).
			WithBundleFetcher(opts.LogReader.ReadEntryBundle).
			WithMirrorCheckpointFetcher(mirrorFetcher.ReadCheckpoint)

		c, err := mirror.NewClient(ctx, mOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to create mirror client for URL %q: %v", u, err)
		}

		target := &mirrorTarget{
			url:    u,
			client: c,
			goals:  make(chan goal, 1),
		}
		gw.targets = append(gw.targets, target)
	}
	// Start the workers
	for _, target := range gw.targets {
		go gw.runWorker(ctx, target)
	}

	return gw, nil
}

// CosignCheckpoint updates the goals for all mirrors and returns a channel over which
// cosignatures will be sent as they are successfully fetched from the mirrors.
// The channel is closed once all mirrors' signatures have been sent or the context is canceled.
//
// It is strongly recommended that the context passed in here has a deadline or timeout set.
func (gw *Gateway) CosignCheckpoint(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
	out := make(chan []byte, len(gw.targets))

	// If there are no mirrors, return immediately.
	if len(gw.targets) == 0 {
		// Return a _closed_ channel, not `nil`, as callers will iterate over the
		// returned channel and we don't want to block them forever.
		close(out)
		return out
	}

	N := &atomic.Uint32{}

	// Send updated goals to each of the target workers.
	for _, target := range gw.targets {
		newGoal := goal{
			cp:     cp,
			cpSize: cpSize,
			done: func(sig []byte, err error) {
				// Last one out please turn off the lights.
				defer func() {
					if N.Add(1) == uint32(len(gw.targets)) {
						close(out)
					}
				}()

				if err != nil {
					slog.ErrorContext(ctx, "MirrorGateway: Sync failed", slog.String("url", target.url.String()), slog.Any("error", err))
					return
				}
				slog.InfoContext(ctx, "MirrorGateway: Sync succeeded", slog.String("url", target.url.String()), slog.Uint64("size", cpSize))
				out <- sig
			},
		}

		for done := false; !done; {
			// Send the goal to the target worker.
			select {
			case <-ctx.Done():
				done = true
			case target.goals <- newGoal:
				done = true
			default:
				// No space in the goals channel, try to supersede the goal currently in there with the new one.
				// This replacement is "racy", but the worst that can happen is that the worker has already
				// picked up the old goal and we end up simply queuing the new goal instead of replacing the old one.
				select {
				case oldGoal := <-target.goals:
					// Ok, we've removed the superseded goal, so we need to signal that it's done:
					oldGoal.done(nil, fmt.Errorf("superseded by newer goal for size %d", cpSize))
					// Then let the loop retry the send in the select at the top.
				default:
					// Channel became empty in the meantime, let the loop retry the send.
				}
			}
		}
	}

	return out
}

// runWorker runs the main loop of a mirror worker: it picks up goals from the goals channel
// and attempts to satisfy them.
//
// It will block on the goals channel until a goal is received, or the context is
// cancelled.
func (gw *Gateway) runWorker(ctx context.Context, target *mirrorTarget) {
	slog.InfoContext(ctx, "MirrorGateway: Starting worker", slog.String("url", target.url.String()))
	defer slog.InfoContext(ctx, "MirrorGateway: Stopping worker", slog.String("url", target.url.String()))

	for {
		select {
		case <-ctx.Done():
			return
		case goal, ok := <-target.goals:
			if !ok {
				// Channel closed: down tools and exit.
				return
			}

			// TODO(al): Consider making this timeout configurable. This should be fine for the moment though.
			goalJob := gw.chaseGoalJob(target, goal, 1*time.Minute)
			sigs, err := goalJob(ctx)
			goal.done(sigs, err)
		}
	}
}

func (gw *Gateway) chaseGoalJob(target *mirrorTarget, job goal, timeout time.Duration) func(context.Context) ([]byte, error) {
	return func(ctx context.Context) ([]byte, error) {
		var rSigs []byte
		var rErr error

		interval := time.Millisecond
		// Loop until the goal is met, or we timeout.
		for i, done := 0, false; !done; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(interval):
				interval = time.Second
			}

			// In a func for context defer.
			func() {
				cctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				slog.DebugContext(cctx, "MirrorGateway: Syncing mirror", slog.String("url", target.url.String()), slog.Uint64("goal", job.cpSize))
				rSigs, rErr = target.client.Sync(cctx, job.cp, job.cpSize)
				if rErr != nil {
					// TODO(al): Update the client so we can tell whether an error is permanent or transient, and abandon jobs which will never succeed.
					// Currently, a worker for a mirror which is unavailable will keep retrying ~forever until the mirror comes back.
					slog.WarnContext(cctx, "MirrorGateway: Sync failed, retrying", slog.String("url", target.url.String()), slog.Any("error", rErr))
					return
				}
				done = true
			}()
		}
		return rSigs, rErr
	}
}
