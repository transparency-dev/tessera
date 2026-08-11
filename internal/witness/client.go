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

// Package witness contains the implementations of a witness client, used to send out a checkpoint to witnesses
// and retrieve sufficient signatures to satisfy a policy, and a witness service, used by the tlog-mirror implementation.
package witness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/cenkalti/backoff/v5"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/mod/sumdb/note"

	wc "github.com/transparency-dev/witness/client/http"
	"github.com/transparency-dev/witness/witness"
)

var (
	witnessClientReqsTotal    metric.Int64Counter
	witnessClientReqHistogram metric.Int64Histogram
	witnessClientRespsTotal   metric.Int64Counter

	// custom histogram buckets as we're interested in 10-100s of millis.
	witnessHistogramBuckets = []float64{0, 10, 20, 30, 40, 60, 80, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 4000, 5000, 6000, 8000, 10000}
)

func init() {
	var err error

	witnessClientReqsTotal, err = meter.Int64Counter(
		"tessera.witness.request",
		metric.WithDescription("Number of requests to the witnesses' submit endpoint"),
		metric.WithUnit("{call}"))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create witnessClientReqsTotal metric", slog.Any("error", err))
		os.Exit(1)
	}
	witnessClientReqHistogram, err = meter.Int64Histogram(
		"tessera.witness.duration",
		metric.WithDescription("Duration of calls to the witnesses' submit endpoint"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(witnessHistogramBuckets...))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create witnessClientReqHistogram metric", slog.Any("error", err))
		os.Exit(1)
	}
	witnessClientRespsTotal, err = meter.Int64Counter(
		"tessera.witness.response",
		metric.WithDescription("Number of responses from the witnesses' submit endpoint"),
		metric.WithUnit("{call}"))
	if err != nil {
		slog.ErrorContext(context.Background(), "Failed to create witnessClientRespsTotal metric", slog.Any("error", err))
		os.Exit(1)
	}
}

type Witness struct {
	URL       *url.URL
	Verifiers []note.Verifier
}

type Options struct {
	// HTTPClient is the HTTP client to use for all HTTP operations, if nil uses the DefaultHTTPClient.
	HTTPClient *http.Client
	// Witnesses defines the pool of witnesses to update.
	Witnesses []Witness
	// FetchTiles knows how to fetch tiles from the log. Used for building consistency proofs.
	FetchTiles client.TileFetcherFunc
}

// NewGateway returns a Gateway that will send out new checkpoints to witnesses.
func NewGateway(ctx context.Context, opts Options) (*WitnessGateway, error) {
	if opts.HTTPClient == nil {
		slog.WarnContext(ctx, "WitnessGateway: No HTTP client configured, using DefaultHTTPClient")
		opts.HTTPClient = http.DefaultClient
	}
	if opts.FetchTiles == nil {
		return nil, fmt.Errorf("fetch tiles is required")
	}

	witnesses := make([]*witnessClient, 0, len(opts.Witnesses))
	if ddw, err := dedup(opts.Witnesses); err != nil {
		return nil, err
	} else {
		opts.Witnesses = ddw
	}

	for _, w := range opts.Witnesses {
		if len(w.Verifiers) == 0 {
			return nil, fmt.Errorf("no verifiers for witness %s", w.URL.String())
		}
		witnesses = append(witnesses, &witnessClient{
			url:       w.URL.String(),
			client:    wc.NewWitness(w.URL, opts.HTTPClient),
			verifiers: w.Verifiers,
		})
	}
	return &WitnessGateway{
		witnesses: witnesses,
		fetchTile: opts.FetchTiles,
	}, nil
}

// dedup merges witnesses with the same URL, deduplicating verifiers within each witness.
func dedup(ws []Witness) ([]Witness, error) {
	// Collapse by URL, grouping verifiers if necessary
	d := make(map[string]*Witness)
	for i, w := range ws {
		if w.URL == nil || w.URL.String() == "" {
			return nil, fmt.Errorf("empty URL for witness at index %d", i)
		}
		uStr := w.URL.String()
		if wit, ok := d[uStr]; !ok {
			w.Verifiers = slices.Clone(w.Verifiers)
			d[uStr] = &w
		} else {
			wit.Verifiers = append(wit.Verifiers, w.Verifiers...)
		}
	}

	// Then ensure that we have no duplicate verifiers, within each witness
	type verifierKey struct {
		name string
		hash uint32
	}

	out := make([]Witness, 0, len(d))
	for _, k := range slices.Sorted(maps.Keys(d)) {
		w := d[k]

		seen := make(map[verifierKey]bool)
		vs := make([]note.Verifier, 0, len(w.Verifiers))
		for _, v := range w.Verifiers {
			k := verifierKey{name: v.Name(), hash: v.KeyHash()}
			if !seen[k] {
				seen[k] = true
				vs = append(vs, v)
			}
		}
		out = append(out, Witness{
			URL:       w.URL,
			Verifiers: vs,
		})
	}
	return out, nil
}

// WitnessGateway allows a log implementation to send out a checkpoint to witnesses.
type WitnessGateway struct {
	witnesses []*witnessClient
	fetchTile client.TileFetcherFunc
}

// CosignCheckpoint sends out a new checkpoint (which must be signed by the log), to all witnesses
// and returns gathered cosignatures via the returned channel as soon as they are available.
// The returned channel will be closed once all requests have completed (successfully or otherwise).
func (wg *WitnessGateway) CosignCheckpoint(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
	out := make(chan []byte, len(wg.witnesses))
	ctx, span := tracer.Start(ctx, "tessera.witnessgateway.CosignCheckpoint")
	defer span.End()

	if len(wg.witnesses) == 0 {
		close(out)
		return out
	}

	pb, err := client.NewProofBuilder(ctx, cpSize, wg.fetchTile)
	if err != nil {
		close(out)
		return out
	}
	pf := sharedConsistencyProofFetcher{
		pb:      pb,
		toSize:  cpSize,
		results: make(map[uint64]consistencyFuture),
	}

	var waitGroup sync.WaitGroup

	// Kick off a goroutine for each witness and send result to results chan
	for _, w := range wg.witnesses {
		waitGroup.Add(1)
		go func() {
			_ = otel.TraceErr(ctx, "tessera.witnessgateway.CosignCheckpoint.update", tracer, func(ctx context.Context, span trace.Span) error {
				span.SetAttributes(attribute.String("url", w.url))
				defer waitGroup.Done()
				sig, err := w.update(ctx, cp, cpSize, pf.ConsistencyProof)
				if err != nil {
					slog.ErrorContext(ctx, "WitnessGateway: failed to update witness", slog.String("url", w.url), slog.Any("error", err))
					return err
				}
				cpWithSig := append(slices.Clone(cp), sig...)
				if _, err := note.Open(cpWithSig, note.VerifierList(w.verifiers...)); err != nil {
					slog.ErrorContext(ctx, "WitnessGateway: invalid signature(s) from witness", slog.String("url", w.url), slog.Any("error", err))
					return err
				}
				out <- sig
				return nil
			})
		}()
	}

	go func() {
		_ = otel.TraceErr(ctx, "tessera.witnessgateway.CosignCheckpoint.closer", tracer, func(ctx context.Context, span trace.Span) error {
			waitGroup.Wait()
			close(out)
			return nil
		})
	}()

	return out
}

type consistencyFuture func() ([][]byte, error)

// sharedConsistencyProofFetcher is a thread-safe caching wrapper around a proof builder.
// This is an optimization for the common case where multiple witnesses are used, and all
// of the witnesses are of the same size, and thus require the same proof.
type sharedConsistencyProofFetcher struct {
	pb      *client.ProofBuilder
	toSize  uint64
	mu      sync.Mutex
	results map[uint64]consistencyFuture
}

// ConsistencyProof constructs a consistency proof, reusing any results from parallel requests.
func (pf *sharedConsistencyProofFetcher) ConsistencyProof(ctx context.Context, smaller, larger uint64) ([][]byte, error) {
	if larger != pf.toSize {
		return nil, fmt.Errorf("required larger size to be %d but was given %d", pf.toSize, larger)
	}
	var f consistencyFuture
	var ok bool
	pf.mu.Lock()
	if f, ok = pf.results[smaller]; !ok {
		f = sync.OnceValues(func() ([][]byte, error) {
			return pf.pb.ConsistencyProof(ctx, smaller, larger)
		})
		pf.results[smaller] = f
	}
	pf.mu.Unlock()
	return f()
}

// witnessClient is the log's model of a witness's view of this log.
type witnessClient struct {
	url string
	// client is the witness client to use to talk to this witness.
	client wc.Witness
	// verifiers are verifiers for the signature(s) returned by the witness.
	verifiers []note.Verifier
	// oldSize is the size of the checkpoint that the log thinks that the witness last signed.
	// This can be zero, in which case the tlog-witness protocol discovery mechanism is used to determine the old size.
	oldSize uint64
}

// names returns a []string with unique names of the verifiers, sorted.
func names(w []note.Verifier) []string {
	m := make(map[string]struct{}, len(w))
	for _, v := range w {
		m[v.Name()] = struct{}{}
	}
	s := slices.Collect(maps.Keys(m))
	sort.Strings(s)
	return s
}

func (w *witnessClient) update(ctx context.Context, cp []byte, cpSize uint64, fetchProof func(ctx context.Context, from, to uint64) ([][]byte, error)) ([]byte, error) {
	const maxUpdateRetries = 3
	// Set InitialInterval low so that we handle catching up after StatusConflict/ErrCheckpointStale quickly.
	// But ramp up quickly to avoid DoSing witnesses.
	retryBackoff := &backoff.ExponentialBackOff{
		InitialInterval: 10 * time.Millisecond,
		MaxInterval:     250 * time.Millisecond,
		Multiplier:      5,
	}

	return otel.Trace(ctx, "tessera.witness.update", tracer, func(ctx context.Context, span trace.Span) ([]byte, error) {
		witNames := names(w.verifiers)
		nameAttr := witnessNameKey.String(strings.Join(witNames, ","))

		// doUpdate implements a single attempt at updating the witness.
		// Used in the backoff.Retry() call below.
		doUpdate := func() ([]byte, error) {
			var (
				proof [][]byte
				err   error
			)
			if w.oldSize > 0 {
				proof, err = fetchProof(ctx, w.oldSize, cpSize)
				if err != nil {
					return nil, fmt.Errorf("fetchProof: %v", err)
				}
			}

			start := time.Now()
			witnessClientReqsTotal.Add(ctx, 1, metric.WithAttributes(nameAttr))
			sigs, actualSize, err := w.client.Update(ctx, w.oldSize, cp, proof)
			if err != nil {
				statusAttr := witnessStatusKey.String(witnessErrorStatus(err))
				witnessClientRespsTotal.Add(ctx, 1, metric.WithAttributes(nameAttr, statusAttr))

				switch {
				case errors.Is(err, witness.ErrCheckpointStale):
					if actualSize > cpSize {
						// This should _never_ happen - the witness somehow knows about a checkpoint larger than we have.
						return nil, backoff.Permanent(fmt.Errorf("witness at %q replied with x.tlog.size %d, larger than log size %d", w.url, actualSize, cpSize))
					}
					slog.InfoContext(ctx, "Retrying stale checkpoint", slog.String("url", w.url), slog.Uint64("actualSize", actualSize), slog.Uint64("cpSize", cpSize))
					w.oldSize = actualSize
					// Don't mark as permanent so that the backoff will retry.
					return nil, err
				case errors.Is(err, witness.ErrRootMismatch):
					// This is a non-retryable error, the log is broken.
					return nil, backoff.Permanent(err)
				}
				// To keep behaviour the same as it was previously, we won't retry for any other type of error.
				return nil, backoff.Permanent(err)
			}
			d := time.Since(start)
			statusAttr := witnessStatusKey.String("success")
			witnessClientRespsTotal.Add(ctx, 1, metric.WithAttributes(nameAttr, statusAttr))
			witnessClientReqHistogram.Record(ctx, d.Milliseconds(), metric.WithAttributes(nameAttr, statusAttr))
			w.oldSize = cpSize
			return sigs, nil
		}

		// Try the update with back-off.
		return backoff.Retry(ctx, doUpdate,
			backoff.WithMaxTries(maxUpdateRetries),
			backoff.WithBackOff(retryBackoff),
		)
	})
}

// witnessErrorStatus returns a string representation of the given witness error, for use in metrics.
//
// All errors from the witness package are here, regardless of whether we _expect_ to receive them.
func witnessErrorStatus(err error) string {
	switch {
	case errors.Is(err, witness.ErrNoValidSignature):
		return "no_valid_signature"
	case errors.Is(err, witness.ErrUnknownLog):
		return "unknown_log"
	case errors.Is(err, witness.ErrOldSizeInvalid):
		return "old_size_invalid"
	case errors.Is(err, witness.ErrCheckpointStale):
		return "stale"
	case errors.Is(err, witness.ErrInvalidProof):
		return "invalid_proof"
	case errors.Is(err, witness.ErrRootMismatch):
		return "root_mismatch"
	case errors.Is(err, witness.ErrPushback):
		return "pushback"
	case errors.Is(err, witness.ErrNoWitnessSignature):
		return "no_witness_signature"
	case errors.Is(err, witness.ErrSubtreeRangeInvalid):
		return "subtree_range_invalid"
	case errors.Is(err, witness.ErrNotImplemented):
		return "not_implemented"
	default:
		return "unknown_error"
	}
}
