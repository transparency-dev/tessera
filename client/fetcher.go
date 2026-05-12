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

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/fetcher"
)

// TransientError indicates that an error is temporary and the operation can be retried.
type TransientError struct {
	Err        error
	RetryAfter time.Duration // Optional, parsed from header if available
}

func (e TransientError) Error() string {
	return fmt.Sprintf("transient error: %v", e.Err)
}

func (e TransientError) Unwrap() error {
	return e.Err
}

// NewHTTPFetcher creates a new HTTPFetcher for the log rooted at the given URL, using
// the provided HTTP client.
//
// rootURL should end in a trailing slash.
// c may be nil, in which case http.DefaultClient will be used.
func NewHTTPFetcher(rootURL *url.URL, c *http.Client) (*HTTPFetcher, error) {
	if !strings.HasSuffix(rootURL.String(), "/") {
		rootURL.Path += "/"
	}
	if c == nil {
		c = http.DefaultClient
	}
	return &HTTPFetcher{
		c:       c,
		rootURL: rootURL,
	}, nil
}

// HTTPFetcher knows how to fetch log artifacts from a log being served via HTTP.
type HTTPFetcher struct {
	c          *http.Client
	rootURL    *url.URL
	authHeader string
}

// SetAuthorizationHeader sets the value to be used with an Authorization: header
// for every request made by this fetcher.
func (h *HTTPFetcher) SetAuthorizationHeader(v string) {
	h.authHeader = v
}

func isTransientNetworkError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ECONNREFUSED:
			return true
		}
	}
	return false
}

func (h HTTPFetcher) fetch(ctx context.Context, p string) ([]byte, error) {
	u, err := h.rootURL.Parse(p)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("NewRequestWithContext(%q): %v", u.String(), err)
	}
	if h.authHeader != "" {
		req.Header.Add("Authorization", h.authHeader)
	}
	r, err := h.c.Do(req)
	if err != nil {
		if isTransientNetworkError(err) {
			return nil, TransientError{Err: err}
		}
		return nil, err
	}
	defer func() {
		// Drain the body to ensure the underlying TCP connection can be returned
		// to the keep-alive pool and reused for future requests.
		// Limit the drain to avoid hanging on large or infinite responses.
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 4096))

		if err := r.Body.Close(); err != nil {
			slog.ErrorContext(ctx, "resp.Body.Close", slog.Any("error", err))	
		}
	}()

	switch r.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			if isTransientNetworkError(err) {
				return nil, TransientError{Err: err}
			}
			return nil, err
		}
		return data, nil
	case http.StatusNotFound:
		// Need to return ErrNotExist here, by contract.
		return nil, fmt.Errorf("get(%q): %w", u.String(), os.ErrNotExist)
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		var retryAfter time.Duration
		if ra := r.Header.Get("Retry-After"); ra != "" {
			retryAfter = parseRetryAfter(ra)
		}
		return nil, TransientError{
			Err:        fmt.Errorf("get(%q): status %d", u.String(), r.StatusCode),
			RetryAfter: retryAfter,
		}
	default:
		return nil, fmt.Errorf("get(%q): %v", u.String(), r.StatusCode)
	}
}

// parseRetryAfter parses the Retry-After header and returns a time.Duration.
func parseRetryAfter(retryAfter string) time.Duration {
	if retryAfter == "" {
		return 0
	}
	d, err := http.ParseTime(retryAfter)
	if err == nil {
		dur := time.Until(d)
		if dur <= 0 {
			return time.Nanosecond
		}
		return dur
	}
	s, err := strconv.Atoi(retryAfter)
	if err == nil {
		if s <= 0 {
			return time.Nanosecond
		}
		return time.Duration(s) * time.Second
	}
	return 0
}

func (h HTTPFetcher) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	return h.fetch(ctx, layout.CheckpointPath)
}

func (h HTTPFetcher) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return h.fetch(ctx, layout.TilePath(l, i, p))
	})
}

func (h HTTPFetcher) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return h.fetch(ctx, layout.EntriesPath(i, p))
	})
}

// retryOpts holds the configuration for retry logic.
type retryOpts struct {
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// RetryOption is a function that modifies retryOpts.
type RetryOption func(*retryOpts)

// WithMaxRetries sets the maximum number of retries.
func WithMaxRetries(n int) RetryOption {
	return func(o *retryOpts) { o.maxRetries = n }
}

// WithInitialBackoff sets the initial backoff duration.
func WithInitialBackoff(d time.Duration) RetryOption {
	return func(o *retryOpts) { o.initialBackoff = d }
}

// WithMaxBackoff sets the maximum backoff duration.
func WithMaxBackoff(d time.Duration) RetryOption {
	return func(o *retryOpts) { o.maxBackoff = d }
}

func defaultRetryOpts() retryOpts {
	return retryOpts{
		maxRetries:     5,
		initialBackoff: 100 * time.Millisecond,
		maxBackoff:     2 * time.Second,
	}
}

// WithTileRetry decorates a TileFetcherFunc with retry logic.
func WithTileRetry(f TileFetcherFunc, opts ...RetryOption) TileFetcherFunc {
	o := defaultRetryOpts()
	for _, opt := range opts {
		opt(&o)
	}
	return func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
		return retry(ctx, o, func() ([]byte, error) {
			return f(ctx, level, index, p)
		})
	}
}

// WithEntryBundleRetry decorates an EntryBundleFetcherFunc with retry logic.
func WithEntryBundleRetry(f EntryBundleFetcherFunc, opts ...RetryOption) EntryBundleFetcherFunc {
	o := defaultRetryOpts()
	for _, opt := range opts {
		opt(&o)
	}
	return func(ctx context.Context, bundleIndex uint64, p uint8) ([]byte, error) {
		return retry(ctx, o, func() ([]byte, error) {
			return f(ctx, bundleIndex, p)
		})
	}
}

// WithCheckpointRetry decorates a CheckpointFetcherFunc with retry logic.
func WithCheckpointRetry(f CheckpointFetcherFunc, opts ...RetryOption) CheckpointFetcherFunc {
	o := defaultRetryOpts()
	for _, opt := range opts {
		opt(&o)
	}
	return func(ctx context.Context) ([]byte, error) {
		return retry(ctx, o, func() ([]byte, error) {
			return f(ctx)
		})
	}
}

// retry retries the function f with exponential backoff up to maxRetries.
func retry[T any](ctx context.Context, opts retryOpts, f func() (T, error)) (T, error) {
	var backoff = opts.initialBackoff
	var err error
	var res T
	for attempt := 0; attempt <= opts.maxRetries; attempt++ {
		res, err = f()
		if err == nil {
			return res, nil
		}

		var tErr TransientError
		if errors.As(err, &tErr) {
			if attempt == opts.maxRetries {
				break
			}
			delay := backoff
			if tErr.RetryAfter > 0 {
				delay = min(tErr.RetryAfter, opts.maxBackoff)
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return res, ctx.Err()
			case <-timer.C:
				if tErr.RetryAfter == 0 {
					// Add jitter up to 50% of the current backoff
					var jitter time.Duration
					if n := int64(backoff / 2); n > 0 {
						jitter = time.Duration(rand.Int64N(n))
					}
					backoff = min(backoff*2, opts.maxBackoff) + jitter
				}
				continue
			}
		}
		return res, err
	}
	return res, fmt.Errorf("after %d retries: %w", opts.maxRetries, err)
}

// FileFetcher knows how to fetch log artifacts from a filesystem rooted at Root.
type FileFetcher struct {
	Root string
}

func (f FileFetcher) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(path.Join(f.Root, layout.CheckpointPath))
}

func (f FileFetcher) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return os.ReadFile(path.Join(f.Root, layout.TilePath(l, i, p)))
	})
}

func (f FileFetcher) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return os.ReadFile(path.Join(f.Root, layout.EntriesPath(i, p)))
	})
}
