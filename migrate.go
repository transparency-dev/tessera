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

package tessera

import (
	"context"
	"fmt"
	"sync/atomic"

	"log/slog"

	"github.com/cenkalti/backoff/v5"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"golang.org/x/sync/errgroup"
)

type setEntryBundleFunc func(ctx context.Context, index uint64, partial uint8, bundle []byte) error

func newCopier(numWorkers uint, setEntryBundle setEntryBundleFunc, getEntryBundle client.EntryBundleFetcherFunc) *copier {
	return &copier{
		setEntryBundle: setEntryBundle,
		getEntryBundle: getEntryBundle,
		todo:           make(chan bundle, numWorkers),
	}
}

// copier controls the migration work.
type copier struct {
	setEntryBundle setEntryBundleFunc
	getEntryBundle client.EntryBundleFetcherFunc

	// todo contains work items to be completed.
	todo chan bundle

	// bundlesCopied is the number of entry bundles copied so far.
	bundlesCopied atomic.Uint64
}

// bundle represents the address of an individual entry bundle.
type bundle struct {
	Index   uint64
	Partial uint8
}

// Copy starts the work of copying sourceSize entries from the source to the target log.
//
// Only the entry bundles are copied as the target storage is expected to integrate them and recalculate the root.
// This is done to ensure the correctness of both the source log as well as the copy process itself.
//
// A call to this function will block until either the copying is done, or an error has occurred.
func (c *copier) Copy(ctx context.Context, fromSize uint64, sourceSize uint64) error {
	slog.InfoContext(ctx, "Starting copy", slog.Uint64("from", fromSize), slog.Uint64("to", sourceSize))

	if fromSize > sourceSize {
		return fmt.Errorf("from size %d > source size %d", fromSize, sourceSize)
	}

	go c.populateWork(ctx, fromSize, sourceSize)

	// Do the copying
	eg := errgroup.Group{}
	for range cap(c.todo) {
		eg.Go(func() error {
			return c.worker(ctx)
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("copy failed: %v", err)
	}

	return nil
}

// Progress returns the number of bundles from the source present in the target.
func (c *copier) BundlesCopied() uint64 {
	return c.bundlesCopied.Load()
}

// populateWork sends entries to the `todo` work channel.
// Each entry corresponds to an individual entryBundle which needs to be copied.
func (m *copier) populateWork(ctx context.Context, from, treeSize uint64) {
	slog.InfoContext(ctx, "Spans for entry range", slog.Uint64("from", from), slog.Uint64("to", treeSize))
	defer close(m.todo)

	for ri := range layout.Range(from, treeSize-from, treeSize) {
		m.todo <- bundle{Index: ri.Index, Partial: ri.Partial}
	}
}

// worker undertakes work items from the `todo` channel.
//
// It will attempt to retry failed operations several times before giving up, this should help
// deal with any transient errors which may occur.
func (m *copier) worker(ctx context.Context) error {
	for b := range m.todo {
		n, err := backoff.Retry(ctx, func() (uint64, error) {
			d, err := m.getEntryBundle(ctx, b.Index, uint8(b.Partial))
			if err != nil {
				wErr := fmt.Errorf("failed to fetch entrybundle %d (p=%d): %v", b.Index, b.Partial, err)
				slog.InfoContext(ctx, "Fetch error", slog.Any("error", wErr))
				return 0, wErr
			}
			if err := m.setEntryBundle(ctx, b.Index, b.Partial, d); err != nil {
				wErr := fmt.Errorf("failed to store entrybundle %d (p=%d): %v", b.Index, b.Partial, err)
				slog.InfoContext(ctx, "Store error", slog.Any("error", wErr))
				return 0, wErr
			}
			return 1, nil
		},
			backoff.WithMaxTries(10),
			backoff.WithBackOff(backoff.NewExponentialBackOff()))
		if err != nil {
			slog.InfoContext(ctx, "Retry error", slog.Any("error", err))
			return err
		}
		m.bundlesCopied.Add(n)
	}
	return nil
}
