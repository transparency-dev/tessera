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
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"errors"
	"testing"

	"github.com/transparency-dev/formats/log"
	"golang.org/x/mod/sumdb/note"
)

func TestAwait(t *testing.T) {
	t.Parallel()
	testTimeout := 100 * time.Millisecond
	testCases := []struct {
		desc    string
		fIndex  uint64
		fErr    error
		fDelay  time.Duration
		cpBody  []byte
		cpErr   error
		cpDelay time.Duration
		wantErr bool
	}{
		{
			desc:    "future error",
			fIndex:  0,
			fErr:    errors.New("you have no future"),
			fDelay:  0,
			wantErr: true,
		},
		{
			desc:    "future takes too long",
			fIndex:  2,
			fErr:    nil,
			fDelay:  testTimeout,
			wantErr: true,
		},
		{
			desc:    "checkpoint is big enough",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin\n3\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n"),
			cpErr:   nil,
			wantErr: false,
		},
		{
			desc:    "checkpoint is too small",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin\n2\nthisisdefinitelyahash\n"),
			cpErr:   nil,
			wantErr: true,
		},
		{
			desc:    "checkpoint takes too long",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin\n3\nthisisdefinitelyahash\n"),
			cpErr:   nil,
			cpDelay: testTimeout,
			wantErr: true,
		},
		{
			desc:    "checkpoint takes a few polls then returns",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin\n3\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n"),
			cpErr:   nil,
			cpDelay: 40 * time.Millisecond,
			wantErr: false,
		},
		{
			desc:    "checkpoint takes a few polls then fails",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  nil,
			cpErr:   errors.New("sorry but the checkpoint is in another castle"),
			cpDelay: 40 * time.Millisecond,
			wantErr: true,
		},
		{
			desc:    "checkpoint is garbled - no newlines",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin22nonewlineshere"),
			cpErr:   nil,
			wantErr: true,
		},
		{
			desc:    "checkpoint is garbled - size not parseable",
			fIndex:  2,
			fErr:    nil,
			fDelay:  0,
			cpBody:  []byte("origin\ntwo\nnonewlineshere"),
			cpErr:   nil,
			wantErr: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			t.Parallel()
			// Await will time out via this context, causing tests to fail
			// if the integration condition is never reached.
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()

			readCheckpoint := func(ctx context.Context) ([]byte, error) {
				<-time.After(tC.cpDelay)
				return tC.cpBody, tC.cpErr
			}
			awaiter := NewPublicationAwaiter(ctx, readCheckpoint, 10*time.Millisecond)

			future := func() (Index, error) {
				<-time.After(tC.fDelay)
				return Index{Index: tC.fIndex}, tC.fErr
			}
			i, cp, err := awaiter.Await(ctx, future)
			if gotErr := err != nil; gotErr != tC.wantErr {
				t.Fatalf("gotErr != wantErr (%t != %t): %v", gotErr, tC.wantErr, err)
			}
			if err != nil {
				// Everything after here tests successful Await
				return
			}
			if i.Index != tC.fIndex {
				t.Errorf("expected index %d but got %d", tC.fIndex, i.Index)
			}
			if !bytes.Equal(cp, tC.cpBody) {
				t.Errorf("expected checkpoint %q but got %q", tC.cpBody, cp)
			}
		})
	}
}

func TestAwait_multiClient(t *testing.T) {
	s, err := note.NewSigner("PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg")
	if err != nil {
		t.Fatal(err)
	}
	v, err := note.NewVerifier("example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx")
	if err != nil {
		t.Fatal(err)
	}

	t.Parallel()
	testTimeout := 1 * time.Second
	// Await will time out via this context, causing tests to fail
	// if the integration condition is never reached.
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	size := uint64(0)
	readCheckpoint := func(ctx context.Context) ([]byte, error) {
		<-time.After(3 * time.Millisecond)
		// Grow the tree every time this is called
		size += 10
		// This isn't generating a real log but can be changed if needed
		hash := sha256.Sum256(fmt.Append(nil, size))
		cpRaw := log.Checkpoint{
			Origin: "example.com/log/testdata",
			Size:   size,
			Hash:   hash[:],
		}.Marshal()
		n, err := note.Sign(&note.Note{Text: string(cpRaw)}, s)
		if err != nil {
			return nil, fmt.Errorf("note.Sign: %w", err)
		}
		return n, nil
	}
	awaiter := NewPublicationAwaiter(ctx, readCheckpoint, 10*time.Millisecond)

	wg := sync.WaitGroup{}
	for i := range 300 {
		index := uint64(i)
		future := func() (Index, error) {
			<-time.After(15 * time.Millisecond)
			return Index{Index: index}, nil
		}
		wg.Go(func() {
			i, cpRaw, err := awaiter.Await(ctx, future)
			if err != nil {
				t.Errorf("function for %d failed: %v", i.Index, err)
			}
			if i.Index != index {
				t.Errorf("got %d but expected %d", i.Index, index)
			}
			cp, _, _, err := log.ParseCheckpoint(cpRaw, "example.com/log/testdata", v)
			if err != nil {
				t.Error(err)
			}
			if cp.Size < i.Index {
				t.Errorf("got cp size of %d for index %d", cp.Size, i.Index)
			}

		})
	}
	wg.Wait()
}

func TestAwait_contextCancel(t *testing.T) {
	s, err := note.NewSigner("PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg")
	if err != nil {
		t.Fatal(err)
	}

	t.Parallel()
	testTimeout := 2 * time.Second
	// Await will time out via this context, causing tests to fail
	// if the integration condition is never reached.
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()

	var size atomic.Uint64
	readCheckpoint := func(ctx context.Context) ([]byte, error) {
		thisSize := size.Load()
		hash := sha256.Sum256(fmt.Append(nil, thisSize))
		cpRaw := log.Checkpoint{
			Origin: "example.com/log/testdata",
			Size:   thisSize,
			Hash:   hash[:],
		}.Marshal()
		n, err := note.Sign(&note.Note{Text: string(cpRaw)}, s)
		if err != nil {
			return nil, fmt.Errorf("note.Sign: %w", err)
		}
		return n, nil
	}
	awaiter := NewPublicationAwaiter(ctx, readCheckpoint, 10*time.Millisecond)
	awaiter.preWaitSignaller = make(chan struct{}, 2)

	wg := sync.WaitGroup{}

	// This one should succeed
	wg.Go(func() {
		future := func() (Index, error) {
			return Index{Index: 50}, nil
		}
		_, _, err := awaiter.Await(ctx, future)
		if err != nil {
			t.Error(err)
		}
	})

	timeoutSuccess := make(chan struct{})
	// This one is expected to hit deadline exceeded
	wg.Go(func() {
		cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		defer close(timeoutSuccess)
		future := func() (Index, error) {
			return Index{Index: 100}, nil
		}
		_, _, err := awaiter.Await(cctx, future)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Expected context deadline exceeded, got %v", err)
		}
	})

	// Block until both of the goroutines are waiting
	<-awaiter.preWaitSignaller
	<-awaiter.preWaitSignaller
	// Then wait until the second one has timed out
	<-timeoutSuccess
	// And finally release the final one for success
	size.Store(75)
	wg.Wait()
}

// awaitWithin runs fn in a goroutine and returns its error, failing the test if
// fn does not return within limit. Regression coverage for #1192 needs this
// because the failure mode being guarded against is an indefinite block, which
// would otherwise hang the test binary until the whole run times out.
func awaitWithin(t *testing.T, limit time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("Await did not return within %v", limit)
		return nil
	}
}

// TestAwait_NoCheckpointRespectsContext covers a log which has not yet published
// its first checkpoint. LogReader.ReadCheckpoint documents os.ErrNotExist as the
// way to signal this, and mirror/migration targets legitimately start out in
// that state. The poll loop publishes nothing at all while this persists, so
// Await only returns if it watches its own context.
func TestAwait_NoCheckpointRespectsContext(t *testing.T) {
	t.Parallel()

	readCheckpoint := func(ctx context.Context) ([]byte, error) {
		return nil, os.ErrNotExist
	}
	awaiter := NewPublicationAwaiter(t.Context(), readCheckpoint, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := awaitWithin(t, 5*time.Second, func() error {
		_, _, err := awaiter.Await(ctx, func() (Index, error) {
			return Index{Index: 0}, nil
		})
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Await err = %v, want context.DeadlineExceeded", err)
	}
}

// TestAwait_PollerShutdown covers the poll loop exiting while an Await is still
// blocked. The awaited context deliberately has no deadline of its own, so the
// only thing that can release the waiter is the awaiter reporting that its poll
// loop has gone away.
func TestAwait_PollerShutdown(t *testing.T) {
	t.Parallel()

	// Deliberately not t.Context(), so that only the explicit cancel below can
	// stop the poll loop.
	pollCtx, stopPoller := context.WithCancel(context.Background())
	defer stopPoller()

	// A healthy log, but one whose tree never grows to cover the index awaited
	// below.
	readCheckpoint := func(ctx context.Context) ([]byte, error) {
		return []byte("origin\n0\nhash\n\n"), nil
	}
	awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

	go func() {
		time.Sleep(50 * time.Millisecond)
		stopPoller()
	}()

	err := awaitWithin(t, 5*time.Second, func() error {
		_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
			return Index{Index: 5}, nil
		})
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Await err = %v, want context.Canceled", err)
	}
}

// TestAwait_CancelWithLongPollPeriod ensures cancellation latency is decoupled
// from the poll period. With an hour between polls the awaiter publishes
// nothing, so this only passes if Await watches its own context.
func TestAwait_CancelWithLongPollPeriod(t *testing.T) {
	t.Parallel()

	readCheckpoint := func(ctx context.Context) ([]byte, error) {
		return []byte("origin\n0\nhash\n\n"), nil
	}
	awaiter := NewPublicationAwaiter(t.Context(), readCheckpoint, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := awaitWithin(t, 5*time.Second, func() error {
		_, _, err := awaiter.Await(ctx, func() (Index, error) {
			return Index{Index: 5}, nil
		})
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Await err = %v, want context.Canceled", err)
	}
}

func BenchmarkAwait(b *testing.B) {
	cpFormat := "origin/\n%d\nhash\n\nsig"
	cpSize := atomic.Uint64{}
	readCP := func(_ context.Context) ([]byte, error) {
		return fmt.Appendf(nil, cpFormat, cpSize.Load()), nil
	}
	a := NewPublicationAwaiter(b.Context(), readCP, time.Millisecond)
	t := time.NewTicker(time.Millisecond)
	go func() {
		for {
			select {
			case <-b.Context().Done():
				return
			case <-t.C:
				cpSize.Add(1)
			}
		}
	}()
	f := func() (Index, error) {
		return Index{Index: cpSize.Load()}, nil
	}
	for b.Loop() {
		_, _, _ = a.Await(b.Context(), f)
	}
}
