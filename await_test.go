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
	"testing/synctest"

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

// TestAwait_noCheckpointRespectsContext checks that Await returns once its
// context expires, even if the log has not yet published a checkpoint.
//
// See https://github.com/transparency-dev/tessera/issues/1192.
func TestAwait_noCheckpointRespectsContext(t *testing.T) {
	t.Parallel()

	// Within the synctest bubble, time only advances once every goroutine is
	// durably blocked, so Await is guaranteed to be waiting on the awaiter
	// before its context deadline passes.
	synctest.Test(t, func(t *testing.T) {
		// The awaiter is long-lived, so it has its own context which outlives the
		// Await call below. Cancelling it at the end of the test lets the poll loop
		// broadcast one final time, releasing any goroutine still stuck in Await.
		awaiterCtx, cancelAwaiter := context.WithCancel(t.Context())
		defer cancelAwaiter()

		// Simulate a log which has not yet published a checkpoint.
		readCheckpoint := func(_ context.Context) ([]byte, error) {
			return nil, os.ErrNotExist
		}
		awaiter := NewPublicationAwaiter(awaiterCtx, readCheckpoint, 10*time.Millisecond)

		awaitCtx, cancelAwait := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancelAwait()

		errC := make(chan error, 1)
		go func() {
			_, _, err := awaiter.Await(awaitCtx, func() (Index, error) { return Index{Index: 0}, nil })
			errC <- err
		}()

		select {
		case err := <-errC:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Await: got err %v, want %v", err, context.DeadlineExceeded)
			}
		case <-time.After(1 * time.Second):
			t.Error("Await did not return after its context expired")
		}
	})
}

// TestAwait_PollerShutdown covers the poll loop exiting while an Await is
// blocked, after it has already exited, and while readCheckpoint is in-flight.
func TestAwait_PollerShutdown(t *testing.T) {
	t.Parallel()

	t.Run("while blocked", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			readCheckpoint := func(_ context.Context) ([]byte, error) {
				return []byte("origin\n0\nhash\n\n"), nil
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

			errC := make(chan error, 1)
			go func() {
				_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
					return Index{Index: 5}, nil
				})
				errC <- err
			}()

			synctest.Wait()
			stopPoller()

			select {
			case err := <-errC:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("Await err = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Error("Await did not return after poller shutdown")
			}
		})
	})

	t.Run("after exit", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				return []byte("origin\n0\nhash\n\n"), nil
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

			stopPoller()
			synctest.Wait()

			_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Await err = %v, want context.Canceled", err)
			}
		})
	})

	t.Run("cancel during in-flight read preserves newer checkpoint", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			inRead := make(chan struct{})
			releaseRead := make(chan struct{})
			cp10 := []byte("origin\n10\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n\n")

			readCheckpoint := func(_ context.Context) ([]byte, error) {
				close(inRead)
				<-releaseRead
				return cp10, nil
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

			err5 := make(chan error, 1)
			err20 := make(chan error, 1)
			go func() {
				_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
					return Index{Index: 5}, nil
				})
				err5 <- err
			}()
			go func() {
				_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
					return Index{Index: 20}, nil
				})
				err20 <- err
			}()

			<-inRead
			stopPoller()
			close(releaseRead)

			if err := <-err5; err != nil {
				t.Errorf("index 5 err = %v, want nil", err)
			}
			if err := <-err20; !errors.Is(err, context.Canceled) {
				t.Errorf("index 20 err = %v, want context.Canceled", err)
			}
		})
	})
}

// TestAwait_CancelWithLongPollPeriod ensures cancellation latency is decoupled
// from the poll period.
func TestAwait_CancelWithLongPollPeriod(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		awaiterCtx, stopPoller := context.WithCancel(t.Context())
		defer stopPoller()

		readCheckpoint := func(_ context.Context) ([]byte, error) {
			return []byte("origin\n0\nhash\n\n"), nil
		}
		awaiter := NewPublicationAwaiter(awaiterCtx, readCheckpoint, time.Hour)

		ctx, cancel := context.WithCancel(t.Context())
		errC := make(chan error, 1)
		go func() {
			_, _, err := awaiter.Await(ctx, func() (Index, error) {
				return Index{Index: 5}, nil
			})
			errC <- err
		}()

		synctest.Wait()
		cancel()

		select {
		case err := <-errC:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Await err = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Error("Await did not return after context cancellation")
		}
	})
}

// TestAwait_AlreadyCancelledContext checks that an already-done caller context
// always takes precedence over cached checkpoints and poller shutdown errors.
func TestAwait_AlreadyCancelledContext(t *testing.T) {
	t.Parallel()

	t.Run("ready checkpoint", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			readCheckpoint := func(_ context.Context) ([]byte, error) {
				return []byte("origin\n10\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n\n"), nil
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)
			time.Sleep(25 * time.Millisecond)

			reqCtx, cancelReq := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancelReq()
			time.Sleep(20 * time.Millisecond)

			_, _, err := awaiter.Await(reqCtx, func() (Index, error) {
				return Index{Index: 2}, nil
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Await err = %v, want context.DeadlineExceeded", err)
			}
		})
	})

	t.Run("after poller shutdown", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				return []byte("origin\n0\nhash\n\n"), nil
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)
			stopPoller()
			synctest.Wait()

			reqCtx, cancelReq := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer cancelReq()
			time.Sleep(20 * time.Millisecond)

			_, _, err := awaiter.Await(reqCtx, func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Await err = %v, want context.DeadlineExceeded", err)
			}
		})
	})
}

// TestAwait_ConsecutiveErrors checks the two-consecutive-errors tolerance,
// recovery across unchanged tree sizes, and fast-path preservation for already
// published indices during transient poll errors.
func TestAwait_ConsecutiveErrors(t *testing.T) {
	t.Parallel()

	err1 := errors.New("transient error 1")
	err2 := errors.New("transient error 2")
	err3 := errors.New("transient error 3")
	cp2 := []byte("origin\n2\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n\n")
	cp10 := []byte("origin\n10\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n\n")

	t.Run("single error tolerated and streak resets on recovery", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			var step atomic.Int32
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				switch step.Add(1) {
				case 1:
					return cp2, nil
				case 2:
					return nil, err1
				case 3:
					return cp2, nil // recovery with unchanged tree size
				case 4:
					return nil, err2
				default:
					return cp10, nil
				}
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

			_, cp, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if err != nil {
				t.Fatalf("Await err = %v, want nil", err)
			}
			if !bytes.Equal(cp, cp10) {
				t.Errorf("Await cp = %q, want %q", cp, cp10)
			}
		})
	})

	t.Run("two consecutive errors fail", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			var step atomic.Int32
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				if step.Add(1) == 1 {
					return nil, err1
				}
				return nil, err2
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

			_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if !errors.Is(err, err2) {
				t.Errorf("Await err = %v, want %v", err, err2)
			}
		})
	})

	t.Run("arrive after two errors waits for next poll and recovers", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			var step atomic.Int32
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				switch step.Add(1) {
				case 1:
					return cp2, nil
				case 2:
					return nil, err1
				case 3:
					return nil, err2
				case 4:
					return cp2, nil // recovers with unchanged size
				case 5:
					return nil, err3 // single error tolerated
				default:
					return cp10, nil
				}
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)
			time.Sleep(35 * time.Millisecond) // polls 1..3 complete

			_, cp, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if err != nil {
				t.Fatalf("Await err = %v, want nil", err)
			}
			if !bytes.Equal(cp, cp10) {
				t.Errorf("Await cp = %q, want %q", cp, cp10)
			}
		})
	})

	t.Run("arrive after two errors fails on third", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			var step atomic.Int32
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				switch step.Add(1) {
				case 1:
					return nil, err1
				case 2:
					return nil, err2
				default:
					return nil, err3
				}
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)
			time.Sleep(25 * time.Millisecond)

			_, _, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if !errors.Is(err, err3) {
				t.Errorf("Await err = %v, want %v", err, err3)
			}
		})
	})

	t.Run("already published index survives transient errors", func(t *testing.T) {
		t.Parallel()
		synctest.Test(t, func(t *testing.T) {
			pollCtx, stopPoller := context.WithCancel(t.Context())
			defer stopPoller()

			var step atomic.Int32
			readCheckpoint := func(_ context.Context) ([]byte, error) {
				if step.Add(1) == 1 {
					return cp10, nil
				}
				return nil, err1
			}
			awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)
			time.Sleep(35 * time.Millisecond) // poll 1 succeeds, polls 2-3 fail

			_, cp, err := awaiter.Await(t.Context(), func() (Index, error) {
				return Index{Index: 5}, nil
			})
			if err != nil {
				t.Fatalf("Await err = %v, want nil", err)
			}
			if !bytes.Equal(cp, cp10) {
				t.Errorf("Await cp = %q, want %q", cp, cp10)
			}
		})
	})
}

// TestAwait_BucketWraparound ensures waiters at indices beyond the bucket
// window (numBuckets << bucketShift) wake cleanly and never miss publication.
func TestAwait_BucketWraparound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pollCtx, stopPoller := context.WithCancel(t.Context())
		defer stopPoller()

		var size atomic.Uint64
		readCheckpoint := func(_ context.Context) ([]byte, error) {
			s := size.Add(200)
			return fmt.Appendf(nil, "origin\n%d\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n\n", s), nil
		}
		awaiter := NewPublicationAwaiter(pollCtx, readCheckpoint, 10*time.Millisecond)

		target := uint64(numBuckets<<bucketShift) + 50
		i, _, err := awaiter.Await(t.Context(), func() (Index, error) {
			return Index{Index: target}, nil
		})
		if err != nil {
			t.Fatalf("Await err = %v, want nil", err)
		}
		if i.Index != target {
			t.Errorf("Await index = %d, want %d", i.Index, target)
		}
	})
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
