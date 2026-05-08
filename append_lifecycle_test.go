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

package tessera

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/mod/sumdb/note"
)

func TestMemoize(t *testing.T) {
	// Set up an AddFn which will increment a counter every time it's called, and return that in the Index.
	i := uint64(0)
	deleg := func() (Index, error) {
		i++
		return Index{
			Index: i,
		}, nil
	}
	add := func(_ context.Context, _ *Entry) IndexFuture {
		return deleg
	}

	// Create a single future (for a single Entry), and convince ourselves that the counter is being incremented
	// each time the future is being invoked.
	f1 := add(nil, nil)
	a, _ := f1()
	b, _ := f1()
	if a.Index == b.Index {
		t.Fatalf("a(=%d) == b(=%d)", a.Index, b.Index)
	}

	// Now create an AddFn which memoizes the result of the delegate, like we do in NewAppender, and assert that
	// repeated calls to the future work as expected; only incrementing the counter once.
	add = func(_ context.Context, _ *Entry) IndexFuture {
		return memoizeFuture(deleg)
	}
	f2 := add(nil, nil)
	c, _ := f2()
	d, _ := f2()

	if c.Index != d.Index {
		t.Fatalf("c(=%d) != d(=%d)", c.Index, d.Index)
	}
}

const testSignerKey = "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"

func TestAppendOptionsValid(t *testing.T) {
	for _, test := range []struct {
		name            string
		opts            *AppendOptions
		wantErrContains string
	}{
		{
			name: "Valid",
			opts: NewAppendOptions().WithCheckpointSigner(mustCreateSigner(t, testSignerKey)),
		}, {
			name: "Valid: CheckpointRepublishInterval == CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(10 * time.Second),
		}, {
			name: "Error: CheckpointRepublishInterval < CheckpointInterval",
			opts: NewAppendOptions().
				WithCheckpointSigner(mustCreateSigner(t, testSignerKey)).
				WithCheckpointInterval(10 * time.Second).
				WithCheckpointRepublishInterval(9 * time.Second),
			wantErrContains: "WithCheckpointRepublishInterval",
		}, {
			name:            "Error: No CheckpointSigner",
			opts:            NewAppendOptions(),
			wantErrContains: "WithCheckpointSigner",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.valid()
			switch gotErr, wantErr := err != nil, test.wantErrContains != ""; {
			case gotErr && !wantErr:
				t.Fatalf("Got unexpected error %q, want no error", err)
			case !gotErr && wantErr:
				t.Fatalf("Got no error, expected error")
			case gotErr:
				if !strings.Contains(err.Error(), test.wantErrContains) {
					t.Fatalf("Got err %q, want error containing %q", err.Error(), test.wantErrContains)
				}
			}
		})
	}
}

func TestMaxEntrySize(t *testing.T) {
	d := func(_ context.Context, e *Entry) IndexFuture {
		return func() (Index, error) {
			return Index{}, nil
		}
	}

	const limit = 128
	add := entrySizeLimitDecorator(d, limit)

	for _, test := range []struct {
		name    string
		size    uint
		wantErr bool
	}{
		{
			name: "< limit",
			size: limit - 1,
		}, {
			name: "== limit",
			size: limit,
		}, {
			name:    "> limit",
			size:    limit + 1,
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := add(t.Context(), NewEntry(make([]byte, test.size)))()
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("Got err %q, want err? %T", err, test.wantErr)
			}
		})
	}
}

func mustCreateSigner(t *testing.T, k string) note.Signer {
	t.Helper()
	s, err := note.NewSigner(k)
	if err != nil {
		t.Fatalf("Failed to create signer: %v", err)
	}
	return s
}

func TestShutdownBehavior(t *testing.T) {
	tests := []struct {
		name         string
		wantTreeSize uint64
		cpSize uint64
		expectWait   bool
	}{
		{
			name:         "no work done",
			wantTreeSize: 0,
			expectWait:   false,
		},
		{
			name:         "wait for index 0",
			wantTreeSize: 1,
			cpSize:       0,
			expectWait:   true,
		},
		{
			name:         "already caught up",
			wantTreeSize: 1,
			cpSize:       1,
			expectWait:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			term := &terminator{
				readCheckpoint: func(ctx context.Context) ([]byte, error) {
					// Return a valid checkpoint string that parse.CheckpointUnsafe can parse.
					return []byte(fmt.Sprintf("example.com\n%d\nqINS1GRFhWHwdkUeqLEoP4yEMkTBBzxBkGwGQlVlVcs=\n", test.cpSize)), nil
				},
				shutdownTimeout: 10 * time.Millisecond,
			}
			term.wantTreeSize.Store(test.wantTreeSize)

			// If we've added an entry, then the terminator should wait for a checkpoint covering it.
			// Since we don't provide any checkpoints, we can detect this by waiting for it to timeout.
			err := term.Shutdown(t.Context())
			if gotTimeout := errors.Is(err, context.DeadlineExceeded); gotTimeout != test.expectWait {
				t.Fatalf("Expected timeout error from waiting for checkpoint to catch up: %v, got timeout: %v, err: %v", test.expectWait, gotTimeout, err)
			}
		})
	}
}

func TestAddUpdatesWantTreeSize(t *testing.T) {
	wantIdx := uint64(5)
	term := &terminator{
		delegate: func(_ context.Context, _ *Entry) IndexFuture {
			return func() (Index, error) {
				return Index{Index: wantIdx}, nil
			}
		},
	}

	f := term.Add(t.Context(), nil)
	if	_, err := f(); err != nil {
		t.Fatal(err)
	}

	if got := term.wantTreeSize.Load(); got != wantIdx+1 {
		t.Fatalf("wantTreeSize should be %d after adding index %d, got %d", wantIdx+1, wantIdx, got)
	}
}

