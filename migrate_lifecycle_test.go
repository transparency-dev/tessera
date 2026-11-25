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

package tessera

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/migrate"
)

type fakeMigrationDriver struct {
	writer migrate.MigrationWriter
	reader LogReader
}

func (d *fakeMigrationDriver) MigrationWriter(context.Context, *MigrationOptions) (migrate.MigrationWriter, LogReader, error) {
	return d.writer, d.reader, nil
}

type fakeMigrationWriter struct {
	awaitIntegrationFunc func(ctx context.Context, size uint64) ([]byte, error)
	sizeFunc             func(ctx context.Context) (uint64, error)
}

func (w *fakeMigrationWriter) SetEntryBundle(context.Context, uint64, uint8, []byte) error {
	return nil
}

func (w *fakeMigrationWriter) AwaitIntegration(ctx context.Context, size uint64) ([]byte, error) {
	if w.awaitIntegrationFunc != nil {
		return w.awaitIntegrationFunc(ctx, size)
	}
	return nil, nil
}

func (w *fakeMigrationWriter) IntegratedSize(ctx context.Context) (uint64, error) {
	if w.sizeFunc != nil {
		return w.sizeFunc(ctx)
	}
	return 0, nil
}

type fakeAntispam struct {
	follower Follower
}

func (a *fakeAntispam) Decorator() func(AddFn) AddFn {
	return func(next AddFn) AddFn { return next }
}

func (a *fakeAntispam) Follower(func([]byte) ([][]byte, error)) Follower {
	return a.follower
}

// fakeFollower is a Follower whose Follow blocks until ctx is cancelled
// and whose EntriesProcessed never advances.
type fakeFollower struct {
	done chan struct{}
}

func (f *fakeFollower) Name() string {
	return "fake-follower"
}

func (f *fakeFollower) Follow(ctx context.Context, _ LogReader) {
	<-ctx.Done()
	if f.done != nil {
		close(f.done)
	}
}

func (f *fakeFollower) EntriesProcessed(context.Context) (uint64, error) {
	return 0, nil
}

// TestMigrate_ErrorCancelsGoroutines verifies that when Copy or AwaitIntegration
// fails, Migrate cancels sibling goroutines (including followers) and returns
// promptly without waiting for the caller context to expire.
func TestMigrate_ErrorCancelsGoroutines(t *testing.T) {
	wantIntegrationErr := errors.New("permanent integration error")

	for _, tc := range []struct {
		name          string
		sourceSize    uint64
		writer        *fakeMigrationWriter
		withFollower  bool
		getEntries    client.EntryBundleFetcherFunc
		wantErr       error
		wantErrSubstr string
	}{
		{
			name:       "copy_error_cancels_await_integration",
			sourceSize: 10,
			writer: &fakeMigrationWriter{
				// IntegratedSize > sourceSize causes copier.Copy to fail immediately.
				sizeFunc: func(context.Context) (uint64, error) {
					return 20, nil
				},
				awaitIntegrationFunc: func(ctx context.Context, _ uint64) ([]byte, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
			getEntries: func(context.Context, uint64, uint8) ([]byte, error) {
				return nil, nil
			},
		},
		{
			name:       "copy_worker_error_cancels_await_integration_and_followers",
			sourceSize: 10,
			writer: &fakeMigrationWriter{
				awaitIntegrationFunc: func(ctx context.Context, _ uint64) ([]byte, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
			withFollower: true,
			getEntries: func(context.Context, uint64, uint8) ([]byte, error) {
				return nil, errors.New("fetch error")
			},
			wantErrSubstr: "fetch error",
		},
		{
			name:       "await_integration_error_cancels_copy_and_followers",
			sourceSize: 10,
			writer: &fakeMigrationWriter{
				awaitIntegrationFunc: func(context.Context, uint64) ([]byte, error) {
					return nil, wantIntegrationErr
				},
			},
			withFollower: true,
			getEntries: func(ctx context.Context, _ uint64, _ uint8) ([]byte, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
			wantErr: wantIntegrationErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()
				opts := NewMigrationOptions()
				var followerDone chan struct{}
				if tc.withFollower {
					followerDone = make(chan struct{})
					opts.WithAntispam(&fakeAntispam{
						follower: &fakeFollower{done: followerDone},
					})
				}
				mt, err := NewMigrationTarget(ctx, &fakeMigrationDriver{
					writer: tc.writer,
					reader: &fakeLogReader{},
				}, opts)
				if err != nil {
					t.Fatalf("NewMigrationTarget: %v", err)
				}

				callCtx, cancel := context.WithTimeout(ctx, time.Hour)
				defer cancel()

				err = mt.Migrate(callCtx, 1, tc.sourceSize, make([]byte, 32), tc.getEntries)
				if err == nil {
					t.Fatal("Migrate() = nil, want error")
				}
				if callCtx.Err() != nil {
					t.Errorf("Migrate() returned %v only after caller context expired (%v), want sibling goroutines cancelled promptly", err, callCtx.Err())
				}
				if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
					t.Errorf("Migrate() = %v, want error wrapping %v", err, tc.wantErr)
				}
				if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Errorf("Migrate() = %v, want error containing %q", err, tc.wantErrSubstr)
				}
				if followerDone != nil {
					select {
					case <-followerDone:
					case <-callCtx.Done():
						t.Errorf("follower Follow did not exit after Migrate returned error")
					}
				}
			})
		})
	}
}

func TestMigrate_CallerCancelWhileAwaitingFollower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		sourceRoot := bytes.Repeat([]byte{0xab}, 32)
		opts := NewMigrationOptions().WithAntispam(&fakeAntispam{
			follower: &fakeFollower{},
		})
		mt, err := NewMigrationTarget(ctx, &fakeMigrationDriver{
			writer: &fakeMigrationWriter{
				awaitIntegrationFunc: func(context.Context, uint64) ([]byte, error) {
					return sourceRoot, nil
				},
			},
			reader: &fakeLogReader{},
		}, opts)
		if err != nil {
			t.Fatalf("NewMigrationTarget: %v", err)
		}

		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		getEntries := func(context.Context, uint64, uint8) ([]byte, error) {
			return []byte("bundle"), nil
		}
		if err := mt.Migrate(callCtx, 1, 10, sourceRoot, getEntries); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Migrate() = %v, want error wrapping %v", err, context.DeadlineExceeded)
		}
	})
}

func TestMigrate_ZeroWorkersReturnsError(t *testing.T) {
	ctx := t.Context()
	mt, err := NewMigrationTarget(ctx, &fakeMigrationDriver{
		writer: &fakeMigrationWriter{
			sizeFunc: func(context.Context) (uint64, error) {
				t.Fatal("IntegratedSize called with 0 workers")
				return 0, nil
			},
		},
		reader: &fakeLogReader{},
	}, NewMigrationOptions())
	if err != nil {
		t.Fatalf("NewMigrationTarget: %v", err)
	}

	if err := mt.Migrate(ctx, 0, 10, make([]byte, 32), nil); err == nil {
		t.Fatal("Migrate() = nil, want error")
	}
}
