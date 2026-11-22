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
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/transparency-dev/tessera/api/layout"
)

func TestCopier_ZeroWorkersReturnsError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		setBundle := func(context.Context, uint64, uint8, []byte) error {
			t.Fatal("setEntryBundle called with 0 workers")
			return nil
		}
		getEntries := func(context.Context, uint64, uint8) ([]byte, error) {
			t.Fatal("getEntryBundle called with 0 workers")
			return nil, nil
		}
		c := newCopier(0, setBundle, getEntries)

		if err := c.Copy(ctx, 0, 2*layout.EntryBundleWidth); err == nil {
			t.Fatal("Copy() = nil, want error")
		}
	})
}

func TestCopier_CallerCancelReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	setBundle := func(context.Context, uint64, uint8, []byte) error {
		return nil
	}
	// The callbacks ignore ctx and succeed, so cancellation is detected by the copier loops rather than the callbacks.
	getEntries := func(context.Context, uint64, uint8) ([]byte, error) {
		cancel()
		return []byte("bundle"), nil
	}
	c := newCopier(1, setBundle, getEntries)

	if err := c.Copy(ctx, 0, 1000*layout.EntryBundleWidth); !errors.Is(err, context.Canceled) {
		t.Errorf("Copy() = %v, want error wrapping %v", err, context.Canceled)
	}
}

func TestCopier_WorkerErrorCancelsPopulateAndSiblings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		setBundle := func(context.Context, uint64, uint8, []byte) error {
			return nil
		}
		getEntries := func(ctx context.Context, idx uint64, _ uint8) ([]byte, error) {
			if idx == 0 {
				return nil, errors.New("fetch error")
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		c := newCopier(2, setBundle, getEntries)

		err := c.Copy(ctx, 0, 6*layout.EntryBundleWidth)
		if err == nil || !strings.Contains(err.Error(), "fetch error") {
			t.Errorf("Copy() = %v, want error containing %q", err, "fetch error")
		}

		closed := false
		for range cap(c.todo) + 1 {
			select {
			case _, ok := <-c.todo:
				if !ok {
					closed = true
				}
			default:
			}
			if closed {
				break
			}
		}
		if !closed {
			t.Errorf("copier work channel still open after Copy returned error; populateWork leaked")
			for range c.todo {
			}
		}
	})
}
