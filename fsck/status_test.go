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

package fsck

import (
	"reflect"
	"strings"
	"testing"

	"github.com/transparency-dev/tessera/api/layout"
)

type update struct {
	idx uint64
	s   State
}

func TestUpdate(t *testing.T) {
	for _, test := range []struct {
		name       string
		size       uint64
		updates    []update
		wantRanges []Range
	}{
		{
			name: "no updates",
			size: 3 * layout.EntryBundleWidth,
			wantRanges: []Range{
				{First: 0, N: 3},
			},
		},
		{
			name: "Split head",
			size: 5 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 0, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 1, State: OK},
				{First: 1, N: 4},
			},
		},
		{
			name: "Split middle",
			size: 11 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 3, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 3},
				{First: 3, N: 1, State: OK},
				{First: 4, N: 7},
			},
		},
		{
			name: "Split tail",
			size: 13 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 12, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 12},
				{First: 12, N: 1, State: OK},
			},
		},
		{
			name: "Multi-split",
			size: 10 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 1, s: OK},
				{idx: 5, s: OK},
				{idx: 7, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 1},
				{First: 1, N: 1, State: OK},
				{First: 2, N: 3},
				{First: 5, N: 1, State: OK},
				{First: 6, N: 1},
				{First: 7, N: 1, State: OK},
				{First: 8, N: 2},
			},
		},
		{
			name: "Coalesce before",
			size: 500 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 1, s: OK},
				{idx: 0, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 2, State: OK},
				{First: 2, N: 498},
			},
		},
		{
			name: "Coalesce after",
			size: 1000 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 1, s: OK},
				{idx: 2, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 1},
				{First: 1, N: 2, State: OK},
				{First: 3, N: 997},
			},
		},
		{
			name: "Coalesce first",
			size: 10 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 0, s: OK},
				{idx: 1, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 2, State: OK},
				{First: 2, N: 8},
			},
		},
		{
			name: "Coalesce last",
			size: 10 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 9, s: OK},
				{idx: 8, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 8},
				{First: 8, N: 2, State: OK},
			},
		},
		{
			name: "Coalesce degenerate",
			size: 10 * layout.EntryBundleWidth,
			updates: []update{
				{idx: 9, s: OK},
				{idx: 7, s: OK},
				{idx: 5, s: OK},
				{idx: 3, s: OK},
				{idx: 1, s: OK},
				{idx: 8, s: OK},
				{idx: 6, s: OK},
				{idx: 4, s: OK},
				{idx: 2, s: OK},
				{idx: 0, s: OK},
			},
			wantRanges: []Range{
				{First: 0, N: 10, State: OK},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRangeTracker(test.size)
			for _, u := range test.updates {
				s.Update(-1, u.idx, u.s)
			}
			p := s.entries.Front()
			for i, want := range test.wantRanges {
				if p == nil {
					t.Fatalf("got %d entry ranges, want %d", i-1, len(test.wantRanges))
				}
				got := p.Value.(*Range)
				if !reflect.DeepEqual(*got, want) {
					t.Errorf("Got range at index %d:\n%+v\nwant:\n%+v", i, *got, want)
					t.Errorf("DUMP:\n%s", strings.Join(s.dump(), "\n"))
				}
				p = p.Next()
			}
			if p != nil {
				t.Errorf("got more ranges than expected")
			}
		})
	}

}
