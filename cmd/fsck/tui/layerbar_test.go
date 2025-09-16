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

// Package tui provides a Bubbletea-based TUI for the fsck command.
package tui

import (
	"testing"

	"github.com/transparency-dev/tessera/fsck"
)

func TestStateForRange(t *testing.T) {
	for _, test := range []struct {
		name string
		rs   []fsck.Range
		f, n uint64
		want stateStyle
	}{
		{
			name: "single range, all contained from LHS",
			rs:   []fsck.Range{{First: 0, N: 10, State: fsck.Calculating}},
			f:    0,
			n:    3,
			want: stateStyles[fsck.Calculating],
		},
		{
			name: "single range, all contained internal",
			rs:   []fsck.Range{{First: 0, N: 10, State: fsck.Calculating}},
			f:    1,
			n:    3,
			want: stateStyles[fsck.Calculating],
		},
		{
			name: "single range, all contained RHS",
			rs:   []fsck.Range{{First: 0, N: 10, State: fsck.Calculating}},
			f:    7,
			n:    3,
			want: stateStyles[fsck.Calculating],
		},
		{
			name: "single range, all contained full overlap",
			rs:   []fsck.Range{{First: 0, N: 10, State: fsck.Calculating}},
			f:    0,
			n:    10,
			want: stateStyles[fsck.Calculating],
		},
		{
			name: "multiple ranges, contained in first",
			rs: []fsck.Range{
				{First: 0, N: 10, State: fsck.Calculating},
				{First: 10, N: 1, State: fsck.Invalid},
				{First: 11, N: 10, State: fsck.OK},
			},
			f:    1,
			n:    3,
			want: stateStyles[fsck.Calculating],
		},
		{
			name: "multiple ranges, contained in last",
			rs: []fsck.Range{
				{First: 0, N: 10, State: fsck.Calculating},
				{First: 10, N: 1, State: fsck.Invalid},
				{First: 11, N: 10, State: fsck.OK},
			},
			f:    11,
			n:    3,
			want: stateStyles[fsck.OK],
		},
		{
			name: "multiple ranges, spans two",
			rs: []fsck.Range{
				{First: 0, N: 10, State: fsck.Calculating},
				{First: 10, N: 10, State: fsck.Invalid},
				{First: 20, N: 10, State: fsck.OK},
			},
			f:    8,
			n:    10,
			want: stateStyles[fsck.Invalid],
		},
		{
			name: "multiple ranges, spans three",
			rs: []fsck.Range{
				{First: 0, N: 10, State: fsck.Calculating},
				{First: 10, N: 10, State: fsck.Invalid},
				{First: 20, N: 10, State: fsck.OK},
				{First: 30, N: 10, State: fsck.Fetching},
			},
			f:    18,
			n:    20,
			want: stateStyles[fsck.Invalid],
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := stateForRange(test.rs, test.f, test.n)
			if got.priority != test.want.priority {
				t.Fatalf("Got %v, want %v", got, test.want)
			}
		})

	}
}
