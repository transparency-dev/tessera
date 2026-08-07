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

package landmark

import (
	"reflect"
	"testing"
)

func mustNew(t *testing.T, lastLandmark, numActive uint64, treeSizes []uint64) *Landmarks {
	t.Helper()
	lm, err := newLandmarks(lastLandmark, numActive, treeSizes)
	if err != nil {
		t.Fatalf("newLandmarks(%d, %d, %v) unexpected error: %v", lastLandmark, numActive, treeSizes, err)
	}
	return lm
}

func TestNewLandmarks(t *testing.T) {
	tests := []struct {
		name         string
		lastLandmark uint64
		numActive    uint64
		treeSizes    []uint64
		wantErr      bool
	}{
		{
			name:         "valid active landmarks with landmark 0",
			lastLandmark: 1,
			numActive:    1,
			treeSizes:    []uint64{100, 0},
		},
		{
			name:         "valid initial landmarks zero active",
			lastLandmark: 0,
			numActive:    0,
			treeSizes:    []uint64{0},
		},
		{
			name:         "valid active landmarks with pruned landmark 0",
			lastLandmark: 2,
			numActive:    1,
			treeSizes:    []uint64{200, 100},
		},
		{
			name:         "invalid landmark 0 non-zero tree size",
			lastLandmark: 1,
			numActive:    1,
			treeSizes:    []uint64{100, 50},
			wantErr:      true,
		},
		{
			name:         "invalid initial landmark 0 non-zero tree size",
			lastLandmark: 0,
			numActive:    0,
			treeSizes:    []uint64{100},
			wantErr:      true,
		},
		{
			name:         "invalid pruned active landmark with zero tree size",
			lastLandmark: 2,
			numActive:    1,
			treeSizes:    []uint64{200, 0},
			wantErr:      true,
		},
		{
			name:         "numActive greater than lastLM",
			lastLandmark: 1,
			numActive:    2,
			treeSizes:    []uint64{200, 100, 0},
			wantErr:      true,
		},
		{
			name:         "mismatch tree sizes length",
			lastLandmark: 1,
			numActive:    1,
			treeSizes:    []uint64{100},
			wantErr:      true,
		},
		{
			name:         "not decreasing tree sizes",
			lastLandmark: 2,
			numActive:    2,
			treeSizes:    []uint64{100, 200, 0},
			wantErr:      true,
		},
		{
			name:         "equal consecutive tree sizes",
			lastLandmark: 2,
			numActive:    2,
			treeSizes:    []uint64{200, 100, 100},
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newLandmarks(tc.lastLandmark, tc.numActive, tc.treeSizes)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newLandmarks() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseLandmarks(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    func(t *testing.T) *Landmarks
		wantErr bool
	}{
		{
			name:  "valid active landmarks",
			input: "1 1\n100\n0\n",
			want:  func(t *testing.T) *Landmarks { return mustNew(t, 1, 1, []uint64{100, 0}) },
		},
		{
			name:  "valid initial landmarks zero active",
			input: "0 0\n0\n",
			want:  func(t *testing.T) *Landmarks { return mustNew(t, 0, 0, []uint64{0}) },
		},
		{
			name:    "invalid landmark 0 non-zero tree size",
			input:   "1 1\n100\n50\n",
			wantErr: true,
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "no trailing newline",
			input:   "1 1\n100\n0",
			wantErr: true,
		},
		{
			name:    "invalid header line format",
			input:   "1\n100\n",
			wantErr: true,
		},
		{
			name:    "invalid integer in header",
			input:   "abc 1\n100\n0\n",
			wantErr: true,
		},
		{
			name:    "invalid integer in tree sizes",
			input:   "1 1\nabc\n0\n",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := &Landmarks{}
			err := got.UnmarshalText([]byte(tc.input))
			if (err != nil) != tc.wantErr {
				t.Fatalf("UnmarshalText() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				want := tc.want(t)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("UnmarshalText() = %+v, want %+v", got, want)
				}
			}
		})
	}
}

func TestMarshalText(t *testing.T) {
	lm := mustNew(t, 2, 2, []uint64{200, 100, 0})
	data, err := lm.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText() unexpected error: %v", err)
	}

	got := &Landmarks{}
	if err := got.UnmarshalText(data); err != nil {
		t.Fatalf("UnmarshalText() unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, lm) {
		t.Errorf("Roundtrip result = %+v, want %+v", got, lm)
	}

	// Marshaling on uninitialized Landmarks struct should return error
	var emptyLM Landmarks
	if _, err := emptyLM.MarshalText(); err == nil {
		t.Errorf("MarshalText on uninitialized struct expected error, got nil")
	}
}

func TestAddLandmark(t *testing.T) {
	lm := mustNew(t, 0, 0, []uint64{0})
	if err := lm.AddLandmark(100, 2); err != nil {
		t.Fatalf("AddLandmark(100, 2) error: %v", err)
	}
	want1 := &Landmarks{lastLandmark: 1, numActiveLandmarks: 1, treeSizes: []uint64{100, 0}}
	if !reflect.DeepEqual(lm, want1) {
		t.Errorf("After 1st AddLandmark = %+v, want %+v", lm, want1)
	}

	if err := lm.AddLandmark(200, 2); err != nil {
		t.Fatalf("AddLandmark(200, 2) error: %v", err)
	}
	want2 := &Landmarks{lastLandmark: 2, numActiveLandmarks: 2, treeSizes: []uint64{200, 100, 0}}
	if !reflect.DeepEqual(lm, want2) {
		t.Errorf("After 2nd AddLandmark = %+v, want %+v", lm, want2)
	}

	// Adding a 3rd landmark when maxActive is 2 must prune the oldest landmark (size 0)
	if err := lm.AddLandmark(300, 2); err != nil {
		t.Fatalf("AddLandmark(300, 2) error: %v", err)
	}
	want3 := &Landmarks{lastLandmark: 3, numActiveLandmarks: 2, treeSizes: []uint64{300, 200, 100}}
	if !reflect.DeepEqual(lm, want3) {
		t.Errorf("After 3rd AddLandmark (pruned) = %+v, want %+v", lm, want3)
	}

	// Adding landmark on uninitialized Landmarks struct should return error
	var emptyLM Landmarks
	if err := emptyLM.AddLandmark(100, 2); err == nil {
		t.Errorf("AddLandmark on uninitialized struct expected error, got nil")
	}

	// Adding landmark with size smaller or equal to current last landmark should fail
	if err := lm.AddLandmark(250, 2); err == nil {
		t.Errorf("AddLandmark(250, 2) expected error for size <= last landmark, got nil")
	}

	// Adding landmark with maxActive 0 should fail
	if err := lm.AddLandmark(400, 0); err == nil {
		t.Errorf("AddLandmark(400, 0) expected error for maxActive == 0, got nil")
	}
}

