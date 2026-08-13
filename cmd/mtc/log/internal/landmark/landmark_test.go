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
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func mustNew(t *testing.T, lastLandmark, numActive uint64, treeSizes []uint64) *ActiveLandmarks {
	t.Helper()
	lm, err := newActiveLandmarks(lastLandmark, numActive, treeSizes)
	if err != nil {
		t.Fatalf("newActiveLandmarks(%d, %d, %v) unexpected error: %v", lastLandmark, numActive, treeSizes, err)
	}
	return lm
}

func TestNewActiveLandmarks(t *testing.T) {
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
			_, err := newActiveLandmarks(tc.lastLandmark, tc.numActive, tc.treeSizes)
			if (err != nil) != tc.wantErr {
				t.Fatalf("newActiveLandmarks() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseLandmarks(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    func(t *testing.T) *ActiveLandmarks
		wantErr bool
	}{
		{
			name:  "valid active landmarks",
			input: "1 1\n100\n0\n",
			want:  func(t *testing.T) *ActiveLandmarks { return mustNew(t, 1, 1, []uint64{100, 0}) },
		},
		{
			name:  "valid initial landmarks zero active",
			input: "0 0\n0\n",
			want:  func(t *testing.T) *ActiveLandmarks { return mustNew(t, 0, 0, []uint64{0}) },
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
			got := &ActiveLandmarks{}
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

	got := &ActiveLandmarks{}
	if err := got.UnmarshalText(data); err != nil {
		t.Fatalf("UnmarshalText() unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, lm) {
		t.Errorf("Roundtrip result = %+v, want %+v", got, lm)
	}

	// Marshaling on uninitialized ActiveLandmarks struct should return error
	var emptyLM ActiveLandmarks
	if _, err := emptyLM.MarshalText(); err == nil {
		t.Errorf("MarshalText on uninitialized struct expected error, got nil")
	}
}

func TestAddLandmark(t *testing.T) {
	lm := mustNew(t, 0, 0, []uint64{0})
	if err := lm.AddLandmark(100, 2); err != nil {
		t.Fatalf("AddLandmark(100, 2) error: %v", err)
	}
	want1 := &ActiveLandmarks{lastLandmark: 1, numActiveLandmarks: 1, treeSizes: []uint64{100, 0}}
	if !reflect.DeepEqual(lm, want1) {
		t.Errorf("After 1st AddLandmark = %+v, want %+v", lm, want1)
	}

	if err := lm.AddLandmark(200, 2); err != nil {
		t.Fatalf("AddLandmark(200, 2) error: %v", err)
	}
	want2 := &ActiveLandmarks{lastLandmark: 2, numActiveLandmarks: 2, treeSizes: []uint64{200, 100, 0}}
	if !reflect.DeepEqual(lm, want2) {
		t.Errorf("After 2nd AddLandmark = %+v, want %+v", lm, want2)
	}

	// Adding a 3rd landmark when maxActive is 2 must prune the oldest landmark (size 0)
	if err := lm.AddLandmark(300, 2); err != nil {
		t.Fatalf("AddLandmark(300, 2) error: %v", err)
	}
	want3 := &ActiveLandmarks{lastLandmark: 3, numActiveLandmarks: 2, treeSizes: []uint64{300, 200, 100}}
	if !reflect.DeepEqual(lm, want3) {
		t.Errorf("After 3rd AddLandmark (pruned) = %+v, want %+v", lm, want3)
	}

	// Adding landmark on uninitialized ActiveLandmarks struct should return error
	var emptyLM ActiveLandmarks
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

func TestActiveLandmarks_GetSubtreeFor(t *testing.T) {
	tests := []struct {
		name      string
		landmarks *ActiveLandmarks
		index     uint64
		wantStart uint64
		wantEnd   uint64
		wantErr   bool
	}{
		{
			name:      "index too old (pruned)",
			landmarks: mustNew(t, 3, 2, []uint64{150, 100, 50}),
			index:     25,
			wantErr:   true,
		},
		{
			name:      "index not covered (exceeds latest landmark)",
			landmarks: mustNew(t, 3, 3, []uint64{150, 100, 50, 0}),
			index:     160,
			wantErr:   true,
		},
		{
			name:      "index 25 in landmark [0, 50)",
			landmarks: mustNew(t, 3, 3, []uint64{150, 100, 50, 0}),
			index:     25,
			wantStart: 0,
			wantEnd:   32,
		},
		{
			name:      "index 40 in landmark [0, 50)",
			landmarks: mustNew(t, 3, 3, []uint64{150, 100, 50, 0}),
			index:     40,
			wantStart: 32,
			wantEnd:   50,
		},
		{
			name:      "index 75 in landmark [50, 100)",
			landmarks: mustNew(t, 3, 3, []uint64{150, 100, 50, 0}),
			index:     75,
			wantStart: 64,
			wantEnd:   100,
		},
		{
			name:      "index 130 in landmark [100, 150)",
			landmarks: mustNew(t, 3, 3, []uint64{150, 100, 50, 0}),
			index:     130,
			wantStart: 128,
			wantEnd:   150,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, e, err := tc.landmarks.GetSubtreeFor(tc.index)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetSubtreeFor(%d) error = %v, wantErr %v", tc.index, err, tc.wantErr)
			}
			if !tc.wantErr {
				if s != tc.wantStart || e != tc.wantEnd {
					t.Errorf("GetSubtreeFor(%d) = [%d, %d), want [%d, %d)", tc.index, s, e, tc.wantStart, tc.wantEnd)
				}
				if tc.index < s || tc.index >= e {
					t.Errorf("GetSubtreeFor(%d) returned range [%d, %d) that does not contain index", tc.index, s, e)
				}
			}
		})
	}
}

type mockStorage struct {
	mu      sync.Mutex
	data    []byte
	modTime time.Time
}

func (m *mockStorage) ReadLandmarks(ctx context.Context) ([]byte, time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.data) == 0 {
		return nil, time.Time{}, os.ErrNotExist
	}
	return m.data, m.modTime, nil
}

func (m *mockStorage) UpdateLandmarks(ctx context.Context, fn func(old []byte, oldModTime time.Time) ([]byte, error)) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	newData, err := fn(m.data, m.modTime)
	if err != nil {
		return time.Time{}, err
	}
	if newData != nil && !bytes.Equal(m.data, newData) {
		m.data = newData
		m.modTime = time.Now()
	}
	return m.modTime, nil
}

func (m *mockStorage) setModTime(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.modTime = t
}

func TestPublisher_Initialise(t *testing.T) {
	ctx := context.Background()
	existingLM := mustNew(t, 1, 1, []uint64{50, 0})
	existingBytes, _ := existingLM.MarshalText()

	testCases := []struct {
		name          string
		initialData   []byte
		wantLandmarks *ActiveLandmarks
	}{
		{
			name:          "no existing resource: initialises landmark 0",
			initialData:   nil,
			wantLandmarks: mustNew(t, 0, 0, []uint64{0}),
		},
		{
			name:          "existing resource: loads existing landmarks",
			initialData:   existingBytes,
			wantLandmarks: existingLM,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			loopCtx, cancelLoop := context.WithCancel(ctx)
			memStorage := &mockStorage{data: tc.initialData}
			_, err := NewPublisher(loopCtx, func() uint64 { return 0 }, memStorage, 24*time.Hour, 1*time.Hour)
			if err != nil {
				t.Fatalf("NewPublisher() error: %v", err)
			}
			cancelLoop()

			data, _, err := memStorage.ReadLandmarks(ctx)
			if err != nil {
				t.Fatalf("memStorage.ReadLandmarks() error: %v", err)
			}
			got := &ActiveLandmarks{}
			if err := got.UnmarshalText(data); err != nil {
				t.Fatalf("UnmarshalText() error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.wantLandmarks) {
				t.Errorf("stored landmarks = %+v, want %+v", got, tc.wantLandmarks)
			}
		})
	}
}

func TestPublisher_Update(t *testing.T) {
	ctx := context.Background()
	loopCtx, cancelLoop := context.WithCancel(ctx)
	currentSize := uint64(0)
	readCheckpointSize := func() uint64 {
		return currentSize
	}

	memStorage := &mockStorage{}
	// maxCertLifetime = 1h, pubInterval = 1h => maxActive = ceil(1/1) + 1 = 2
	pub, err := NewPublisher(loopCtx, readCheckpointSize, memStorage, 1*time.Hour, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewPublisher() error: %v", err)
	}
	cancelLoop()

	// Verify initial state (landmark 0 at size 0).
	wantInit := mustNew(t, 0, 0, []uint64{0})
	data, _, err := memStorage.ReadLandmarks(ctx)
	if err != nil {
		t.Fatalf("memStorage.ReadLandmarks() error: %v", err)
	}
	gotInit := &ActiveLandmarks{}
	if err := gotInit.UnmarshalText(data); err != nil {
		t.Fatalf("UnmarshalText() error: %v", err)
	}
	if !reflect.DeepEqual(gotInit, wantInit) {
		t.Fatalf("initial landmarks = %+v, want %+v", gotInit, wantInit)
	}

	now := time.Now()
	testCases := []struct {
		name          string
		currentSize   uint64
		lastPublished time.Time
		wantLandmarks *ActiveLandmarks
		wantErr       bool
	}{
		{
			name:          "skip because last update too recent",
			currentSize:   50,
			lastPublished: now,
			wantLandmarks: mustNew(t, 0, 0, []uint64{0}),
		},
		{
			name:          "publish new landmark on tree growth after interval",
			currentSize:   50,
			lastPublished: now.Add(-2 * time.Hour),
			wantLandmarks: mustNew(t, 1, 1, []uint64{50, 0}),
		},
		{
			name:          "skip because tree has not grown",
			currentSize:   50,
			lastPublished: now.Add(-2 * time.Hour),
			wantLandmarks: mustNew(t, 1, 1, []uint64{50, 0}),
		},
		{
			name:          "publish next landmark on further tree growth without pruning (maxActive is 2 due to +1)",
			currentSize:   100,
			lastPublished: now.Add(-2 * time.Hour),
			wantLandmarks: mustNew(t, 2, 2, []uint64{100, 50, 0}),
		},
		{
			name:          "publish next landmark on further tree growth with pruning",
			currentSize:   150,
			lastPublished: now.Add(-2 * time.Hour),
			wantLandmarks: mustNew(t, 3, 2, []uint64{150, 100, 50}),
		},
		{
			name:          "error when checkpoint size is smaller than last landmark size",
			currentSize:   120,
			lastPublished: now.Add(-2 * time.Hour),
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			currentSize = tc.currentSize
			memStorage.setModTime(tc.lastPublished)

			nextIn, err := pub.Update(ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Update() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if nextIn <= 0 {
				t.Errorf("Update() nextIn = %v, want > 0", nextIn)
			}
			data, _, err := memStorage.ReadLandmarks(ctx)
			if err != nil {
				t.Fatalf("memStorage.ReadLandmarks() error: %v", err)
			}
			got := &ActiveLandmarks{}
			if err := got.UnmarshalText(data); err != nil {
				t.Fatalf("UnmarshalText() error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.wantLandmarks) {
				t.Errorf("published landmarks = %+v, want %+v", got, tc.wantLandmarks)
			}
		})
	}
}

func TestNewPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dummyReader := func() uint64 { return 0 }
	dummyStorage := &mockStorage{}

	tests := []struct {
		name               string
		readCheckpointSize ReadCheckpointSize
		storage            LandmarksStorage
		maxCertLifetime    time.Duration
		pubInterval        time.Duration
		wantErr            bool
		wantMaxActive      uint64
	}{
		{
			name:               "valid exact division",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    24 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantMaxActive:      25, // ceil(24/1) + 1 = 25
		},
		{
			name:               "valid inexact division rounds up plus one",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    25 * time.Hour,
			pubInterval:        2 * time.Hour,
			wantMaxActive:      14, // ceil(12.5) + 1 = 13 + 1 = 14
		},
		{
			name:               "valid lifetime equal to interval",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    1 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantMaxActive:      2, // ceil(1/1) + 1 = 2
		},
		{
			name:               "valid 47-day lifetime with 4-hour interval",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    47 * 24 * time.Hour,
			pubInterval:        4 * time.Hour,
			wantMaxActive:      283, // ceil(1128/4) + 1 = 283
		},
		{
			name:               "pubInterval exceeds maxCertLifetime",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    10 * time.Minute,
			pubInterval:        15 * time.Minute,
			wantErr:            true,
		},
		{
			name:               "maxActive exceeds limit (47 days with 1-hour interval yields 1129 > 370)",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    47 * 24 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantErr:            true,
		},
		{
			name:               "nil storage",
			readCheckpointSize: dummyReader,
			storage:            nil,
			maxCertLifetime:    24 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantErr:            true,
		},
		{
			name:               "nil readCheckpointSize",
			readCheckpointSize: nil,
			storage:            dummyStorage,
			maxCertLifetime:    24 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantErr:            true,
		},
		{
			name:               "zero maxCertLifetime",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    0,
			pubInterval:        1 * time.Hour,
			wantErr:            true,
		},
		{
			name:               "negative maxCertLifetime",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    -1 * time.Hour,
			pubInterval:        1 * time.Hour,
			wantErr:            true,
		},
		{
			name:               "zero pubInterval",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    24 * time.Hour,
			pubInterval:        0,
			wantErr:            true,
		},
		{
			name:               "negative pubInterval",
			readCheckpointSize: dummyReader,
			storage:            dummyStorage,
			maxCertLifetime:    24 * time.Hour,
			pubInterval:        -1 * time.Hour,
			wantErr:            true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub, err := NewPublisher(ctx, tc.readCheckpointSize, tc.storage, tc.maxCertLifetime, tc.pubInterval)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewPublisher() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && pub.maxActive != tc.wantMaxActive {
				t.Errorf("pub.maxActive = %d, want %d", pub.maxActive, tc.wantMaxActive)
			}
		})
	}
}

func TestPublisher_GetSubtreeFor(t *testing.T) {
	ctx := context.Background()
	treeSize := uint64(150)
	pubInterval := 1 * time.Hour
	memStorage := &mockStorage{}

	pub, err := NewPublisher(ctx, func() uint64 { return treeSize }, memStorage, 24*time.Hour, pubInterval)
	if err != nil {
		t.Fatalf("NewPublisher() error: %v", err)
	}

	tests := []struct {
		name           string
		active         *ActiveLandmarks
		pubAt          time.Time
		index          uint64
		wantStart      uint64
		wantEnd        uint64
		wantRetryAfter bool
		wantErrTooOld  bool
		wantErr        bool
	}{
		{
			name:           "uninitialized published state returns retryAfter",
			active:         nil,
			index:          10,
			wantRetryAfter: true,
		},
		{
			name:      "covered index in active landmark returns subtree range",
			active:    mustNew(t, 2, 2, []uint64{100, 50, 0}),
			pubAt:     time.Now(),
			index:     75,
			wantStart: 64,
			wantEnd:   100,
		},
		{
			name:           "in-flight index within tree size returns retryAfter",
			active:         mustNew(t, 2, 2, []uint64{100, 50, 0}),
			pubAt:          time.Now(),
			index:          120,
			wantRetryAfter: true,
		},
		{
			name:    "index beyond tree size returns error",
			active:  mustNew(t, 2, 2, []uint64{100, 50, 0}),
			pubAt:   time.Now(),
			index:   160,
			wantErr: true,
		},
		{
			name:          "pruned index older than oldest active landmark returns ErrTooOld",
			active:        mustNew(t, 3, 2, []uint64{150, 100, 50}),
			pubAt:         time.Now(),
			index:         25,
			wantErrTooOld: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pub.mu.Lock()
			if tc.active != nil {
				pub.active = *tc.active
			} else {
				pub.active = ActiveLandmarks{}
			}
			pub.pubAt = tc.pubAt
			pub.mu.Unlock()

			s, e, retry, err := pub.GetSubtreeFor(ctx, tc.index)
			if tc.wantErrTooOld {
				if !errors.Is(err, ErrTooOld) {
					t.Fatalf("GetSubtreeFor(%d) error = %v, want %v", tc.index, err, ErrTooOld)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatalf("GetSubtreeFor(%d) expected error, got nil", tc.index)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetSubtreeFor(%d) unexpected error: %v", tc.index, err)
			}

			if tc.wantRetryAfter {
				if retry <= 0 {
					t.Errorf("GetSubtreeFor(%d) retry = %v, want > 0", tc.index, retry)
				}
				return
			}

			if retry != 0 {
				t.Errorf("GetSubtreeFor(%d) retry = %v, want 0", tc.index, retry)
			}
			if s != tc.wantStart || e != tc.wantEnd {
				t.Errorf("GetSubtreeFor(%d) = [%d, %d), want [%d, %d)", tc.index, s, e, tc.wantStart, tc.wantEnd)
			}
		})
	}
}
