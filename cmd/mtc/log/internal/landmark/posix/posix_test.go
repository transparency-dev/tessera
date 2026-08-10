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

package posix

import (
	"bytes"
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/landmark"
)

func mustUnmarshalActiveLandmarks(t *testing.T, text string) *landmark.ActiveLandmarks {
	t.Helper()
	lm := &landmark.ActiveLandmarks{}
	if err := lm.UnmarshalText([]byte(text)); err != nil {
		t.Fatalf("UnmarshalText(%q) unexpected error: %v", text, err)
	}
	return lm
}

func TestStorage(t *testing.T) {
	dir := t.TempDir()
	storage := NewStorage(dir)
	ctx := context.Background()

	if _, _, err := storage.ReadLandmarks(ctx); !os.IsNotExist(err) {
		t.Fatalf("ReadLandmarks on missing file error = %v, want os.ErrNotExist", err)
	}

	data1, _ := mustUnmarshalActiveLandmarks(t, "1 1\n100\n0\n").MarshalText()
	data2, _ := mustUnmarshalActiveLandmarks(t, "2 2\n200\n100\n0\n").MarshalText()

	testCases := []struct {
		name        string
		updateFn    func(old []byte, oldModTime time.Time) ([]byte, error)
		wantData    []byte
		wantChanged bool
	}{
		{
			name: "initial write: file content created",
			updateFn: func(old []byte, oldModTime time.Time) ([]byte, error) {
				return data1, nil
			},
			wantData:    data1,
			wantChanged: true,
		},
		{
			name: "content unchanged: skips write and preserves modTime",
			updateFn: func(old []byte, oldModTime time.Time) ([]byte, error) {
				return old, nil
			},
			wantData:    data1,
			wantChanged: false,
		},
		{
			name: "content changed: overwrites file and updates modTime",
			updateFn: func(old []byte, oldModTime time.Time) ([]byte, error) {
				return data2, nil
			},
			wantData:    data2,
			wantChanged: true,
		},
	}

	var lastModTime time.Time
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			modTime, err := storage.UpdateLandmarks(ctx, tc.updateFn)
			if err != nil {
				t.Fatalf("UpdateLandmarks() error: %v", err)
			}
			if modTime.IsZero() {
				t.Fatalf("UpdateLandmarks() returned zero modTime")
			}

			if tc.wantChanged {
				if !lastModTime.IsZero() && modTime.Before(lastModTime) {
					t.Errorf("UpdateLandmarks() modTime = %v, expected >= %v", modTime, lastModTime)
				}
			} else {
				if !modTime.Equal(lastModTime) {
					t.Errorf("UpdateLandmarks() modTime = %v, want unchanged %v", modTime, lastModTime)
				}
			}
			lastModTime = modTime

			readData, readMod, err := storage.ReadLandmarks(ctx)
			if err != nil {
				t.Fatalf("ReadLandmarks() error: %v", err)
			}
			if !bytes.Equal(readData, tc.wantData) {
				t.Errorf("ReadLandmarks() data = %q, want %q", readData, tc.wantData)
			}
			if !readMod.Equal(modTime) {
				t.Errorf("ReadLandmarks() modTime = %v, want %v", readMod, modTime)
			}

			got := &landmark.ActiveLandmarks{}
			if err := got.UnmarshalText(readData); err != nil {
				t.Fatalf("UnmarshalText() error: %v", err)
			}
			want := &landmark.ActiveLandmarks{}
			if err := want.UnmarshalText(tc.wantData); err != nil {
				t.Fatalf("UnmarshalText(wantData) error: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("unmarshaled landmarks = %+v, want %+v", got, want)
			}
		})
	}
}


