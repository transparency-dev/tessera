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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
)

func TestTrimFullToPartial(t *testing.T) {
	for _, test := range []struct {
		name       string
		r          resource
		storedTile []byte
		wantErr    bool
	}{
		{
			name: "partial request, partial exists",
			r: resource{
				partial: 10,
				content: makeTile(10),
			},
			storedTile: makeTile(10),
		},
		{
			name: "partial request, full exists",
			r: resource{
				partial: 10,
				content: makeTile(10),
			},
			storedTile: makeTile(256),
		},
		{
			name: "invalid stored data",
			r: resource{
				content: makeTile(256),
			},
			storedTile: makeTile(2),
			wantErr:    true,
		},
		{
			name: "full request, full exists",
			r: resource{
				content: makeTile(256),
			},
			storedTile: makeTile(256),
		},
	} {
		t.Run(test.name, func(t *testing.T) {

			f := &fsckTree{
				expectedResources: make(chan resource, 1),
				fetcher: &fakeFetcher{
					tile: test.storedTile,
				},
			}

			f.expectedResources <- test.r
			close(f.expectedResources)

			err := f.resourceCheckWorker(t.Context())()
			if gotErr := err != nil; gotErr != test.wantErr {
				t.Fatalf("resourceCheckWorker: %v want err %t", err, test.wantErr)
			}
		})
	}
}

type fakeFetcher struct {
	tile []byte
}

func (f *fakeFetcher) ReadCheckpoint(_ context.Context) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeFetcher) ReadTile(_ context.Context, _, _ uint64, _ uint8) ([]byte, error) {
	return f.tile, nil
}

func (f *fakeFetcher) ReadEntryBundle(_ context.Context, _ uint64, _ uint8) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func makeTile(n int) []byte {
	r := make([]byte, 0, n*sha256.Size)
	for i := range n {
		h := sha256.Sum256(fmt.Appendf(nil, "%50d", i))
		r = append(r, h[:]...)
	}
	return r
}
