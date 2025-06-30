// Copyright 2025 Google LLC. All Rights Reserved.
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

package fetcher

import (
	"context"
	"os"
	"testing"
)

func TestFetchPartialOrFullResource(t *testing.T) {
	for _, test := range []struct {
		name        string
		p           uint8
		responses   []error
		expectCalls int
		wantErr     bool
	}{
		{
			name:      "partial resource found",
			p:         23,
			responses: []error{nil},
		},
		{
			name:      "partial resource missing, full resource found",
			p:         23,
			responses: []error{os.ErrNotExist, nil},
		},
		{
			name:      "partial resource missing, full resource missing",
			p:         23,
			responses: []error{os.ErrNotExist, os.ErrNotExist},
			wantErr:   true,
		},
		{
			name:      "full resource found",
			responses: []error{nil},
		},
		{
			name:      "full resource missing",
			responses: []error{os.ErrNotExist},
			wantErr:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			i := 0
			_, gotE := PartialOrFullResource(t.Context(), test.p, func(ctx context.Context, u uint8) ([]byte, error) {
				defer func() {
					i++
				}()
				return []byte("ret"), test.responses[i]
			})
			if gotErr := gotE != nil; gotErr != test.wantErr {
				t.Fatalf("got error %v, want err %t", gotErr, test.wantErr)
			}
		})

	}

}
