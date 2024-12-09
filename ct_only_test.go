// Copyright 2024 Google LLC. All Rights Reserved.
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
	"fmt"
	"testing"
)

func TestCTEntriesPath(t *testing.T) {
	for _, test := range []struct {
		N        uint64
		p        uint8
		wantPath string
	}{
		{
			N:        0,
			wantPath: "tile/data/000",
		},
		{
			N:        0,
			p:        8,
			wantPath: "tile/data/000.p/8",
		}, {
			N:        255,
			wantPath: "tile/data/255",
		}, {
			N:        255,
			p:        253,
			wantPath: "tile/data/255.p/253",
		}, {
			N:        256,
			wantPath: "tile/data/256",
		}, {
			N:        123456789000,
			wantPath: "tile/data/x123/x456/x789/000",
		},
	} {
		desc := fmt.Sprintf("N %d", test.N)
		t.Run(desc, func(t *testing.T) {
			gotPath := ctEntriesPath(test.N, test.p)
			if gotPath != test.wantPath {
				t.Errorf("got file %q want %q", gotPath, test.wantPath)
			}
		})
	}
}
