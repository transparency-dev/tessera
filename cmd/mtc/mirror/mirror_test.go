// Copyright 2026 The Tessera Authors. All Rights Reserved.
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

package mirror

import (
	"testing"

	"golang.org/x/mod/sumdb/note"
)

type fakeSigner struct {
	name    string
	keyHash uint32
}

func (s fakeSigner) Name() string {
	return s.name
}

func (s fakeSigner) KeyHash() uint32 {
	return s.keyHash
}

func (s fakeSigner) Sign([]byte) ([]byte, error) {
	return nil, nil
}

func TestAssertDistinctSigners(t *testing.T) {
	for _, test := range []struct {
		desc    string
		w       []note.Signer
		m       []note.Signer
		wantErr bool
	}{
		{
			desc:    "both empty",
			w:       nil,
			m:       nil,
			wantErr: false,
		},
		{
			desc:    "empty witness signers",
			w:       nil,
			m:       []note.Signer{fakeSigner{name: "mirror1", keyHash: 1}},
			wantErr: false,
		},
		{
			desc:    "empty mirror signers",
			w:       []note.Signer{fakeSigner{name: "wit1", keyHash: 1}},
			m:       nil,
			wantErr: false,
		},
		{
			desc: "distinct signers with different names and hashes",
			w: []note.Signer{
				fakeSigner{name: "wit1", keyHash: 1},
			},
			m: []note.Signer{
				fakeSigner{name: "mirror1", keyHash: 2},
			},
			wantErr: false,
		},
		{
			desc: "distinct signers with same name but different hashes",
			w: []note.Signer{
				fakeSigner{name: "signer1", keyHash: 1},
			},
			m: []note.Signer{
				fakeSigner{name: "signer1", keyHash: 2},
			},
			wantErr: false,
		},
		{
			desc: "distinct signers with different names but same hash",
			w: []note.Signer{
				fakeSigner{name: "signer1", keyHash: 1},
			},
			m: []note.Signer{
				fakeSigner{name: "signer2", keyHash: 1},
			},
			wantErr: false,
		},
		{
			desc: "overlapping signer with same name and hash",
			w: []note.Signer{
				fakeSigner{name: "signer1", keyHash: 1},
			},
			m: []note.Signer{
				fakeSigner{name: "signer1", keyHash: 1},
			},
			wantErr: true,
		},
		{
			desc: "multiple signers with one overlapping",
			w: []note.Signer{
				fakeSigner{name: "wit1", keyHash: 1},
				fakeSigner{name: "shared", keyHash: 2},
			},
			m: []note.Signer{
				fakeSigner{name: "mirror1", keyHash: 3},
				fakeSigner{name: "shared", keyHash: 2},
			},
			wantErr: true,
		},
		{
			desc: "multiple signers with no overlap",
			w: []note.Signer{
				fakeSigner{name: "wit1", keyHash: 1},
				fakeSigner{name: "wit2", keyHash: 2},
			},
			m: []note.Signer{
				fakeSigner{name: "mirror1", keyHash: 3},
				fakeSigner{name: "mirror2", keyHash: 4},
			},
			wantErr: false,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			err := assertDistinctSigners(test.w, test.m)
			if (err != nil) != test.wantErr {
				t.Errorf("assertDistinctSigners() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
