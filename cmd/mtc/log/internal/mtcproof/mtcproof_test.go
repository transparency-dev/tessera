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

package mtcproof

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/crypto/cryptobyte"
)

// readUint48 reads a big-endian, 48-bit value from the byte string.
func readUint48(s *cryptobyte.String, out *uint64) bool {
	var b []byte
	if !s.ReadBytes(&b, 6) {
		return false
	}
	*out = uint64(b[0])<<40 |
		uint64(b[1])<<32 |
		uint64(b[2])<<24 |
		uint64(b[3])<<16 |
		uint64(b[4])<<8 |
		uint64(b[5])
	return true
}

func (p *mtcProof) unmarshal(data []byte) error {
	s := cryptobyte.String(data)

	var extensions cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&extensions) {
		return errors.New("malformed extensions")
	}
	p.extensions = append([]byte(nil), extensions...)

	if !readUint48(&s, &p.start) {
		return errors.New("malformed start index")
	}
	if !readUint48(&s, &p.end) {
		return errors.New("malformed end index")
	}

	var incProof cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&incProof) {
		return errors.New("malformed inclusion_proof")
	}
	p.inclusionProof = nil
	for !incProof.Empty() {
		var hash []byte
		if !incProof.ReadBytes(&hash, 32) {
			return errors.New("malformed hash in inclusion_proof")
		}
		p.inclusionProof = append(p.inclusionProof, hashValue(hash))
	}

	var sigs cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&sigs) {
		return errors.New("malformed signatures")
	}
	p.signatures = nil
	for !sigs.Empty() {
		var cosignerID cryptobyte.String
		if !sigs.ReadUint8LengthPrefixed(&cosignerID) || len(cosignerID) == 0 {
			return errors.New("malformed cosigner_id in signatures")
		}
		var sig cryptobyte.String
		if !sigs.ReadUint16LengthPrefixed(&sig) {
			return errors.New("malformed signature in signatures")
		}
		p.signatures = append(p.signatures, SubtreeSignature{
			CosignerID: append([]byte(nil), cosignerID...),
			Signature:  append([]byte(nil), sig...),
		})
	}

	if !s.Empty() {
		return errors.New("trailing bytes after MTCProof")
	}
	return nil
}

func TestMTCProof_Marshal(t *testing.T) {
	var h1, h2 hashValue
	copy(h1[:], bytes.Repeat([]byte{0xaa}, sha256.Size))
	copy(h2[:], bytes.Repeat([]byte{0xbb}, sha256.Size))

	tests := []struct {
		name  string
		proof mtcProof
	}{
		{
			name: "full proof with extensions and signatures",
			proof: mtcProof{
				extensions:     []byte{1, 2, 3},
				start:          3631,
				end:            3981,
				inclusionProof: []hashValue{h1, h2},
				signatures: []SubtreeSignature{
					{CosignerID: []byte("c1"), Signature: []byte("sig1")},
					{CosignerID: []byte("c2"), Signature: []byte("sig2")},
				},
			},
		},
		{
			name: "empty extensions and signatures",
			proof: mtcProof{
				start:          0,
				end:            1,
				inclusionProof: []hashValue{h1},
			},
		},
		{
			name: "zero-length inclusion proof and extensions",
			proof: mtcProof{
				start: 0,
				end:   10,
				signatures: []SubtreeSignature{
					{CosignerID: []byte("c1"), Signature: []byte("sig1")},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.proof.marshal()
			if err != nil {
				t.Fatalf("marshal() error = %v", err)
			}

			var parsed mtcProof
			if err := parsed.unmarshal(data); err != nil {
				t.Fatalf("unmarshal() error = %v", err)
			}

			if !reflect.DeepEqual(tc.proof, parsed) {
				t.Errorf("unmarshal() = %+v, want %+v", parsed, tc.proof)
			}
		})
	}
}

func TestCompareCosignerIDs(t *testing.T) {
	tests := []struct {
		name string
		a, b []byte
		want int
	}{
		{name: "equal single byte", a: []byte("a"), b: []byte("a"), want: 0},
		{name: "less single byte", a: []byte("a"), b: []byte("b"), want: -1},
		{name: "greater single byte", a: []byte("b"), b: []byte("a"), want: 1},
		{name: "shorter length comes first", a: []byte("z"), b: []byte("aa"), want: -1},
		{name: "longer length comes after", a: []byte("aa"), b: []byte("z"), want: 1},
		{name: "lexicographical same length", a: []byte("ab"), b: []byte("ac"), want: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := compareCosignerIDs(tc.a, tc.b)
			if (tc.want < 0 && got >= 0) || (tc.want > 0 && got <= 0) || (tc.want == 0 && got != 0) {
				t.Errorf("compareCosignerIDs(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestSerialize(t *testing.T) {
	node1 := bytes.Repeat([]byte{0xaa}, sha256.Size)
	node2 := bytes.Repeat([]byte{0xbb}, sha256.Size)

	tests := []struct {
		name           string
		extensions     []byte
		start          uint64
		end            uint64
		inclusionProof [][]byte
		signatures     []SubtreeSignature
		wantErr        bool
		wantSigOrder   [][]byte
	}{
		{
			name:           "valid proof with canonical signature sorting",
			start:          0,
			end:            10,
			inclusionProof: [][]byte{node1, node2},
			signatures: []SubtreeSignature{
				{CosignerID: []byte("longest-cosigner-id"), Signature: []byte("sig3")},
				{CosignerID: []byte("id-b"), Signature: []byte("sig2")},
				{CosignerID: []byte("id-a"), Signature: []byte("sig1")},
			},
			wantSigOrder: [][]byte{
				[]byte("id-a"),
				[]byte("id-b"),
				[]byte("longest-cosigner-id"),
			},
		},
		{
			name:    "start exceeds uint48 max",
			start:   1 << 48,
			end:     (1 << 48) + 1,
			wantErr: true,
		},
		{
			name:    "end exceeds uint48 max",
			start:   0,
			end:     1 << 48,
			wantErr: true,
		},
		{
			name:    "invalid range start equals end",
			start:   10,
			end:     10,
			wantErr: true,
		},
		{
			name:    "invalid range start greater than end",
			start:   15,
			end:     10,
			wantErr: true,
		},
		{
			name:       "extensions too large",
			extensions: make([]byte, 1<<16),
			start:      0,
			end:        10,
			wantErr:    true,
		},
		{
			name:           "inclusion proof hash node too short",
			start:          0,
			end:            10,
			inclusionProof: [][]byte{bytes.Repeat([]byte{0xaa}, sha256.Size-1)},
			wantErr:        true,
		},
		{
			name:           "inclusion proof hash node too long",
			start:          0,
			end:            10,
			inclusionProof: [][]byte{bytes.Repeat([]byte{0xaa}, sha256.Size+1)},
			wantErr:        true,
		},
		{
			name:  "duplicate cosigner_id rejected",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: []byte("id1"), Signature: []byte("sig1")},
				{CosignerID: []byte("id1"), Signature: []byte("sig2")},
			},
			wantErr: true,
		},
		{
			name:  "cosigner_id empty (0 bytes) rejected",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: []byte(""), Signature: []byte("sig1")},
			},
			wantErr: true,
		},
		{
			name:  "cosigner_id max length (255 bytes) accepted",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: make([]byte, 255), Signature: []byte("sig1")},
			},
			wantErr: false,
			wantSigOrder: [][]byte{
				make([]byte, 255),
			},
		},
		{
			name:  "cosigner_id too long (256 bytes) rejected",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: make([]byte, 256), Signature: []byte("sig1")},
			},
			wantErr: true,
		},
		{
			name:  "signature too long rejected",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: []byte("id1"), Signature: make([]byte, 1<<16)},
			},
			wantErr: true,
		},
		{
			name:  "signatures vector too large",
			start: 0,
			end:   10,
			signatures: []SubtreeSignature{
				{CosignerID: []byte("id1"), Signature: make([]byte, (1<<16)-10)},
				{CosignerID: []byte("id2"), Signature: make([]byte, 100)},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rawBytes, err := Serialize(tc.extensions, tc.start, tc.end, tc.inclusionProof, tc.signatures)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Serialize() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(rawBytes) == 0 {
				t.Error("Serialize() returned empty byte slice")
			}

			p, err := new(tc.extensions, tc.start, tc.end, tc.inclusionProof, tc.signatures)
			if err != nil {
				t.Fatalf("new() error = %v", err)
			}
			if len(p.signatures) != len(tc.wantSigOrder) {
				t.Fatalf("len(signatures) = %d, want %d", len(p.signatures), len(tc.wantSigOrder))
			}
			for i, wantID := range tc.wantSigOrder {
				if !bytes.Equal(p.signatures[i].CosignerID, wantID) {
					t.Errorf("signatures[%d].CosignerID = %q, want %q", i, p.signatures[i].CosignerID, wantID)
				}
			}
		})
	}
}

func TestParseCosignerID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []byte
		wantErr bool
	}{
		{
			name:  "valid PEN 32473.1",
			input: "oid/1.3.6.1.4.1.32473.1",
			want:  []byte{0x81, 0xfd, 0x59, 0x01},
		},
		{
			name:  "valid PEN 32473.106",
			input: "oid/1.3.6.1.4.1.32473.106",
			want:  []byte{0x81, 0xfd, 0x59, 0x6a},
		},
		{
			name:  "valid PEN 0",
			input: "oid/1.3.6.1.4.1.0",
			want:  []byte{0x00},
		},
		{
			name:  "valid PEN 1.2.3",
			input: "oid/1.3.6.1.4.1.1.2.3",
			want:  []byte{0x01, 0x02, 0x03},
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "missing oid prefix",
			input:   "32473.1",
			wantErr: true,
		},
		{
			name:    "missing oid scheme prefix",
			input:   "1.3.6.1.4.1.32473.1",
			wantErr: true,
		},
		{
			name:    "trailing dot only",
			input:   "oid/1.3.6.1.4.1.",
			wantErr: true,
		},
		{
			name:    "non-numeric characters",
			input:   "oid/1.3.6.1.4.1.invalid",
			wantErr: true,
		},
		{
			name:    "alphanumeric component",
			input:   "oid/1.3.6.1.4.1.1.2.abc",
			wantErr: true,
		},
		{
			name:    "consecutive dots",
			input:   "oid/1.3.6.1.4.1.1..2",
			wantErr: true,
		},
		{
			name:    "wrong base OID arc",
			input:   "oid/1.2.840.113549.1.1.1",
			wantErr: true,
		},
		{
			name:    "OID resulting in binary ID > 255 bytes rejected",
			input:   "oid/1.3.6.1.4.1." + strings.Repeat("123456789.", 70) + "1",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseCosignerID(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseCosignerID(%q) error = %v, wantErr %v", tc.input, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("ParseCosignerID(%q) = %x, want %x", tc.input, got, tc.want)
			}
		})
	}
}
