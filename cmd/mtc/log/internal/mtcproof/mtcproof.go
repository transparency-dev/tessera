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
	"cmp"
	"crypto/sha256"
	"crypto/x509"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/crypto/cryptobyte"
)

const (
	oidPrefix           = "oid/1.3.6.1.4.1."
	oidPrefixDERLen     = 5
	maxTrustAnchorIDLen = (1 << 8) - 1
	maxUint48           = (1 << 48) - 1
)

// hashValue represents a 32-byte hash as per
// draft-ietf-plants-merkle-tree-certs section 6.2.
type hashValue [sha256.Size]byte

// SubtreeSignature represents a cosigner's signature on a subtree root
// as per draft-ietf-plants-merkle-tree-certs section 6.2:
type SubtreeSignature struct {
	// CosignerID is the binary representation of the trust anchor ID (1..255 bytes).
	CosignerID []byte
	// Signature is the raw signature bytes over the subtree.
	Signature []byte
}

// mtcProof represents an MTC inclusion proof as per
// draft-ietf-plants-merkle-tree-certs section 6.2.
//
// New instances MUST be created with new().
type mtcProof struct {
	extensions     []byte
	start          uint64
	end            uint64
	inclusionProof []hashValue
	signatures     []SubtreeSignature
}

// ParseCosignerID converts an ASCII cosigner name into its binary representation
// as per draft-ietf-tls-trust-anchor-ids Section 3.
func ParseCosignerID(name string) ([]byte, error) {
	if !strings.HasPrefix(name, oidPrefix) {
		return nil, fmt.Errorf("cosigner name %q must start with %q", name, oidPrefix)
	}
	oid, err := x509.ParseOID(strings.TrimPrefix(name, "oid/"))
	if err != nil {
		return nil, fmt.Errorf("invalid cosigner ID OID %q: %w", name, err)
	}
	der, err := oid.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal cosigner ID OID %q: %w", name, err)
	}
	// SPEC: draft-ietf-tls-trust-anchor-ids section 3.
	// "For use in binary protocols such as TLS, a trust anchor ID's binary
	// representation consists of the contents octets of the relative object
	// identifier's DER encoding, as described in Section 8.20 of [X690]. Note
	// this omits the tag and length portion of the encoding."
	res := der[oidPrefixDERLen:]
	if l := len(res); l == 0 || l > maxTrustAnchorIDLen {
		return nil, fmt.Errorf("cosigner ID binary length must be 1..%d bytes, got %d", maxTrustAnchorIDLen, l)
	}
	return res, nil
}

// Serialize validates MTC proof parameters, orders signatures canonically, and returns
// the TLS-encoded MTCProof binary bytes.
func Serialize(extensions []byte, start, end uint64, inclusionProof [][]byte, signatures []SubtreeSignature) ([]byte, error) {
	p, err := new(extensions, start, end, inclusionProof, signatures)
	if err != nil {
		return nil, err
	}
	return p.marshal()
}

// new constructs a validated mtcProof with signatures sorted canonically.
func new(extensions []byte, start, end uint64, inclusionProof [][]byte, signatures []SubtreeSignature) (*mtcProof, error) {
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
	// "uint48 start; uint48 end;"
	if start >= end {
		return nil, fmt.Errorf("start (%d) must be strictly less than end (%d)", start, end)
	}
	if end > maxUint48 {
		return nil, fmt.Errorf("end index %d exceeds uint48 maximum (%d)", end, maxUint48)
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 5.2.1.
	// "opaque extension_data<0..2^16-1>; "
	if limit, l := 1<<16, len(extensions); l >= limit {
		return nil, fmt.Errorf("extensions too large (%d bytes, max %d)", l, limit-1)
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
	// "HashValue inclusion_proof<0..2^16-1>;"
	if limit, l := 1<<16, len(inclusionProof)*sha256.Size; l >= limit {
		return nil, fmt.Errorf("inclusion proof too large (%d bytes, max %d)", l, limit-1)
	}

	hashes := make([]hashValue, len(inclusionProof))
	for i, n := range inclusionProof {
		if l := len(n); l != sha256.Size {
			return nil, fmt.Errorf("inclusion proof node %d invalid size: got %d bytes, want %d", i, l, sha256.Size)
		}
		hashes[i] = hashValue(n)
	}

	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
	// "Each element of the signatures field MUST have a unique cosigner_id.
	// Elements MUST be ordered by cosigner_id"
	sortedSigs := slices.Clone(signatures)
	slices.SortFunc(sortedSigs, func(a, b SubtreeSignature) int {
		return compareCosignerIDs(a.CosignerID, b.CosignerID)
	})

	var totalSigsLen int
	for i, sig := range sortedSigs {
		// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
		// /* From Section 4.1 of draft-ietf-tls-trust-anchor-ids */
		// "opaque TrustAnchorID<1..2^8-1>;"
		if l := len(sig.CosignerID); l == 0 || l > maxTrustAnchorIDLen {
			return nil, fmt.Errorf("cosigner_id length must be 1..%d bytes, got %d", maxTrustAnchorIDLen, l)
		}
		// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
		// "opaque signature<0..2^16-1>;"
		if limit, l := 1<<16, len(sig.Signature); l >= limit {
			return nil, fmt.Errorf("signature too large (%d bytes, max %d)", l, limit-1)
		}
		// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
		// "SubtreeSignature signatures<0..2^16-1>;"
		// In TLS presentation syntax, each SubtreeSignature consists of:
		// 1 byte (length of TrustAnchorID) + len(CosignerID) + 2 bytes (length of signature) + len(Signature).
		totalSigsLen += 1 + len(sig.CosignerID) + 2 + len(sig.Signature)
		if limit, l := 1<<16, totalSigsLen; l >= limit {
			return nil, fmt.Errorf("signatures vector too large (%d bytes, max %d)", totalSigsLen, limit-1)
		}
		// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
		// "An MTCProof parser MUST reject the input if there are duplicate cosigner_id values,
		// or if they are not ordered correctly. This can be done by checking each cosigner_id
		// value comes strictly after the previous one in the above order."
		if i > 0 && compareCosignerIDs(sortedSigs[i-1].CosignerID, sig.CosignerID) == 0 {
			return nil, fmt.Errorf("duplicate cosigner_id: %x", sig.CosignerID)
		}
	}

	return &mtcProof{
		extensions:     slices.Clone(extensions),
		start:          start,
		end:            end,
		inclusionProof: hashes,
		signatures:     sortedSigs,
	}, nil
}

// marshal TLS encodes mtcProof.
//
// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
// /* From Section 4.1 of draft-ietf-tls-trust-anchor-ids */
// opaque TrustAnchorID<1..2^8-1>;
//
// opaque HashValue[HASH_SIZE];
//
// struct {
//     TrustAnchorID cosigner_id;
//     opaque signature<0..2^16-1>;
// } SubtreeSignature;
//
// struct {
//     MTCLogEntryExtension extensions<0..2^16-1>;
//     uint48 start;
//     uint48 end;
//     HashValue inclusion_proof<0..2^16-1>;
//     SubtreeSignature signatures<0..2^16-1>;
// } MTCProof;
func (p *mtcProof) marshal() ([]byte, error) {
	var b cryptobyte.Builder

	// MTCLogEntryExtension extensions<0..2^16-1>
	b.AddUint16LengthPrefixed(func(child *cryptobyte.Builder) {
		child.AddBytes(p.extensions)
	})

	// uint48 start
	addUint48(&b, p.start)

	// uint48 end
	addUint48(&b, p.end)

	// HashValue inclusion_proof<0..2^16-1>
	b.AddUint16LengthPrefixed(func(child *cryptobyte.Builder) {
		for _, h := range p.inclusionProof {
			child.AddBytes(h[:])
		}
	})

	// SubtreeSignature signatures<0..2^16-1>
	b.AddUint16LengthPrefixed(func(child *cryptobyte.Builder) {
		for _, sig := range p.signatures {
			// TrustAnchorID cosigner_id<1..2^8-1>
			child.AddUint8LengthPrefixed(func(c *cryptobyte.Builder) {
				c.AddBytes(sig.CosignerID)
			})
			// opaque signature<0..2^16-1>
			child.AddUint16LengthPrefixed(func(s *cryptobyte.Builder) {
				s.AddBytes(sig.Signature)
			})
		}
	})

	return b.Bytes()
}

// addUint48 appends a big-endian, 48-bit value to the byte string.
func addUint48(b *cryptobyte.Builder, v uint64) {
	buf := [6]byte{byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	b.AddBytes(buf[:])
}

// compareCosignerIDs compares two cosigner_id byte slices.
// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
// " - Shorter byte strings are ordered before longer byte strings.
//   - Byte strings of the same length are ordered lexicographically."
func compareCosignerIDs(a, b []byte) int {
	if len(a) != len(b) {
		return cmp.Compare(len(a), len(b))
	}
	return bytes.Compare(a, b)
}
