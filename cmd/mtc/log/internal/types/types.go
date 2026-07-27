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

package types

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	"golang.org/x/crypto/cryptobyte"
)

// Standard log entry type constants from Section 5.2.1.
const (
	MTCLogEntryTypeNull    uint16 = 0
	MTCLogEntryTypeTBSCert uint16 = 1

	// SPEC: draft-ietf-plants-merkle-tree-certs section 5.2.1.
	// "An MTCLogEntry's size SHOULD NOT exceed 65535 (2^16-1) bytes.
	// Doing so may exceed size limits in common log-serving protocols,
	// such as [TLOG-TILES]."
	MaxMTCLogEntrySize = 65535 // 2^16 - 1
)

var (
	errEntryTooLarge       = errors.New("log entry size exceeds tile limit")
	errInvalidEntryType    = errors.New("unknown or unsupported log entry type")
	errMalformedExtensions = errors.New("log entry extensions list is malformed")
	errMissingType         = errors.New("log entry type field is missing")
	errTrailingData        = errors.New("unexpected trailing data in log entry")
)

// MTCLogEntryExtension represents a single key-value metadata extension
// appended to an MTCLogEntry.
type MTCLogEntryExtension struct {
	Type uint16 // 2-byte extension identifier
	Data []byte // Opaque data payload
}

// MTCLogEntry represents leaf node as defined in
// draft-ietf-plants-merkle-tree-certs section 5.2.1.
type MTCLogEntry struct {
	Extensions []MTCLogEntryExtension
	Type       uint16 // MTCLogEntryTypeNull, MTCLogEntryTypeTBSCert
	EntryData  []byte // Raw DER bytes of TBSCertificateLogEntry if Type is TBSCert
}

// Marshal encodes the MTCLogEntry into TLS Presentation bytes.
//
// Returns an error if the extensions are not specs compliant, or if the
// resulting bytes do not fit in a t-log leaf.
func (e *MTCLogEntry) Marshal() ([]byte, error) {
	if e.Type == MTCLogEntryTypeNull && len(e.EntryData) > 0 {
		return nil, fmt.Errorf("null entry must have empty EntryData: %w", errTrailingData)
	}
	// struct {} Empty;
	//
	// enum { (2^16-1) } MTCLogEntryExtensionType;
	//
	// struct {
	//     MTCLogEntryExtensionType extension_type;
	//     opaque extension_data<0..2^16-1>;
	// } MTCLogEntryExtension;
	//
	// enum {
	//     null_entry(0), tbs_cert_entry(1), (2^16-1)
	// } MTCLogEntryType;
	//
	// struct {
	//     MTCLogEntryExtension extensions<0..2^16-1>;
	//     MTCLogEntryType type;
	//     select (type) {
	//        case null_entry: Empty;
	//        case tbs_cert_entry: opaque tbs_cert_entry_data[N];
	//        /* May be extended with future types. */
	//     }
	// } MTCLogEntry;
	var b cryptobyte.Builder

	// SPEC: draft-ietf-plants-merkle-tree-certs section 5.2.1.
	// "The extensions list MUST appear in ascending order by extension_type and
	// MUST NOT contain two extensions with the same extension_type."
	exts := slices.Clone(e.Extensions)
	slices.SortStableFunc(exts, func(a, b MTCLogEntryExtension) int {
		return cmp.Compare(a.Type, b.Type)
	})

	// Creates a 16-bit (2-byte) length-prefixed block for the list of extensions.
	b.AddUint16LengthPrefixed(func(child *cryptobyte.Builder) {
		for i, ext := range exts {
			if i > 0 && ext.Type == exts[i-1].Type {
				if !bytes.Equal(ext.Data, exts[i-1].Data) {
					child.SetError(fmt.Errorf("conflicting duplicate extension type %d with differing data: %w", ext.Type, errMalformedExtensions))
					return
				}
				continue
			}
			child.AddUint16(ext.Type)
			// Each extension data block has its own 16-bit length prefix.
			child.AddUint16LengthPrefixed(func(dataBlock *cryptobyte.Builder) {
				dataBlock.AddBytes(ext.Data)
			})
		}
	})

	b.AddUint16(e.Type)

	// EntryData fills up the rest of the structure, no size prefix needed.
	b.AddBytes(e.EntryData)

	res, err := b.Bytes()
	if err != nil {
		return nil, err
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 5.2.1.
	// "An MTCLogEntry's size SHOULD NOT exceed 65535 (2^16-1) bytes.
	// Doing so may exceed size limits in common log-serving protocols,
	// such as [TLOG-TILES]."
	if len(res) > MaxMTCLogEntrySize {
		return nil, fmt.Errorf("log entry size %d exceeds tile limit %d: %w", len(res), MaxMTCLogEntrySize, errEntryTooLarge)
	}
	return res, nil
}
