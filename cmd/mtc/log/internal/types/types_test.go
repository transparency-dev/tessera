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
	"testing"

	"golang.org/x/crypto/cryptobyte"
)


// unmarshal decodes a raw TLS presentation byte stream into an MTCLogEntry structure.
// This is kept in the test suite for verifying round-trip serialization of MTCLogEntry.
func (e *MTCLogEntry) unmarshal(data []byte) error {
	s := cryptobyte.String(data)

	var extListStr cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&extListStr) {
		return errMalformedExtensions
	}

	e.Extensions = nil
	for !extListStr.Empty() {
		var ext MTCLogEntryExtension
		if !extListStr.ReadUint16(&ext.Type) {
			return fmt.Errorf("failed to read extension type: %w", errMalformedExtensions)
		}
		var extDataStr cryptobyte.String
		if !extListStr.ReadUint16LengthPrefixed(&extDataStr) {
			return fmt.Errorf("failed to read extension length: %w", errMalformedExtensions)
		}
		ext.Data = append([]byte(nil), extDataStr...)
		if n := len(e.Extensions); n > 0 {
			if ext.Type < e.Extensions[n-1].Type {
				return fmt.Errorf("mtc: entry extensions out of order (type %d after %d): %w", ext.Type, e.Extensions[n-1].Type, errMalformedExtensions)
			}
			if ext.Type == e.Extensions[n-1].Type {
				return fmt.Errorf("mtc: duplicate entry extension type %d: %w", ext.Type, errMalformedExtensions)
			}
		}
		e.Extensions = append(e.Extensions, ext)
	}

	if !s.ReadUint16(&e.Type) {
		return errMissingType
	}

	if e.Type != MTCLogEntryTypeNull && e.Type != MTCLogEntryTypeTBSCert {
		return fmt.Errorf("%w: type %d", errInvalidEntryType, e.Type)
	}

	if e.Type == MTCLogEntryTypeNull && !s.Empty() {
		return fmt.Errorf("null entry must have empty data: %w", errTrailingData)
	}

	e.EntryData = append([]byte(nil), s...)
	return nil
}

func TestMTCLogEntry_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry MTCLogEntry
	}{
		{
			name: "null entry no extensions",
			entry: MTCLogEntry{
				Type: MTCLogEntryTypeNull,
			},
		},
		{
			name: "tbs cert entry with sorted extensions",
			entry: MTCLogEntry{
				Type:      MTCLogEntryTypeTBSCert,
				EntryData: []byte("fake-der-octets"),
				Extensions: []MTCLogEntryExtension{
					{Type: 1, Data: []byte("ext-1-data")},
					{Type: 5, Data: []byte("ext-5-data")},
					{Type: 10, Data: []byte("")},
				},
			},
		},
		{
			name: "tbs cert entry with unsorted extensions",
			entry: MTCLogEntry{
				Type:      MTCLogEntryTypeTBSCert,
				EntryData: []byte("fake-der-octets"),
				Extensions: []MTCLogEntryExtension{
					{Type: 10, Data: []byte("ext-10-data")},
					{Type: 1, Data: []byte("ext-1-data")},
					{Type: 5, Data: []byte("ext-5-data")},
				},
			},
		},
		{
			name: "tbs cert entry with identical duplicate extensions",
			entry: MTCLogEntry{
				Type:      MTCLogEntryTypeTBSCert,
				EntryData: []byte("fake-der-octets"),
				Extensions: []MTCLogEntryExtension{
					{Type: 5, Data: []byte("ext-5-data")},
					{Type: 1, Data: []byte("ext-1-data")},
					{Type: 5, Data: []byte("ext-5-data")},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.entry.Marshal()
			if err != nil {
				t.Fatalf("Marshal() unexpected error: %v", err)
			}

			var got MTCLogEntry
			if err := got.unmarshal(data); err != nil {
				t.Fatalf("unmarshal() unexpected error: %v", err)
			}

			if got.Type != tc.entry.Type {
				t.Errorf("Type = %d, want %d", got.Type, tc.entry.Type)
			}
			if !bytes.Equal(got.EntryData, tc.entry.EntryData) {
				t.Errorf("EntryData = %x, want %x", got.EntryData, tc.entry.EntryData)
			}

			wantExts := slices.Clone(tc.entry.Extensions)
			slices.SortStableFunc(wantExts, func(a, b MTCLogEntryExtension) int {
				return cmp.Compare(a.Type, b.Type)
			})
			wantExts = slices.CompactFunc(wantExts, func(a, b MTCLogEntryExtension) bool {
				return a.Type == b.Type && bytes.Equal(a.Data, b.Data)
			})
			if len(got.Extensions) != len(wantExts) {
				t.Fatalf("len(Extensions) = %d, want %d", len(got.Extensions), len(wantExts))
			}
			for i := range got.Extensions {
				if got.Extensions[i].Type != wantExts[i].Type || !bytes.Equal(got.Extensions[i].Data, wantExts[i].Data) {
					t.Errorf("Extension[%d] = %+v, want %+v", i, got.Extensions[i], wantExts[i])
				}
			}
		})
	}
}

func TestMTCLogEntry_MarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		entry   MTCLogEntry
		wantErr error
	}{
		{
			name: "trailing data on null entry",
			entry: MTCLogEntry{
				Type:      MTCLogEntryTypeNull,
				EntryData: []byte("unexpected-data"),
			},
			wantErr: errTrailingData,
		},
		{
			name: "conflicting duplicate extensions with different data",
			entry: MTCLogEntry{
				Type: MTCLogEntryTypeTBSCert,
				Extensions: []MTCLogEntryExtension{
					{Type: 2, Data: []byte("data-a")},
					{Type: 2, Data: []byte("data-b")},
				},
			},
			wantErr: errMalformedExtensions,
		},
		{
			name: "entry size exceeds tile limit",
			entry: MTCLogEntry{
				Type:      MTCLogEntryTypeTBSCert,
				EntryData: make([]byte, MaxMTCLogEntrySize),
			},
			wantErr: errEntryTooLarge,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.entry.Marshal()
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestMTCLogEntry_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr error
	}{
		{
			name: "truncated extension list length prefix",
			mutate: func(b []byte) []byte {
				return []byte{0x00, 0x05, 0x01}
			},
			wantErr: errMalformedExtensions,
		},
		{
			name: "trailing data on null entry",
			mutate: func(b []byte) []byte {
				e := MTCLogEntry{Type: MTCLogEntryTypeNull}
				data, _ := e.Marshal()
				return append(data, 0x00)
			},
			wantErr: errTrailingData,
		},
		{
			name: "unknown entry type",
			mutate: func(b []byte) []byte {
				var builder cryptobyte.Builder
				builder.AddUint16(0)  // 0 length extensions
				builder.AddUint16(99) // invalid type 99
				return builder.BytesOrPanic()
			},
			wantErr: errInvalidEntryType,
		},
		{
			name: "unsorted extensions over the wire",
			mutate: func(b []byte) []byte {
				var builder cryptobyte.Builder
				var extList cryptobyte.Builder
				extList.AddUint16(5)
				extList.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("a")) })
				extList.AddUint16(1)
				extList.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("b")) })
				builder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes(extList.BytesOrPanic()) })
				builder.AddUint16(MTCLogEntryTypeTBSCert)
				return builder.BytesOrPanic()
			},
			wantErr: errMalformedExtensions,
		},
		{
			name: "duplicate extensions over the wire",
			mutate: func(b []byte) []byte {
				var builder cryptobyte.Builder
				var extList cryptobyte.Builder
				extList.AddUint16(2)
				extList.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("a")) })
				extList.AddUint16(2)
				extList.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("b")) })
				builder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes(extList.BytesOrPanic()) })
				builder.AddUint16(MTCLogEntryTypeTBSCert)
				return builder.BytesOrPanic()
			},
			wantErr: errMalformedExtensions,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var entry MTCLogEntry
			err := entry.unmarshal(tc.mutate(nil))
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("unmarshal() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
