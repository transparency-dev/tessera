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

package entry

import (
	"bytes"
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
		return errors.New("malformed extension list length")
	}

	var extensions []MTCLogEntryExtension
	for !extListStr.Empty() {
		var ext MTCLogEntryExtension
		if !extListStr.ReadUint16((*uint16)(&ext.Type)) {
			return fmt.Errorf("failed to read extension type")
		}
		var extDataStr cryptobyte.String
		if !extListStr.ReadUint16LengthPrefixed(&extDataStr) {
			return fmt.Errorf("failed to read extension length")
		}
		ext.Data = append([]byte(nil), extDataStr...)
		if n := len(extensions); n > 0 {
			if ext.Type < extensions[n-1].Type {
				return fmt.Errorf("mtc: entry extensions out of order (type %d after %d)", ext.Type, extensions[n-1].Type)
			}
			if ext.Type == extensions[n-1].Type {
				return fmt.Errorf("mtc: duplicate entry extension type %d", ext.Type)
			}
		}
		extensions = append(extensions, ext)
	}

	var entryType EntryType
	if !s.ReadUint16((*uint16)(&entryType)) {
		return errors.New("missing entry type")
	}

	if entryType != MTCLogEntryTypeNull && entryType != MTCLogEntryTypeTBSCert {
		return fmt.Errorf("unknown or unsupported log entry type %d", entryType)
	}

	if entryType == MTCLogEntryTypeNull && !s.Empty() {
		return fmt.Errorf("null entry must have empty data")
	}

	*e = MTCLogEntry{
		extensions: extensions,
		entryType:  entryType,
		entryData:  slices.Clone(s),
	}
	return nil
}

func TestMTCLogEntry_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry *MTCLogEntry
	}{
		{
			name:  "null entry no extensions",
			entry: New(nil),
		},
		{
			name: "tbs cert entry with sorted extensions",
			entry: New(
				[]byte("fake-der-octets"),
				MTCLogEntryExtension{Type: 1, Data: []byte("ext-1-data")},
				MTCLogEntryExtension{Type: 5, Data: []byte("ext-5-data")},
				MTCLogEntryExtension{Type: 10, Data: []byte("")},
			),
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

			if got.Type() != tc.entry.Type() {
				t.Errorf("Type() = %d, want %d", got.Type(), tc.entry.Type())
			}
			if !bytes.Equal(got.EntryData(), tc.entry.EntryData()) {
				t.Errorf("EntryData() = %x, want %x", got.EntryData(), tc.entry.EntryData())
			}

			gotExts := got.Extensions()
			wantExts := tc.entry.Extensions()
			if len(gotExts) != len(wantExts) {
				t.Fatalf("len(Extensions) = %d, want %d", len(gotExts), len(wantExts))
			}
			for i := range gotExts {
				if gotExts[i].Type != wantExts[i].Type || !bytes.Equal(gotExts[i].Data, wantExts[i].Data) {
					t.Errorf("Extension[%d] = %+v, want %+v", i, gotExts[i], wantExts[i])
				}
			}
		})
	}
}

func TestMTCLogEntry_MarshalErrors(t *testing.T) {
	nullWithData := New([]byte("unexpected-data"))
	nullWithData.entryType = MTCLogEntryTypeNull

	tests := []struct {
		name    string
		entry   *MTCLogEntry
		wantErr bool
	}{
		{
			name:    "trailing data on null entry",
			entry:   nullWithData,
			wantErr: true,
		},
		{
			name: "duplicate extensions with same data",
			entry: New(
				[]byte("fake-der"),
				MTCLogEntryExtension{Type: 2, Data: []byte("data-a")},
				MTCLogEntryExtension{Type: 2, Data: []byte("data-a")},
			),
			wantErr: true,
		},
		{
			name: "duplicate extensions with different data",
			entry: New(
				[]byte("fake-der"),
				MTCLogEntryExtension{Type: 2, Data: []byte("data-a")},
				MTCLogEntryExtension{Type: 2, Data: []byte("data-b")},
			),
			wantErr: true,
		},
		{
			name: "unsorted extensions",
			entry: New(
				[]byte("fake-der"),
				MTCLogEntryExtension{Type: 5, Data: []byte("data-5")},
				MTCLogEntryExtension{Type: 1, Data: []byte("data-1")},
			),
			wantErr: true,
		},
		{
			name:    "entry size exceeds tile limit",
			entry:   New(make([]byte, MaxMTCLogEntrySize)),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.entry.Marshal()
			if (err != nil) != tc.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestMTCLogEntry_UnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr bool
	}{
		{
			name: "truncated extension list length prefix",
			mutate: func(b []byte) []byte {
				return []byte{0x00, 0x05, 0x01}
			},
			wantErr: true,
		},
		{
			name: "trailing data on null entry",
			mutate: func(b []byte) []byte {
				e := New(nil)
				data, _ := e.Marshal()
				return append(data, 0x00)
			},
			wantErr: true,
		},
		{
			name: "unknown entry type",
			mutate: func(b []byte) []byte {
				var builder cryptobyte.Builder
				builder.AddUint16(0)  // 0 length extensions
				builder.AddUint16(99) // invalid type 99
				return builder.BytesOrPanic()
			},
			wantErr: true,
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
				builder.AddUint16(uint16(MTCLogEntryTypeTBSCert))
				return builder.BytesOrPanic()
			},
			wantErr: true,
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
				builder.AddUint16(uint16(MTCLogEntryTypeTBSCert))
				return builder.BytesOrPanic()
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var entry MTCLogEntry
			err := entry.unmarshal(tc.mutate(nil))
			if (err != nil) != tc.wantErr {
				t.Errorf("unmarshal() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestExtractExtensions(t *testing.T) {
	entryWithExts := New(
		[]byte("sample-data"),
		MTCLogEntryExtension{Type: 1, Data: []byte("ext-1")},
		MTCLogEntryExtension{Type: 3, Data: []byte("ext-3")},
	)
	entryWithExtsBytes, err := entryWithExts.Marshal()
	if err != nil {
		t.Fatalf("Marshal() unexpected error: %v", err)
	}

	entryNoExts := New([]byte("sample-data"))
	entryNoExtsBytes, err := entryNoExts.Marshal()
	if err != nil {
		t.Fatalf("Marshal() unexpected error: %v", err)
	}

	nullEntry := New(nil)
	nullEntryBytes, err := nullEntry.Marshal()
	if err != nil {
		t.Fatalf("Marshal() unexpected error: %v", err)
	}

	var wantExtsBuilder cryptobyte.Builder
	wantExtsBuilder.AddUint16(1)
	wantExtsBuilder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("ext-1")) })
	wantExtsBuilder.AddUint16(3)
	wantExtsBuilder.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) { b.AddBytes([]byte("ext-3")) })
	wantExts := wantExtsBuilder.BytesOrPanic()

	tests := []struct {
		name         string
		entryData    []byte
		wantExtBytes []byte
		wantErr      bool
	}{
		{
			name:         "entry with multiple extensions",
			entryData:    entryWithExtsBytes,
			wantExtBytes: wantExts,
		},
		{
			name:         "entry with no extensions",
			entryData:    entryNoExtsBytes,
			wantExtBytes: []byte{},
		},
		{
			name:         "null entry with no extensions",
			entryData:    nullEntryBytes,
			wantExtBytes: []byte{},
		},
		{
			name:      "empty input",
			entryData: []byte{},
			wantErr:   true,
		},
		{
			name:      "truncated 1-byte length prefix",
			entryData: []byte{0x00},
			wantErr:   true,
		},
		{
			name:      "length prefix exceeds available data",
			entryData: []byte{0x00, 0x10, 0x01},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ExtractExtensions(tc.entryData)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractExtensions() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !bytes.Equal(got, tc.wantExtBytes) {
				t.Errorf("ExtractExtensions() = %x, want %x", got, tc.wantExtBytes)
			}
		})
	}
}
