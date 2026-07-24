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

package log

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
)


// unmarshal decodes the contents octets of a TBSCertificateLogEntry
// (i.e. WITHOUT the outer SEQUENCE tag and length prefix) into the struct.
// This is kept in the test suite for verifying round-trip serialization of TBSCertificateLogEntry.
func (e *TBSCertificateLogEntry) unmarshal(contents []byte) error {
	seq := cryptobyte.String(contents)

	e.Version = 0
	versionTag := asn1.Tag(0).ContextSpecific().Constructed()
	if seq.PeekASN1Tag(versionTag) {
		var verWrapped cryptobyte.String
		if !seq.ReadASN1(&verWrapped, versionTag) {
			return fmt.Errorf("failed to read version wrapper")
		}
		if !verWrapped.ReadASN1Integer(&e.Version) || !verWrapped.Empty() {
			return fmt.Errorf("version field is malformed")
		}
	}

	readElement := func(target *[]byte, tag asn1.Tag, fieldName string) error {
		var rawElem cryptobyte.String
		if !seq.ReadASN1Element(&rawElem, tag) {
			return fmt.Errorf("failed to read %s", fieldName)
		}
		*target = append([]byte(nil), rawElem...)
		return nil
	}

	if err := readElement(&e.Issuer, asn1.SEQUENCE, "issuer"); err != nil {
		return err
	}
	if err := readElement(&e.Validity, asn1.SEQUENCE, "validity"); err != nil {
		return err
	}
	if err := readElement(&e.Subject, asn1.SEQUENCE, "subject"); err != nil {
		return err
	}
	if err := readElement(&e.SubjectPublicKeyAlgorithm, asn1.SEQUENCE, "subjectPublicKeyAlgorithm"); err != nil {
		return err
	}
	var spkiHash cryptobyte.String
	if !seq.ReadASN1(&spkiHash, asn1.OCTET_STRING) {
		return fmt.Errorf("failed to read subjectPublicKeyInfoHash")
	}
	e.SubjectPublicKeyInfoHash = append([]byte(nil), spkiHash...)

	tag1 := asn1.Tag(1).ContextSpecific()
	if seq.PeekASN1Tag(tag1) {
		if err := readElement(&e.IssuerUniqueID, tag1, "issuerUniqueID"); err != nil {
			return err
		}
	}

	tag2 := asn1.Tag(2).ContextSpecific()
	if seq.PeekASN1Tag(tag2) {
		if err := readElement(&e.SubjectUniqueID, tag2, "subjectUniqueID"); err != nil {
			return err
		}
	}

	tag3 := asn1.Tag(3).ContextSpecific().Constructed()
	if seq.PeekASN1Tag(tag3) {
		if err := readElement(&e.Extensions, tag3, "extensions"); err != nil {
			return err
		}
	}

	if !seq.Empty() {
		return fmt.Errorf("sequence has trailing bytes")
	}
	return nil
}

func dummySeq(data string) []byte {
	var b cryptobyte.Builder
	b.AddASN1(asn1.SEQUENCE, func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(data))
	})
	return b.BytesOrPanic()
}

func dummyTag(tag asn1.Tag, data string) []byte {
	var b cryptobyte.Builder
	b.AddASN1(tag, func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(data))
	})
	return b.BytesOrPanic()
}

func TestTBSCertificateLogEntry_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		entry TBSCertificateLogEntry
	}{
		{
			name: "default version 0 minimal fields",
			entry: TBSCertificateLogEntry{
				Version:                   0,
				Issuer:                    dummySeq("issuer-der-seq"),
				Validity:                  dummySeq("validity-der-seq"),
				Subject:                   dummySeq("subject-der-seq"),
				SubjectPublicKeyAlgorithm: dummySeq("spki-algo-der-seq"),
				SubjectPublicKeyInfoHash:  make([]byte, 32),
			},
		},
		{
			name: "version 2 with optional unique IDs and extensions",
			entry: TBSCertificateLogEntry{
				Version:                   2,
				Issuer:                    dummySeq("issuer-der-seq"),
				Validity:                  dummySeq("validity-der-seq"),
				Subject:                   dummySeq("subject-der-seq"),
				SubjectPublicKeyAlgorithm: dummySeq("spki-algo-der-seq"),
				SubjectPublicKeyInfoHash:  bytes.Repeat([]byte{0x42}, 32),
				IssuerUniqueID:            dummyTag(asn1.Tag(1).ContextSpecific(), "uid-1"),
				SubjectUniqueID:           dummyTag(asn1.Tag(2).ContextSpecific(), "uid-2"),
				Extensions:                dummyTag(asn1.Tag(3).ContextSpecific().Constructed(), "exts"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.entry.Marshal()
			if err != nil {
				t.Fatalf("Marshal() unexpected error: %v", err)
			}

			var got TBSCertificateLogEntry
			if err := got.unmarshal(data); err != nil {
				t.Fatalf("Unmarshal() unexpected error: %v", err)
			}

			if got.Version != tc.entry.Version {
				t.Errorf("Version = %d, want %d", got.Version, tc.entry.Version)
			}
			if !bytes.Equal(got.Issuer, tc.entry.Issuer) {
				t.Errorf("Issuer = %x, want %x", got.Issuer, tc.entry.Issuer)
			}
			if !bytes.Equal(got.Validity, tc.entry.Validity) {
				t.Errorf("Validity = %x, want %x", got.Validity, tc.entry.Validity)
			}
			if !bytes.Equal(got.Subject, tc.entry.Subject) {
				t.Errorf("Subject = %x, want %x", got.Subject, tc.entry.Subject)
			}
			if !bytes.Equal(got.SubjectPublicKeyAlgorithm, tc.entry.SubjectPublicKeyAlgorithm) {
				t.Errorf("SubjectPublicKeyAlgorithm = %x, want %x", got.SubjectPublicKeyAlgorithm, tc.entry.SubjectPublicKeyAlgorithm)
			}
			if !bytes.Equal(got.SubjectPublicKeyInfoHash, tc.entry.SubjectPublicKeyInfoHash) {
				t.Errorf("SubjectPublicKeyInfoHash = %x, want %x", got.SubjectPublicKeyInfoHash, tc.entry.SubjectPublicKeyInfoHash)
			}
			if !bytes.Equal(got.IssuerUniqueID, tc.entry.IssuerUniqueID) {
				t.Errorf("IssuerUniqueID = %x, want %x", got.IssuerUniqueID, tc.entry.IssuerUniqueID)
			}
			if !bytes.Equal(got.SubjectUniqueID, tc.entry.SubjectUniqueID) {
				t.Errorf("SubjectUniqueID = %x, want %x", got.SubjectUniqueID, tc.entry.SubjectUniqueID)
			}
			if !bytes.Equal(got.Extensions, tc.entry.Extensions) {
				t.Errorf("Extensions = %x, want %x", got.Extensions, tc.entry.Extensions)
			}
		})
	}
}

func TestTBSCertificateLogEntry_Validate(t *testing.T) {
	valid := func() TBSCertificateLogEntry {
		return TBSCertificateLogEntry{
			Issuer:                    dummySeq("issuer"),
			Validity:                  dummySeq("validity"),
			Subject:                   dummySeq("subject"),
			SubjectPublicKeyAlgorithm: dummySeq("algo"),
			SubjectPublicKeyInfoHash:  make([]byte, 32),
		}
	}

	tests := []struct {
		name    string
		mutate  func(*TBSCertificateLogEntry)
		wantErr bool
	}{
		{
			name:    "valid",
			mutate:  func(e *TBSCertificateLogEntry) {},
			wantErr: false,
		},
		{
			name:    "negative version",
			mutate:  func(e *TBSCertificateLogEntry) { e.Version = -1 },
			wantErr: true,
		},
		{
			name:    "version out of bounds",
			mutate:  func(e *TBSCertificateLogEntry) { e.Version = 3 },
			wantErr: true,
		},
		{
			name:    "malformed issuer asn1",
			mutate:  func(e *TBSCertificateLogEntry) { e.Issuer = []byte("not-asn1-sequence") },
			wantErr: true,
		},
		{
			name:    "trailing data on issuer",
			mutate:  func(e *TBSCertificateLogEntry) { e.Issuer = append(dummySeq("issuer"), 0x00) },
			wantErr: true,
		},
		{
			name:    "wrong tag on issuerUniqueID",
			mutate:  func(e *TBSCertificateLogEntry) { e.IssuerUniqueID = dummyTag(asn1.Tag(5).ContextSpecific(), "uid") },
			wantErr: true,
		},
		{
			name:    "wrong tag on extensions (primitive instead of constructed)",
			mutate:  func(e *TBSCertificateLogEntry) { e.Extensions = dummyTag(asn1.Tag(3).ContextSpecific(), "exts") },
			wantErr: true,
		},
		{
			name:    "missing issuer",
			mutate:  func(e *TBSCertificateLogEntry) { e.Issuer = nil },
			wantErr: true,
		},
		{
			name:    "missing validity",
			mutate:  func(e *TBSCertificateLogEntry) { e.Validity = nil },
			wantErr: true,
		},
		{
			name:    "missing subject",
			mutate:  func(e *TBSCertificateLogEntry) { e.Subject = nil },
			wantErr: true,
		},
		{
			name:    "missing spki algo",
			mutate:  func(e *TBSCertificateLogEntry) { e.SubjectPublicKeyAlgorithm = nil },
			wantErr: true,
		},
		{
			name:    "invalid hash size (short)",
			mutate:  func(e *TBSCertificateLogEntry) { e.SubjectPublicKeyInfoHash = make([]byte, 10) },
			wantErr: true,
		},
		{
			name:    "invalid hash size (long)",
			mutate:  func(e *TBSCertificateLogEntry) { e.SubjectPublicKeyInfoHash = make([]byte, 33) },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := valid()
			tc.mutate(&e)
			err := e.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

type dummyLogReader struct {
	tessera.LogReader
}

func TestMTCOptionsValid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    *Options
		wantErr bool
	}{
		{
			name:    "Valid",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}),
			wantErr: false,
		}, {
			name:    "Valid: custom poll period",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithAwaiterPollInterval(10 * time.Millisecond),
			wantErr: false,
		}, {
			name:    "Error: Negative poll period",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithAwaiterPollInterval(-10 * time.Millisecond),
			wantErr: true,
		}, {
			name:    "Error: No TesseraReader",
			opts:    NewOptions(),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.valid()
			if (err != nil) != tc.wantErr {
				t.Errorf("valid() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

