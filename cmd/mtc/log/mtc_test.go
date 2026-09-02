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
	"context"
	stdasn1 "encoding/asn1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/entry"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/mtcproof"
	tposix "github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
	snote "golang.org/x/mod/sumdb/note"
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

func dummyValidity(notBefore, notAfter time.Time) []byte {
	// ASN.1 time structures (UTCTime/GeneralizedTime) only support second-level precision.
	// We truncate nanoseconds here so test assertions match marshaled/unmarshaled values.
	val := validity{
		NotBefore: notBefore.Truncate(time.Second),
		NotAfter:  notAfter.Truncate(time.Second),
	}
	der, err := stdasn1.Marshal(val)
	if err != nil {
		panic(fmt.Sprintf("dummyValidity failed to marshal: %v", err))
	}
	return der
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

func TestMTCLog_Accept(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	l := &MTCLog{maxCertLifetime: 24 * time.Hour}

	valid := func() TBSCertificateLogEntry {
		return TBSCertificateLogEntry{
			Issuer:                    dummySeq("issuer"),
			Validity:                  dummyValidity(now, now.Add(12*time.Hour)),
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
			name:    "validity lifetime equals max",
			mutate:  func(e *TBSCertificateLogEntry) { e.Validity = dummyValidity(now, now.Add(24*time.Hour)) },
			wantErr: false,
		},
		{
			name:    "invalid structural entry",
			mutate:  func(e *TBSCertificateLogEntry) { e.Issuer = nil },
			wantErr: true,
		},
		{
			name:    "unparseable validity",
			mutate:  func(e *TBSCertificateLogEntry) { e.Validity = dummySeq("unparseable-time") },
			wantErr: true,
		},
		{
			name:    "validity lifetime exceeds max",
			mutate:  func(e *TBSCertificateLogEntry) { e.Validity = dummyValidity(now, now.Add(48*time.Hour)) },
			wantErr: true,
		},
		{
			name:    "validity notAfter before notBefore",
			mutate:  func(e *TBSCertificateLogEntry) { e.Validity = dummyValidity(now.Add(24*time.Hour), now) },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := valid()
			tc.mutate(&e)
			err := l.accept(e)
			if (err != nil) != tc.wantErr {
				t.Errorf("accept() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

type dummyLogReader struct{ tessera.LogReader }

func (dummyLogReader) ReadCheckpoint(context.Context) ([]byte, error) {
	return []byte("test.origin\n0\nAAAA\n"), nil
}

type dummyLandmarksStorage struct{}

func (dummyLandmarksStorage) ReadLandmarks(context.Context) ([]byte, time.Time, error) {
	return nil, time.Time{}, os.ErrNotExist
}

func (dummyLandmarksStorage) UpdateLandmarks(_ context.Context, fn func([]byte, time.Time) ([]byte, error)) (time.Time, error) {
	_, _ = fn(nil, time.Time{})
	return time.Now(), nil
}

const (
	testSignerName = "oid/1.3.6.1.4.1.32473.106"
	testOrigin     = "oid/1.3.6.1.4.1.32473.106.0.1"
)

func mustTestSigner() note.SubtreeSigner {
	validKey, _, err := note.GenerateMLDSAKey(testSignerName)
	if err != nil {
		panic(err)
	}
	s, err := note.NewMLDSASigner(validKey)
	if err != nil {
		panic(err)
	}
	return s
}

func newDummyOptions() *Options {
	return NewOptions().
		WithTesseraReader(&dummyLogReader{}).
		WithLandmarksStorage(&dummyLandmarksStorage{}).
		WithOrigin(testOrigin).
		WithSubtreeSigner(mustTestSigner())
}

func TestMTCOptionsValid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    *Options
		wantErr bool
	}{
		{
			name:    "Valid",
			opts:    newDummyOptions(),
			wantErr: false,
		}, {
			name:    "Valid: custom landmark interval",
			opts:    newDummyOptions().WithLandmarkInterval(2 * time.Hour),
			wantErr: false,
		}, {
			name:    "Valid: order independence (interval before storage)",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithLandmarkInterval(2 * time.Hour).WithLandmarksStorage(&dummyLandmarksStorage{}).WithOrigin(testOrigin).WithSubtreeSigner(mustTestSigner()),
			wantErr: false,
		}, {
			name:    "Valid: custom poll period",
			opts:    newDummyOptions().WithAwaiterPollInterval(10 * time.Millisecond),
			wantErr: false,
		}, {
			name:    "Error: Negative poll period",
			opts:    newDummyOptions().WithAwaiterPollInterval(-10 * time.Millisecond),
			wantErr: true,
		}, {
			name:    "Error: No TesseraReader",
			opts:    NewOptions().WithLandmarksStorage(&dummyLandmarksStorage{}).WithOrigin(testOrigin).WithSubtreeSigner(mustTestSigner()),
			wantErr: true,
		}, {
			name:    "Error: No LandmarksStorage",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithOrigin(testOrigin).WithSubtreeSigner(mustTestSigner()),
			wantErr: true,
		}, {
			name:    "Error: No Origin",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithLandmarksStorage(&dummyLandmarksStorage{}).WithSubtreeSigner(mustTestSigner()),
			wantErr: true,
		}, {
			name:    "Error: No SubtreeSigner",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithLandmarksStorage(&dummyLandmarksStorage{}).WithOrigin(testOrigin),
			wantErr: true,
		}, {
			name:    "Valid: Zero LandmarkInterval defaults to recommended",
			opts:    newDummyOptions().WithLandmarkInterval(0),
			wantErr: false,
		}, {
			name:    "Error: Negative LandmarkInterval",
			opts:    newDummyOptions().WithLandmarkInterval(-1 * time.Hour),
			wantErr: true,
		}, {
			name:    "Error: Zero MaxCertLifetime",
			opts:    newDummyOptions().WithMaxCertLifetime(0),
			wantErr: true,
		}, {
			name:    "Error: Negative MaxCertLifetime",
			opts:    newDummyOptions().WithMaxCertLifetime(-1 * time.Hour),
			wantErr: true,
		}, {
			name:    "Error: MaxCertLifetime exceeds default max",
			opts:    newDummyOptions().WithMaxCertLifetime(DefaultMaxCertLifetime + 24*time.Hour),
			wantErr: true,
		}, {
			name:    "Error: MaxCertLifetime has sub-second precision",
			opts:    newDummyOptions().WithMaxCertLifetime(1*time.Hour + 500*time.Millisecond),
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

func TestRecommendedLandmarkInterval(t *testing.T) {
	tests := []struct {
		lifetime time.Duration
		want     time.Duration
	}{
		{lifetime: 1 * time.Hour, want: 1 * time.Hour},
		{lifetime: 7 * 24 * time.Hour, want: 1 * time.Hour},
		{lifetime: 15 * 24 * time.Hour, want: 1 * time.Hour},
		{lifetime: 16 * 24 * time.Hour, want: 2 * time.Hour},
		{lifetime: 25 * 24 * time.Hour, want: 2 * time.Hour},
		{lifetime: 30 * 24 * time.Hour, want: 2 * time.Hour},
		{lifetime: 31 * 24 * time.Hour, want: 4 * time.Hour},
		{lifetime: 47 * 24 * time.Hour, want: 4 * time.Hour},
	}
	for _, tc := range tests {
		if got := RecommendedLandmarkInterval(tc.lifetime); got != tc.want {
			t.Errorf("RecommendedLandmarkInterval(%v) = %v, want %v", tc.lifetime, got, tc.want)
		}
	}
}

func TestFormatOriginAndSigner(t *testing.T) {
	for _, tc := range []struct {
		name           string
		caID           string
		logNumber      uint64
		wantOrigin     string
		wantSignerName string
		wantErr        bool
	}{
		{
			name:           "valid single integer caID",
			caID:           "32473",
			logNumber:      1,
			wantOrigin:     "oid/1.3.6.1.4.1.32473.0.1",
			wantSignerName: "oid/1.3.6.1.4.1.32473",
			wantErr:        false,
		},
		{
			name:           "valid multi dot caID",
			caID:           "32473.106",
			logNumber:      42,
			wantOrigin:     "oid/1.3.6.1.4.1.32473.106.0.42",
			wantSignerName: "oid/1.3.6.1.4.1.32473.106",
			wantErr:        false,
		},
		{
			name:      "empty caID",
			caID:      "",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "caID leading dot",
			caID:      ".32473",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "caID trailing dot",
			caID:      "32473.",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "caID invalid char",
			caID:      "32473a",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "logNumber zero",
			caID:      "32473",
			logNumber: 0,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin, signerName, err := formatOriginAndSigner(tc.caID, tc.logNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("formatOriginAndSigner(%q, %d) error = %v, wantErr %v", tc.caID, tc.logNumber, err, tc.wantErr)
			}
			if !tc.wantErr {
				if origin != tc.wantOrigin {
					t.Errorf("origin = %q, want %q", origin, tc.wantOrigin)
				}
				if signerName != tc.wantSignerName {
					t.Errorf("signerName = %q, want %q", signerName, tc.wantSignerName)
				}
			}
		})
	}
}

func TestCreateSignerAndOrigin(t *testing.T) {
	validKey, _, err := note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey failed: %v", err)
	}

	for _, tc := range []struct {
		name           string
		caID           string
		logNumber      uint64
		privKey        string
		wantOrigin     string
		wantSignerName string
		wantErr        bool
	}{
		{
			name:           "valid matching key and caID",
			caID:           "32473.106",
			logNumber:      1,
			privKey:        validKey,
			wantOrigin:     "oid/1.3.6.1.4.1.32473.106.0.1",
			wantSignerName: "oid/1.3.6.1.4.1.32473.106",
			wantErr:        false,
		},
		{
			name:      "empty caID",
			caID:      "",
			logNumber: 1,
			privKey:   validKey,
			wantErr:   true,
		},
		{
			name:      "zero log number",
			caID:      "32473.106",
			logNumber: 0,
			privKey:   validKey,
			wantErr:   true,
		},
		{
			name:      "invalid private key string",
			caID:      "32473.106",
			logNumber: 1,
			privKey:   "not-a-valid-key",
			wantErr:   true,
		},
		{
			name:      "caID mismatch with signer key name",
			caID:      "99999",
			logNumber: 1,
			privKey:   validKey,
			wantErr:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			origin, signer, err := CreateSignerAndOrigin(tc.caID, tc.logNumber, tc.privKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CreateSignerAndOrigin(%q, %d, ...) error = %v, wantErr %v", tc.caID, tc.logNumber, err, tc.wantErr)
			}
			if !tc.wantErr {
				if origin != tc.wantOrigin {
					t.Errorf("origin = %q, want %q", origin, tc.wantOrigin)
				}
				if signer.Name() != tc.wantSignerName {
					t.Errorf("signer.Name() = %q, want %q", signer.Name(), tc.wantSignerName)
				}
			}
		})
	}
}

func TestNewMTCLog(t *testing.T) {
	ctx := t.Context()

	badSignerKey, _, err := note.GenerateMLDSAKey("example.com/invalid")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey failed: %v", err)
	}
	badSigner, err := note.NewMLDSASigner(badSignerKey)
	if err != nil {
		t.Fatalf("NewMLDSASigner failed: %v", err)
	}

	_, vkey1, err := note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	witURL, _ := url.Parse("http://wit1.example.com")
	w1, err := tessera.NewWitness(vkey1, witURL)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}
	witnessGroup := tessera.NewWitnessGroup(1, w1)

	tests := []struct {
		name        string
		appender    *tessera.Appender
		opts        *Options
		wantGateway bool
		wantErr     bool
	}{
		{
			name:     "valid default options (47-day certs)",
			appender: &tessera.Appender{},
			opts:     newDummyOptions(),
			wantErr:  false,
		},
		{
			name:     "valid 7-day certs with default interval",
			appender: &tessera.Appender{},
			opts:     newDummyOptions().WithMaxCertLifetime(7 * 24 * time.Hour),
			wantErr:  false,
		},
		{
			name:     "valid explicit 0 interval defaults to recommended",
			appender: &tessera.Appender{},
			opts:     newDummyOptions().WithLandmarkInterval(0),
			wantErr:  false,
		},
		{
			name:     "valid custom landmark interval",
			appender: &tessera.Appender{},
			opts: newDummyOptions().
				WithMaxCertLifetime(20 * 24 * time.Hour).
				WithLandmarkInterval(2 * time.Hour),
			wantErr: false,
		},
		{
			name:        "valid with single witness policy",
			appender:    &tessera.Appender{},
			opts:        newDummyOptions().WithSubtreeWitnesses(witnessGroup),
			wantGateway: true,
			wantErr:     false,
		},
		{
			name:        "valid with empty witness group",
			appender:    &tessera.Appender{},
			opts:        newDummyOptions().WithSubtreeWitnesses(tessera.WitnessGroup{}),
			wantGateway: false,
			wantErr:     false,
		},
		{
			name:     "nil appender",
			appender: nil,
			opts:     newDummyOptions(),
			wantErr:  true,
		},
		{
			name:     "nil options",
			appender: &tessera.Appender{},
			opts:     nil,
			wantErr:  true,
		},
		{
			name:     "invalid subtree signer name",
			appender: &tessera.Appender{},
			opts:     newDummyOptions().WithSubtreeSigner(badSigner),
			wantErr:  true,
		},
		{
			name:     "origin mismatch with subtree signer",
			appender: &tessera.Appender{},
			opts:     newDummyOptions().WithOrigin("oid/1.3.6.1.4.1.99999.0.1"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l, err := NewMTCLog(ctx, tc.appender, tc.opts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewMTCLog() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if l == nil {
					t.Fatal("NewMTCLog() returned nil instance on success")
				}
				if got := l.subtreeGateway != nil; got != tc.wantGateway {
					t.Errorf("has subtreeGateway = %v, want %v", got, tc.wantGateway)
				}
			}
		})
	}
}

type parsedMTCProof struct {
	Extensions     []byte
	Start          uint64
	End            uint64
	InclusionProof [][]byte
	Signatures     []mtcproof.SubtreeSignature
}

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

func unmarshalMTCProof(data []byte) (*parsedMTCProof, error) {
	s := cryptobyte.String(data)

	var extensions cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&extensions) {
		return nil, errors.New("malformed extensions")
	}

	var p parsedMTCProof
	p.Extensions = append([]byte(nil), extensions...)

	if !readUint48(&s, &p.Start) {
		return nil, errors.New("malformed start index")
	}
	if !readUint48(&s, &p.End) {
		return nil, errors.New("malformed end index")
	}

	var incProof cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&incProof) {
		return nil, errors.New("malformed inclusion_proof")
	}
	for !incProof.Empty() {
		var hash []byte
		if !incProof.ReadBytes(&hash, 32) {
			return nil, errors.New("malformed hash in inclusion_proof")
		}
		p.InclusionProof = append(p.InclusionProof, hash)
	}

	var sigs cryptobyte.String
	if !s.ReadUint16LengthPrefixed(&sigs) {
		return nil, errors.New("malformed signatures")
	}
	for !sigs.Empty() {
		var cosignerID cryptobyte.String
		if !sigs.ReadUint8LengthPrefixed(&cosignerID) || len(cosignerID) == 0 {
			return nil, errors.New("malformed cosigner_id in signatures")
		}
		var sig cryptobyte.String
		if !sigs.ReadUint16LengthPrefixed(&sig) {
			return nil, errors.New("malformed signature in signatures")
		}
		p.Signatures = append(p.Signatures, mtcproof.SubtreeSignature{
			CosignerID: append([]byte(nil), cosignerID...),
			Signature:  append([]byte(nil), sig...),
		})
	}

	if !s.Empty() {
		return nil, errors.New("trailing bytes after MTCProof")
	}
	return &p, nil
}

func setupTestWitness(t *testing.T) (tessera.WitnessGroup, note.SubtreeVerifier) {
	t.Helper()
	sKey, vKey, err := note.GenerateMLDSAKey("oid/1.3.6.1.4.1.32473.106.1")
	if err != nil {
		t.Fatalf("GenerateMLDSAKey: %v", err)
	}
	signer, _ := note.NewMLDSASigner(sKey)
	verifier, _ := note.NewMLDSAVerifier(vKey)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/add-checkpoint":
			var unverified *snote.UnverifiedNoteError
			if _, err := snote.Open(body, snote.VerifierList()); !errors.As(err, &unverified) {
				http.Error(w, fmt.Sprintf("invalid checkpoint note: %v", err), http.StatusBadRequest)
				return
			}
			_, cpText, _ := strings.Cut(unverified.Note.Text, "\n\n")
			signed, err := snote.Sign(&snote.Note{Text: cpText}, signer)
			if err != nil {
				http.Error(w, fmt.Sprintf("sign checkpoint: %v", err), http.StatusInternalServerError)
				return
			}
			if _, err := w.Write(signed[len(cpText)+1:]); err != nil {
				t.Errorf("write /add-checkpoint response: %v", err)
			}
		case "/sign-subtree":
			lines := strings.Split(string(body), "\n")
			var start, end uint64
			if _, err := fmt.Sscanf(lines[0], "subtree %d %d", &start, &end); err != nil {
				http.Error(w, fmt.Sprintf("scan subtree range: %v", err), http.StatusBadRequest)
				return
			}
			subRoot, err := base64.StdEncoding.DecodeString(lines[1])
			if err != nil {
				http.Error(w, fmt.Sprintf("decode subRoot: %v", err), http.StatusBadRequest)
				return
			}
			rawSig, err := signer.SignSubtree(0, testOrigin, start, end, subRoot)
			if err != nil {
				http.Error(w, fmt.Sprintf("sign subtree: %v", err), http.StatusInternalServerError)
				return
			}
			buf := binary.BigEndian.AppendUint32(nil, signer.KeyHash())
			if _, err := fmt.Fprintf(w, "— %s %s\n", signer.Name(), base64.StdEncoding.EncodeToString(append(buf, rawSig...))); err != nil {
				t.Errorf("write /sign-subtree response: %v", err)
			}
		}
	}))
	t.Cleanup(ts.Close)

	witURL, _ := url.Parse(ts.URL)
	w, err := tessera.NewWitness(vKey, witURL)
	if err != nil {
		t.Fatalf("NewWitness: %v", err)
	}
	return tessera.NewWitnessGroup(1, w), verifier
}

func setupTestMTCLog(t *testing.T) (*MTCLog, note.SubtreeVerifier) {
	t.Helper()
	ctx := t.Context()
	storageDir := t.TempDir()

	driver, err := tposix.New(ctx, tposix.Config{Path: storageDir})
	if err != nil {
		t.Fatalf("Failed to initialize POSIX storage: %v", err)
	}

	sk := "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
	signer, err := snote.NewSigner(sk)
	if err != nil {
		t.Fatalf("Failed to create test signer: %v", err)
	}

	witGroup, witVerifier := setupTestWitness(t)

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithBatching(4, 500*time.Millisecond).
		WithCheckpointInterval(500 * time.Millisecond).
		WithWitnesses(witGroup, &tessera.WitnessOptions{Timeout: time.Second})
	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		t.Fatalf("Failed to initialize Tessera appender: %v", err)
	}

	mtcLog, err := NewMTCLog(ctx, appender, NewOptions().
		WithTesseraReader(reader).
		WithAwaiterPollInterval(20*time.Millisecond).
		WithLandmarksStorage(dummyLandmarksStorage{}).
		WithMaxCertLifetime(7*24*time.Hour).
		WithOrigin(testOrigin).
		WithSubtreeSigner(mustTestSigner()).
		WithSubtreeWitnesses(witGroup))
	if err != nil {
		t.Fatalf("Failed to initialize MTC log: %v", err)
	}
	return mtcLog, witVerifier
}

func TestMTCLog_AddTBS(t *testing.T) {
	ctx := t.Context()
	mtcLog, witVerifier := setupTestMTCLog(t)
	now := time.Now().Truncate(time.Second)

	makeEntry := func(id int) TBSCertificateLogEntry {
		return TBSCertificateLogEntry{
			Issuer:                    dummySeq(fmt.Sprintf("issuer%d", id)),
			Validity:                  dummyValidity(now, now.Add(24*time.Hour)),
			Subject:                   dummySeq(fmt.Sprintf("subject%d", id)),
			SubjectPublicKeyAlgorithm: dummySeq("algo"),
			SubjectPublicKeyInfoHash:  make([]byte, 32),
		}
	}

	entries := make([]TBSCertificateLogEntry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = makeEntry(i)
	}

	// Add first 4 entries concurrently to form a single batch of size 4 in [0, 4)
	responses := make([]*AddTBSRsp, 5)
	entriesByIndex := make([]TBSCertificateLogEntry, 5)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(entry TBSCertificateLogEntry) {
			defer wg.Done()
			rsp, err := mtcLog.AddTBS(ctx, entry)
			if err != nil {
				t.Errorf("AddTBS error: %v", err)
				return
			}
			mu.Lock()
			responses[rsp.Index] = rsp
			entriesByIndex[rsp.Index] = entry
			mu.Unlock()
		}(entries[i])
	}
	wg.Wait()

	// Add 5th entry individually after the initial batch is sequenced
	rsp5, err := mtcLog.AddTBS(ctx, entries[4])
	if err != nil {
		t.Fatalf("AddTBS(entries[4]) error: %v", err)
	}
	responses[rsp5.Index] = rsp5
	entriesByIndex[rsp5.Index] = entries[4]

	t.Run("invalid entry fails validation", func(t *testing.T) {
		invalidEntry := entries[0]
		invalidEntry.Issuer = nil
		if _, err := mtcLog.AddTBS(ctx, invalidEntry); err == nil {
			t.Error("AddTBS(invalidEntry) expected error, got nil")
		}
	})

	tests := []struct {
		name         string
		entryIdx     int
		wantStart    uint64
		wantEnd      uint64
		wantProofLen int
	}{
		{name: "entry 0 in subtree [0, 2)", entryIdx: 0, wantStart: 0, wantEnd: 2, wantProofLen: 1},
		{name: "entry 1 in subtree [0, 2)", entryIdx: 1, wantStart: 0, wantEnd: 2, wantProofLen: 1},
		{name: "entry 2 in subtree [2, 4)", entryIdx: 2, wantStart: 2, wantEnd: 4, wantProofLen: 1},
		{name: "entry 3 in subtree [2, 4)", entryIdx: 3, wantStart: 2, wantEnd: 4, wantProofLen: 1},
		{name: "entry 4 in single-entry subtree [4, 5)", entryIdx: 4, wantStart: 4, wantEnd: 5, wantProofLen: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rsp := responses[tc.entryIdx]
			if rsp == nil {
				t.Fatalf("responses[%d] is nil", tc.entryIdx)
			}
			if rsp.Index != uint64(tc.entryIdx) {
				t.Errorf("Index = %d, want %d", rsp.Index, tc.entryIdx)
			}
			proofData, err := unmarshalMTCProof(rsp.MTCProof)
			if err != nil {
				t.Fatalf("unmarshalMTCProof error: %v", err)
			}
			if proofData.Start != tc.wantStart || proofData.End != tc.wantEnd {
				t.Errorf("subtree = [%d, %d), want [%d, %d)", proofData.Start, proofData.End, tc.wantStart, tc.wantEnd)
			}
			if len(proofData.Extensions) != 0 {
				t.Errorf("Extensions = %q, want empty", proofData.Extensions)
			}
			if len(proofData.InclusionProof) != tc.wantProofLen {
				t.Errorf("InclusionProof length = %d, want %d", len(proofData.InclusionProof), tc.wantProofLen)
			}
			m, err := entriesByIndex[tc.entryIdx].Marshal()
			if err != nil {
				t.Fatalf("entries[%d].Marshal error: %v", tc.entryIdx, err)
			}
			e := entry.New(m)
			eb, err := e.Marshal()
			if err != nil {
				t.Fatalf("entry.Marshal error: %v", err)
			}
			leafHash := rfc6962.DefaultHasher.HashLeaf(eb)
			offset := uint64(tc.entryIdx) - tc.wantStart
			treeSize := tc.wantEnd - tc.wantStart
			subRoot, err := proof.RootFromInclusionProof(rfc6962.DefaultHasher, offset, treeSize, leafHash, proofData.InclusionProof)
			if err != nil {
				t.Fatalf("RootFromInclusionProof(entry%d): %v", tc.entryIdx, err)
			}
			if len(subRoot) != 32 {
				t.Fatalf("subRoot length = %d, want 32", len(subRoot))
			}
			if len(proofData.Signatures) != 2 {
				t.Fatalf("got %d signatures, want 2", len(proofData.Signatures))
			}
			// SPEC: draft-ietf-plants-merkle-tree-certs section 6.2.
			// SubtreeSignature.Signature contains the raw signature.
			// SPEC: https://c2sp.org/tlog-cosignature
			// note.SubtreeVerifier expects a C2SP timestamped_signature prefixed with the 8-byte u64 timestamp.
			reconstructCosig := func(rawSig []byte) []byte {
				return append(make([]byte, 8), rawSig...)
			}
			if !mtcLog.subtreeSigner.Verifier().VerifySubtree(0, mtcLog.origin, tc.wantStart, tc.wantEnd, subRoot, reconstructCosig(proofData.Signatures[0].Signature)) {
				t.Errorf("VerifySubtree failed for log signature on entry%d", tc.entryIdx)
			}
			if !bytes.Equal(proofData.Signatures[0].CosignerID, mtcLog.logCosignerID) {
				t.Errorf("CosignerID = %x, want %x", proofData.Signatures[0].CosignerID, mtcLog.logCosignerID)
			}
			if !witVerifier.VerifySubtree(0, mtcLog.origin, tc.wantStart, tc.wantEnd, subRoot, reconstructCosig(proofData.Signatures[1].Signature)) {
				t.Errorf("VerifySubtree failed for witness signature on entry%d", tc.entryIdx)
			}
		})
	}
}
