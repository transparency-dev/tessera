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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/transparency-dev/formats/note"
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
	return nil, nil
}

type dummyLandmarksStorage struct{}

func (dummyLandmarksStorage) ReadLandmarks(context.Context) ([]byte, time.Time, error) {
	return nil, time.Time{}, os.ErrNotExist
}

func (dummyLandmarksStorage) UpdateLandmarks(_ context.Context, fn func([]byte, time.Time) ([]byte, error)) (time.Time, error) {
	_, _ = fn(nil, time.Time{})
	return time.Now(), nil
}

func newDummyOptions() *Options {
	return NewOptions().
		WithTesseraReader(&dummyLogReader{}).
		WithLandmarksStorage(&dummyLandmarksStorage{})
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
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}).WithLandmarkInterval(2 * time.Hour).WithLandmarksStorage(&dummyLandmarksStorage{}),
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
			opts:    NewOptions().WithLandmarksStorage(&dummyLandmarksStorage{}),
			wantErr: true,
		}, {
			name:    "Error: No LandmarksStorage",
			opts:    NewOptions().WithTesseraReader(&dummyLogReader{}),
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
			caID:           "44363",
			logNumber:      1,
			wantOrigin:     "oid/1.3.6.1.4.1.44363.0.1",
			wantSignerName: "oid/1.3.6.1.4.1.44363",
			wantErr:        false,
		},
		{
			name:           "valid multi dot caID",
			caID:           "44363.47",
			logNumber:      42,
			wantOrigin:     "oid/1.3.6.1.4.1.44363.47.0.42",
			wantSignerName: "oid/1.3.6.1.4.1.44363.47",
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
			caID:      ".44363",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "caID trailing dot",
			caID:      "44363.",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "caID invalid char",
			caID:      "44363a",
			logNumber: 1,
			wantErr:   true,
		},
		{
			name:      "logNumber zero",
			caID:      "44363",
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
	validKey, _, err := note.GenerateMLDSAKey("oid/1.3.6.1.4.1.44363.47")
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
			caID:           "44363.47",
			logNumber:      1,
			privKey:        validKey,
			wantOrigin:     "oid/1.3.6.1.4.1.44363.47.0.1",
			wantSignerName: "oid/1.3.6.1.4.1.44363.47",
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
			caID:      "44363.47",
			logNumber: 0,
			privKey:   validKey,
			wantErr:   true,
		},
		{
			name:      "invalid private key string",
			caID:      "44363.47",
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := newDummyOptions()

	t.Run("nil appender", func(t *testing.T) {
		if _, err := NewMTCLog(ctx, nil, opts); err == nil {
			t.Error("NewMTCLog with nil appender expected error, got nil")
		}
	})

	t.Run("nil options", func(t *testing.T) {
		if _, err := NewMTCLog(ctx, &tessera.Appender{}, nil); err == nil {
			t.Error("NewMTCLog with nil options expected error, got nil")
		}
	})

	t.Run("valid default options (47-day certs)", func(t *testing.T) {
		logInst, err := NewMTCLog(ctx, &tessera.Appender{}, opts)
		if err != nil {
			t.Fatalf("NewMTCLog unexpected error: %v", err)
		}
		if logInst == nil {
			t.Fatal("NewMTCLog returned nil instance")
		}
	})

	t.Run("valid 7-day certs with default interval", func(t *testing.T) {
		opts7d := newDummyOptions().WithMaxCertLifetime(7 * 24 * time.Hour)
		logInst, err := NewMTCLog(ctx, &tessera.Appender{}, opts7d)
		if err != nil {
			t.Fatalf("NewMTCLog unexpected error: %v", err)
		}
		if logInst == nil {
			t.Fatal("NewMTCLog returned nil instance")
		}
	})

	t.Run("valid explicit 0 interval defaults to recommended", func(t *testing.T) {
		opts0 := newDummyOptions().WithLandmarkInterval(0)
		logInst, err := NewMTCLog(ctx, &tessera.Appender{}, opts0)
		if err != nil {
			t.Fatalf("NewMTCLog unexpected error: %v", err)
		}
		if logInst == nil {
			t.Fatal("NewMTCLog returned nil instance")
		}
	})

	t.Run("valid custom landmark interval", func(t *testing.T) {
		optsCustom := newDummyOptions().
			WithMaxCertLifetime(20 * 24 * time.Hour).
			WithLandmarkInterval(2 * time.Hour)
		logInst, err := NewMTCLog(ctx, &tessera.Appender{}, optsCustom)
		if err != nil {
			t.Fatalf("NewMTCLog unexpected error: %v", err)
		}
		if logInst == nil {
			t.Fatal("NewMTCLog returned nil instance")
		}
	})
}
