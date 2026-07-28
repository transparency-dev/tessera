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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/transparency-dev/tessera"
	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
)

type MTCLog struct {
	a *tessera.Appender
}

// MTCProof represents an MTC inclusion proof as per
// draft-ietf-plants-merkle-tree-certs section 6.2.
type MTCProof struct{}

// TBSCertificateLogEntry represents a log entry as per
// draft-ietf-plants-merkle-tree-certs section 7.2.
type TBSCertificateLogEntry struct {
	Version                   int64  `json:"version"`                   // 0 = v1 (default), tag 0
	Issuer                    []byte `json:"issuer"`                    // Raw DER-encoded SEQUENCE
	Validity                  []byte `json:"validity"`                  // Raw DER-encoded SEQUENCE
	Subject                   []byte `json:"subject"`                   // Raw DER-encoded SEQUENCE
	SubjectPublicKeyAlgorithm []byte `json:"subjectPublicKeyAlgorithm"` // Raw DER-encoded SEQUENCE
	SubjectPublicKeyInfoHash  []byte `json:"subjectPublicKeyInfoHash"`  // Raw 32-byte public key hash
	IssuerUniqueID            []byte `json:"issuerUniqueId,omitempty"`  // Optional raw IMPLICIT BIT STRING bytes, tag 1
	SubjectUniqueID           []byte `json:"subjectUniqueId,omitempty"` // Optional raw IMPLICIT BIT STRING bytes, tag 2
	Extensions                []byte `json:"extensions,omitempty"`      // Optional raw EXPLICIT SEQUENCE bytes, tag 3
}

// Marshal returns the contents octets of the TBSCertificateLogEntry's
// DER encoding, i.e. WITHOUT the outer SEQUENCE tag and length prefix.
//
// SPEC: draft-ietf-plants-merkle-tree-certs section 5.2.1.
// "tbs_cert_entry_data contains the contents octets (i.e. excluding the initial
// identifier and length octets) of the DER"
func (e *TBSCertificateLogEntry) Marshal() ([]byte, error) {
	// TBSCertificateLogEntry ::= SEQUENCE {
	//   version               [0] EXPLICIT Version DEFAULT v1,
	//   issuer                    Name,
	//   validity                  Validity,
	//   subject                   Name,
	//   subjectPublicKeyAlgorithm AlgorithmIdentifier{PUBLIC-KEY,
	//                                 {PublicKeyAlgorithms}},
	//   subjectPublicKeyInfoHash  OCTET STRING,
	//   issuerUniqueID        [1] IMPLICIT UniqueIdentifier OPTIONAL,
	//   subjectUniqueID       [2] IMPLICIT UniqueIdentifier OPTIONAL,
	//   extensions            [3] EXPLICIT Extensions{{CertExtensions}}
	//                                          OPTIONAL
	// }
	if err := e.Validate(); err != nil {
		return nil, err
	}

	var b cryptobyte.Builder

	// SPEC: RFC 5280 section 4.1.2.1.
	// "If only basic fields are present, the version SHOULD be 1 (the value is
	// omitted from the certificate as the default value)"
	// "The encoding of a set value or sequence value shall not include an
	// encoding for any component value which is equal to its default value."
	//
	// SPEC: ITU-T X.690 section 11.5
	// "The encoding of a set value or sequence value shall not include an encoding
	//  for any component value which is equal to its default value."
	if e.Version != 0 {
		b.AddASN1(asn1.Tag(0).ContextSpecific().Constructed(), func(b *cryptobyte.Builder) {
			b.AddASN1Int64(e.Version)
		})
	}

	b.AddBytes(e.Issuer)
	b.AddBytes(e.Validity)
	b.AddBytes(e.Subject)
	b.AddBytes(e.SubjectPublicKeyAlgorithm)
	b.AddASN1OctetString(e.SubjectPublicKeyInfoHash)

	if len(e.IssuerUniqueID) > 0 {
		b.AddBytes(e.IssuerUniqueID)
	}
	if len(e.SubjectUniqueID) > 0 {
		b.AddBytes(e.SubjectUniqueID)
	}
	if len(e.Extensions) > 0 {
		b.AddBytes(e.Extensions)
	}

	return b.Bytes()
}

func validateASN1Element(data []byte, expectedTag asn1.Tag, fieldName string) error {
	if len(data) == 0 {
		return nil
	}
	s := cryptobyte.String(data)
	var elem cryptobyte.String
	if !s.ReadASN1(&elem, expectedTag) || !s.Empty() {
		return fmt.Errorf("%s: malformed ASN.1 or incorrect tag (expected %v)", fieldName, expectedTag)
	}
	return nil
}

// Validate checks that TBSCertificateLogEntry fields are correct.
// It checks that mandatory fields are present, and that all fields are well
// formatted.
func (e *TBSCertificateLogEntry) Validate() error {
	if e.Version < 0 || e.Version > 2 {
		return fmt.Errorf("invalid version %d", e.Version)
	}

	switch {
	case len(e.Issuer) == 0:
		return errors.New("issuer: mandatory field is missing")
	case len(e.Validity) == 0:
		return errors.New("validity: mandatory field is missing")
	case len(e.Subject) == 0:
		return errors.New("subject: mandatory field is missing")
	case len(e.SubjectPublicKeyAlgorithm) == 0:
		return errors.New("subjectPublicKeyAlgorithm: mandatory field is missing")
	}

	if err := validateASN1Element(e.Issuer, asn1.SEQUENCE, "issuer"); err != nil {
		return err
	}
	if err := validateASN1Element(e.Validity, asn1.SEQUENCE, "validity"); err != nil {
		return err
	}
	if err := validateASN1Element(e.Subject, asn1.SEQUENCE, "subject"); err != nil {
		return err
	}
	if err := validateASN1Element(e.SubjectPublicKeyAlgorithm, asn1.SEQUENCE, "subjectPublicKeyAlgorithm"); err != nil {
		return err
	}
	if err := validateASN1Element(e.IssuerUniqueID, asn1.Tag(1).ContextSpecific(), "issuerUniqueID"); err != nil {
		return err
	}
	if err := validateASN1Element(e.SubjectUniqueID, asn1.Tag(2).ContextSpecific(), "subjectUniqueID"); err != nil {
		return err
	}
	if err := validateASN1Element(e.Extensions, asn1.Tag(3).ContextSpecific().Constructed(), "extensions"); err != nil {
		return err
	}

	// SPEC: https://c2sp.org/mtc-log
	// "MTC CAs following this profile MUST use SHA-256 as the hash algorithm".
	// Hence, the hash size MUST be 32 bytes.
	if len(e.SubjectPublicKeyInfoHash) != sha256.Size {
		return fmt.Errorf("subjectPublicKeyInfoHash must be %d bytes, got %d", sha256.Size, len(e.SubjectPublicKeyInfoHash))
	}
	return nil
}

// NewMTCLog creates a new MTCLog compliant with
// draft-ietf-plants-merkle-tree-certs and http://c2sp.org/mtc-tlog.
func NewMTCLog(ctx context.Context, a *tessera.Appender) *MTCLog {
	// TODO: schedule landmark publishing
	return &MTCLog{a}
}

// AddTBS adds a TBSCertificateLogEntry to the log.
func (l *MTCLog) AddTBS(ctx context.Context, e TBSCertificateLogEntry) (uint64, MTCProof, error) {
	// TODO: marshal
	// TODO: add to log
	// TODO: get subtree cosignatures
	// TODO: build MTCProof
	return 0, MTCProof{}, nil
}

// ProofToLandmark builds an MTCProof for the entry at idx to a
// published landmark.
// TODO: better arg
func (l *MTCLog) ProofToLandmark(ctx context.Context, idx uint64) (MTCProof, error) {
	// TODO check if landmark is available
	//   If available, build and return an MTCProof
	//   If not, return a clever error
	return MTCProof{}, nil
}
