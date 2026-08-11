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
	"crypto/x509"
	stdasn1 "encoding/asn1"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/checkpoint"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/entry"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/landmark"
	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/mtcproof"
	"golang.org/x/crypto/cryptobyte"
	"golang.org/x/crypto/cryptobyte/asn1"
)

const (
	// DefaultAwaiterPollInterval is the fallback polling period for the publication awaiter.
	DefaultAwaiterPollInterval = 200 * time.Millisecond

	// SPEC: CQRP Policy v0.2.0
	// "MTC CA Operators MUST NOT issue Subscriber certificates with a
	// validity period exceeding 47 days."
	DefaultMaxCertLifetime = 47 * 24 * time.Hour

	// landmarkPublicationBuffer is an additional buffer added to RetryAfter in ProofToLandmark
	// responses to account for landmark publication latency (integrating entries, signing, and storage writes).
	landmarkPublicationBuffer = 500 * time.Millisecond

	// maxRetryAfterJitter is the upper bound of randomized jitter added to RetryAfter in ProofToLandmark
	// responses to desynchronize polling clients and prevent a thundering herd when a new landmark closes.
	maxRetryAfterJitter = 15 * time.Second
)

var (
	// ErrTooOld indicates that the requested index precedes the earliest available active landmark.
	ErrTooOld = errors.New("entry precedes earliest active landmark")

	// ErrExceedsTreeSize indicates that the requested index exceeds the current log tree size.
	ErrExceedsTreeSize = errors.New("index exceeds current log tree size")
)

// RecommendedLandmarkInterval returns the recommended landmark publication interval
// for a given maximum certificate lifetime, based on CQRP Policy v0.2.0 values.
//
// - Up to 15 days: 1 hour (CQRP recommendation for 7-day certs)
// - Up to 30 days: 2 hours
// - Up to 47 days: 4 hours (CQRP recommendation for 47-day certs)
func RecommendedLandmarkInterval(maxCertLifetime time.Duration) time.Duration {
	// SPEC: CQRP Policy v0.2.0
	// "For CA Cosigners with a maximum permitted certificate validity of up to
	// 7 days, MTC CA landmarks SHOULD be generated approximately every hour"
	if maxCertLifetime <= 15*24*time.Hour {
		return 1 * time.Hour
	}
	if maxCertLifetime <= 30*24*time.Hour {
		return 2 * time.Hour
	}
	// SPEC: CQRP Policy v0.2.0
	// "For CA Cosigners with a maximum permitted certificate validity of up to
	// 47 days, MTC CA landmarks SHOULD be generated approximately 4 hour"
	return 4 * time.Hour
}

const (
	// SPEC: draft-ietf-plants-merkle-tree-certs section 5.3.1.
	// "cosigner_name and log_origin are computed from the cosigner ID and the
	// issuance log's ID (Section 5.1), respectively. They contain the concatenation of:
	//   - The 16-byte ASCII string oid/1.3.6.1.4.1.
	oidPrefix = "oid/1.3.6.1.4.1."
)

// Options holds settings for configuring MTCLog instances.
type Options struct {
	reader           tessera.LogReader
	pollPeriod       time.Duration
	landmarkStorage  landmark.LandmarksStorage
	landmarkInterval time.Duration
	maxCertLifetime  time.Duration
}

// NewOptions creates a new options struct for configuring MTCLog instances.
func NewOptions() *Options {
	return &Options{
		pollPeriod:      DefaultAwaiterPollInterval,
		maxCertLifetime: DefaultMaxCertLifetime,
	}
}

// valid returns an error if an invalid combination of options has been set, or nil otherwise.
func (o *Options) valid() error {
	if o.reader == nil {
		return errors.New("invalid Options: WithTesseraReader must be set")
	}
	if o.landmarkStorage == nil {
		return errors.New("invalid Options: WithLandmarksStorage must be set")
	}
	if o.landmarkInterval < 0 {
		return errors.New("invalid Options: WithLandmarkInterval must be >= 0")
	}
	if o.pollPeriod < 0 {
		return errors.New("invalid Options: pollPeriod must be >= 0")
	}
	if o.maxCertLifetime <= 0 || o.maxCertLifetime > DefaultMaxCertLifetime {
		return fmt.Errorf("invalid Options: WithMaxCertLifetime must be > 0 and <= %v", DefaultMaxCertLifetime)
	}
	if o.maxCertLifetime%time.Second != 0 {
		return errors.New("invalid Options: WithMaxCertLifetime must have second precision (no fractional seconds)")
	}
	return nil
}

// WithTesseraReader configures the Tessera LogReader used for checkpoint reading.
func (o *Options) WithTesseraReader(r tessera.LogReader) *Options {
	o.reader = r
	return o
}

// WithAwaiterPollInterval configures the polling period for the publication awaiter.
// duration MUST be strictly positive, otherwise valid() will fail.
// If unset, falls back to DefaultAwaiterPollInterval.
func (o *Options) WithAwaiterPollInterval(duration time.Duration) *Options {
	o.pollPeriod = duration
	return o
}

// WithLandmarksStorage configures the LandmarksStorage backend used for active landmarks.
// storage MUST not be nil, otherwise valid() will fail.
func (o *Options) WithLandmarksStorage(storage landmark.LandmarksStorage) *Options {
	o.landmarkStorage = storage
	return o
}

// WithLandmarkInterval configures the interval between publishing active landmarks.
// duration MUST be >= 0, otherwise valid() will fail.
// If 0 or unset, defaults to RecommendedLandmarkInterval(maxCertLifetime).
func (o *Options) WithLandmarkInterval(duration time.Duration) *Options {
	o.landmarkInterval = duration
	return o
}

// WithMaxCertLifetime configures a maximum validity duration for incoming certificates.
// duration MUST be strictly positive and smaller than or equal to 47 days.
//
// SPEC: CQRP Policy v0.2.0
// "MTC CA Operators MUST NOT issue Subscriber certificates with a
// validity period exceeding 47 days."
func (o *Options) WithMaxCertLifetime(duration time.Duration) *Options {
	o.maxCertLifetime = duration
	return o
}

type MTCLog struct {
	a                 *tessera.Appender
	reader            tessera.LogReader
	awaiter           *tessera.PublicationAwaiter
	landmarkPublisher *landmark.Publisher
	maxCertLifetime   time.Duration
}

// AddTBSRsp contains enough information from the log
// to build a standalone certificate.
type AddTBSRsp struct {
	Index    uint64 `json:"index"`
	MTCProof []byte `json:"mtcProof"`
}

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

// ProofToLandmarkRsp contains the landmark inclusion proof.
type ProofToLandmarkRsp struct {
	// MTCProof is the TLS-encoded landmark-relative certificate proof.
	MTCProof []byte `json:"mtcProof"`
}

type validity struct {
	NotBefore time.Time
	NotAfter  time.Time
}

func parseValidity(data []byte) (validity, error) {
	var v validity
	rest, err := stdasn1.Unmarshal(data, &v)
	if err != nil {
		return validity{}, fmt.Errorf("unmarshal validity: %v", err)
	}
	if len(rest) > 0 {
		return validity{}, errors.New("trailing bytes in validity SEQUENCE")
	}
	return v, nil
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
// It checks that mandatory fields are present, and that all fields are well formatted.
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

func (l *MTCLog) accept(tbs TBSCertificateLogEntry) error {
	if err := tbs.Validate(); err != nil {
		return err
	}
	v, err := parseValidity(tbs.Validity)
	if err != nil {
		return fmt.Errorf("cannot parse validity: %v", err)
	}
	if v.NotAfter.Before(v.NotBefore) {
		return fmt.Errorf("validity: notAfter %q cannot be before notBefore %q", v.NotAfter.Format(time.RFC3339), v.NotBefore.Format(time.RFC3339))
	}
	if lifetime := v.NotAfter.Sub(v.NotBefore); lifetime > l.maxCertLifetime {
		return fmt.Errorf("validity lifetime %q exceeds maximum allowed lifetime %q", lifetime, l.maxCertLifetime)
	}
	return nil
}

// NewMTCLog creates a new MTCLog compliant with
// draft-ietf-plants-merkle-tree-certs and http://c2sp.org/mtc-tlog.
func NewMTCLog(ctx context.Context, a *tessera.Appender, opts *Options) (*MTCLog, error) {
	if a == nil {
		return nil, errors.New("appender must not be nil")
	}
	if opts == nil {
		return nil, errors.New("options must not be nil")
	}
	if err := opts.valid(); err != nil {
		return nil, err
	}

	cpReader, err := checkpoint.NewReader(ctx, opts.reader.ReadCheckpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to read initial checkpoint: %v", err)
	}

	interval := opts.landmarkInterval
	if interval <= 0 {
		interval = RecommendedLandmarkInterval(opts.maxCertLifetime)
	} else if rec := RecommendedLandmarkInterval(opts.maxCertLifetime); interval != rec {
		slog.WarnContext(ctx, "configured landmark interval differs from CQRP recommendation",
			slog.Duration("configured", interval),
			slog.Duration("recommended", rec),
		)
	}

	pub, err := landmark.NewPublisher(ctx, cpReader.LatestSize, opts.landmarkStorage, opts.maxCertLifetime, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise landmark publisher: %v", err)
	}

	return &MTCLog{
		a:                 a,
		reader:            opts.reader,
		awaiter:           tessera.NewPublicationAwaiter(ctx, cpReader.Checkpoint, opts.pollPeriod),
		landmarkPublisher: pub,
		maxCertLifetime:   opts.maxCertLifetime,
	}, nil
}

// AddTBS adds a TBSCertificateLogEntry to the log.
func (l *MTCLog) AddTBS(ctx context.Context, tbs TBSCertificateLogEntry) (*AddTBSRsp, error) {
	if err := l.accept(tbs); err != nil {
		return nil, fmt.Errorf("invalid entry: %v", err)
	}

	tbsb, err := tbs.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal contents: %v", err)
	}

	e := entry.New(tbsb)
	eb, err := e.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal: %v", err)
	}

	future := l.a.Add(ctx, tessera.NewEntry(eb))

	idx, _, err := l.awaiter.Await(ctx, future)
	if err != nil {
		return nil, fmt.Errorf("error waiting for Tessera index future and its integration: %v", err)
	}

	// TODO: get subtree cosignatures
	// TODO: build MTCProof

	return &AddTBSRsp{
		Index:    idx.Index,
		MTCProof: nil,
	}, nil
}

// ProofToLandmark builds an MTCProof for the entry at index to a published landmark.
//   - If index precedes the earliest available active landmark, returns ErrTooOld.
//   - If index exceeds the current log tree size, returns ErrExceedsTreeSize.
//   - If index belongs to an unclosed/pending landmark, returns retryAfter indicating when
//     the client should retry (including a publication buffer and randomized jitter to avoid a thundering herd).
//   - If proof generation fails, returns an error.
func (l *MTCLog) ProofToLandmark(ctx context.Context, index uint64) ([]byte, time.Duration, error) {
	start, end, retryAfter, err := l.landmarkPublisher.GetSubtreeFor(ctx, index)
	switch {
	case errors.Is(err, landmark.ErrTooOld):
		return nil, 0, ErrTooOld
	case errors.Is(err, landmark.ErrExceedsTreeSize):
		return nil, 0, ErrExceedsTreeSize
	case err != nil:
		return nil, 0, fmt.Errorf("get subtree for index %d: %v", index, err)
	case retryAfter > 0:
		jitter := time.Duration(rand.Int64N(int64(maxRetryAfterJitter)))
		return nil, retryAfter + landmarkPublicationBuffer + jitter, nil
	}

	// Construct the inclusion proof to the active landmark.
	pb, err := client.NewProofBuilder(ctx, end, l.reader.ReadTile)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot create proof builder")
	}
	proofNodes, err := pb.SubtreeInclusionProof(ctx, index, start, end)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot get subtree inclusion proof for index %d in subtree [%d, %d): %v", index, start, end, err)
	}

	// Extract extensions from the log entry.
	// SPEC: draft-ietf-plants-merkle-tree-certs Section 6.2
	// "extensions MUST contain the log entry's extensions value (Section 5.2.1)."
	bundleIndex := index / layout.EntryBundleWidth
	entryOffset := index % layout.EntryBundleWidth
	eb, err := client.GetEntryBundle(ctx, l.reader.ReadEntryBundle, bundleIndex, end)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot read entry bundle for entry %d: %v", index, err)
	}
	if entryOffset >= uint64(len(eb.Entries)) {
		return nil, 0, fmt.Errorf("entry offset %d exceeds bundle size %d for entry %d", entryOffset, len(eb.Entries), index)
	}
	extBytes, err := entry.ExtractExtensions(eb.Entries[entryOffset])
	if err != nil {
		return nil, 0, fmt.Errorf("cannot read extensions for entry %d: %v", index, err)
	}

	// SPEC: draft-ietf-plants-merkle-tree-certs Section 6.4
	// "A landmark-relative certificate is a Merkle Tree certificate which contains no signatures"
	proof, err := mtcproof.Serialize(extBytes, start, end, proofNodes, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot construct MTCProof: %w", err)
	}

	return proof, 0, nil
}

// formatOriginAndSigner generates valid MTC origin and signerName.
//
// SPEC: draft-ietf-plants-merkle-tree-certs section 5.3.1.
// "cosigner_name and log_origin are computed from the cosigner ID and the
// issuance log's ID (Section 5.1), respectively. They contain the concatenation of:
//   - The 16-byte ASCII string oid/1.3.6.1.4.1.
//   - The trust anchor ID's ASCII representation (Section 3 of
//     [I-D.ietf-tls-trust-anchor-ids])"
//
// SPEC: draft-ietf-plants-merkle-tree-certs section 5.1.
// "For each positive integer N, the OID {caID logs(0) N} represents the
// issuance log N (Section 5.2)."
func formatOriginAndSigner(caID string, logNumber uint64) (origin, signerName string, err error) {
	if caID == "" {
		return "", "", errors.New("ca_id cannot be empty")
	}
	if logNumber == 0 {
		return "", "", errors.New("log_number must be strictly positive (> 0)")
	}
	signerName = oidPrefix + caID
	origin = fmt.Sprintf("%s.0.%d", signerName, logNumber)

	if _, err := x509.ParseOID(signerName[len("oid/"):]); err != nil {
		return "", "", fmt.Errorf("invalid ca_id %q: signer name is not a valid OID: %w", caID, err)
	}
	if _, err := x509.ParseOID(origin[len("oid/"):]); err != nil {
		return "", "", fmt.Errorf("invalid log origin %q is not a valid OID: %w", origin, err)
	}
	return origin, signerName, nil
}

// CreateSignerAndOrigin generates valid MTC origin and signer.
// Returns an error if the CA ID doesn't match with the private key's signer name.
func CreateSignerAndOrigin(caID string, logNumber uint64, privKey string) (origin string, signer note.SubtreeSigner, err error) {
	origin, expectedSignerName, err := formatOriginAndSigner(caID, logNumber)
	if err != nil {
		return "", nil, fmt.Errorf("invalid --ca_id or --log_number: %w", err)
	}
	s, err := note.NewMLDSASigner(privKey)
	if err != nil {
		return "", nil, fmt.Errorf("failed to instantiate ML-DSA signer: %w", err)
	}
	if s.Name() != expectedSignerName {
		return "", nil, fmt.Errorf("signer key name %q does not match expected CA ID name %q", s.Name(), expectedSignerName)
	}
	return origin, s, nil
}
