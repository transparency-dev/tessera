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

package mtc

import (
	"context"

	"github.com/transparency-dev/tessera"
)

type MTCLog struct {
	a *tessera.Appender
}

type MTCProof struct{}

type TBSCertificateLogEntry struct{}

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
