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

package mirror

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/transparency-dev/tessera"
)

const (
	testLogOrigin = "example.com/log"
)

func TestAddCheckpointUnknownLog(t *testing.T) {
	ctx := t.Context()
	m := New(nil)
	if _, _, err := m.AddCheckpoint(ctx, "unknown-log", 0, nil, nil); !errors.Is(err, ErrUnknownLog) {
		t.Errorf("AddCheckpoint() error = %v, want %v", err, ErrUnknownLog)
	}
}

func TestAddCheckpointHitsTarget(t *testing.T) {
	ctx := t.Context()
	dummy := &dummyTarget{}
	oldSize := uint64(10)
	proof := [][]byte{{1}}
	cpRaw := []byte("hello")
	m := New(map[string]Target{testLogOrigin: dummy})
	if _, _, err := m.AddCheckpoint(ctx, testLogOrigin, oldSize, proof, cpRaw); err != nil {
		t.Errorf("AddCheckpoint() error = %v, want %v", err, nil)
	}
	if !dummy.calledAddCheckpoint {
		t.Errorf("AddCheckpoint() was not called")
	}
	if dummy.addCheckpointOldSize != oldSize {
		t.Errorf("AddCheckpoint() was called with oldSize %d, want %d", dummy.addCheckpointOldSize, oldSize)
	}
	if !bytes.Equal(dummy.addCheckpointProof[0], []byte{1}) {
		t.Errorf("AddCheckpoint() was called with proof %v, want %v", dummy.addCheckpointProof, proof)
	}
	if !bytes.Equal(dummy.addCheckpointCpRaw, []byte("hello")) {
		t.Errorf("AddCheckpoint() was called with cpRaw %v, want %v", dummy.addCheckpointCpRaw, cpRaw)
	}
}

type dummyTarget struct {
	calledAddCheckpoint bool
	addCheckpointOldSize uint64
	addCheckpointProof [][]byte
	addCheckpointCpRaw   []byte
}

func (d *dummyTarget) AddCheckpoint(ctx context.Context, oldSize uint64, proof [][]byte, cpRaw []byte) (cosig []byte, wantSize uint64, err error) {
	d.addCheckpointCpRaw = cpRaw
	d.addCheckpointOldSize = oldSize
	d.addCheckpointProof = proof
	d.calledAddCheckpoint = true
	return nil, 0, nil
}

func (d *dummyTarget) AddEntries(ctx context.Context, uploadStart, uploadEnd uint64, ticket []byte, next func() (*tessera.MirrorPackage, error)) ([]byte, error) {
	return nil, nil
}

func (d *dummyTarget) IntegratedSize(ctx context.Context) (uint64, error) {
	return 0, nil
}