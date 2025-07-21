// Copyright 2025 The Tessera authors. All Rights Reserved.
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

package testonly

import (
	"context"
	"testing"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/mod/sumdb/note"
)

// NewTestLog creates a temporary POSIX log instance in Appender mode with the provided options.
//
// This log will be rooted in a temporary directory which will be automatically removed by the
// testing package after use.
//
// Returns an instance of TestLog containing the various structures created, and a shutdown function
// which MUST be called when the test has finished with the log.
func NewTestLog(t *testing.T, opts *tessera.AppendOptions) (*TestLog, func(context.Context) error) {
	t.Helper()
	sk, vk, err := note.GenerateKey(nil, "test")
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	s, err := note.NewSigner(sk)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	v, err := note.NewVerifier(vk)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	root := t.TempDir()
	driver, err := posix.New(t.Context(), posix.Config{Path: root})
	if err != nil {
		t.Fatalf("posix.New: %v", err)
	}

	opts.WithCheckpointSigner(s)
	a, shutdown, lr, err := tessera.NewAppender(t.Context(), driver, opts)
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}

	r := &TestLog{
		Root:        root,
		SigVerifier: v,
		LogReader:   lr,
		Appender:    a,
	}

	return r, shutdown
}

// TestLog represents an ephemeral POSIX log instance intended for use in tests.
type TestLog struct {
	// Root is the path to the directory which contains the log data.
	Root string
	// SigVerifier can verify log signatures on its checkpoints.
	SigVerifier note.Verifier
	// LogReader reads from the log storage directly.
	LogReader tessera.LogReader
	// Appender provides access to the Appender lifecycle mode for this log.
	Appender *tessera.Appender
}
