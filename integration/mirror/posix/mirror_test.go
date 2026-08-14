// Copyright 2026 The Tessera Authors. All Rights Reserved.
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

// Package mirror_test contains integration tests for the POSIX MTC mirror
// and the POSIX conformance log.
package mirror_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/transparency-dev/formats/log"
	fnote "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/cmd/mtc/mirror"
	"github.com/transparency-dev/tessera/fsck"
	iLog "github.com/transparency-dev/tessera/integration/log"
	"github.com/transparency-dev/tessera/storage/posix"
	witnessConfig "github.com/transparency-dev/witness/config"
	"github.com/transparency-dev/witness/persistence/sqlite"
	"github.com/transparency-dev/witness/witness"
	"golang.org/x/mod/sumdb/note"
)

func TestPosixMirrorIntegration(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := t.Context()

	const (
		logOrigin    = "example.com/log/posix_mirror_test"
		mirrorOrigin = "example.com/mirror/posix_mirror_test"
	)

	logSigner, logPubKey := mustGenerateMLDSACosigner(t, logOrigin)
	mirrorSigner, mirrorPubKey := mustGenerateMLDSACosigner(t, mirrorOrigin)

	// Storage directories.
	logStorageDir := filepath.Join(tmpDir, "log_storage")
	if err := os.MkdirAll(logStorageDir, 0o755); err != nil {
		t.Fatalf("Failed to create log storage dir: %v", err)
	}
	mirrorStorageDir := filepath.Join(tmpDir, "mirror_storage")
	if err := os.MkdirAll(mirrorStorageDir, 0o755); err != nil {
		t.Fatalf("Failed to create mirror storage dir: %v", err)
	}
	mirrorLogDir := filepath.Join(mirrorStorageDir, "public/mirrors", fmt.Sprintf("%0x", sha256.Sum256([]byte(logOrigin))))
	if err := os.MkdirAll(mirrorLogDir, 0o755); err != nil {
		t.Fatalf("Failed to create mirror target dir: %v", err)
	}
	witnessDir := filepath.Join(mirrorStorageDir, "private/witness")
	if err := os.MkdirAll(witnessDir, 0o700); err != nil {
		t.Fatalf("Failed to create witness dir: %v", err)
	}

	// Create a signer-less witness to be used with the mirror service.
	w, wp := mustCreateWitness(t, ctx, witnessDir)
	// Provision test log onto witness
	if err := wp.AddLogs(ctx, []witnessConfig.Log{{Origin: logOrigin, VKey: logPubKey}}); err != nil {
		t.Fatalf("witness AddLogs: %v", err)
	}

	// Create the mirror service...
	m, err := mirror.New(ctx, w, mirrorSigner, []mirror.LogConfig{
		{
			Log: witnessConfig.Log{
				Origin:   logOrigin,
				VKey:     logPubKey,
				Verifier: logSigner.Verifier(),
			},
			Driver: mustCreateDriver(t, mirrorLogDir),
		},
	})
	if err != nil {
		t.Fatalf("mirror.New: %v", err)
	}

	mirrorServer := httptest.NewServer(m)
	t.Cleanup(mirrorServer.Close)

	// 3. Create mirror policy for the log pointing to the mirror server.
	mirrorPolicyStr := fmt.Sprintf(`
		witness mirror1 %s %s
		group g1 all mirror1
		quorum g1
		`,
		mirrorPubKey, mirrorServer.URL)

	mirrorPolicy, err := tessera.NewWitnessGroupFromPolicy([]byte(mirrorPolicyStr))
	if err != nil {
		t.Fatalf("NewWitnessGroupFromPolicy: %v", err)
	}

	// 4. Programmatically instantiate the POSIX conformance log.
	logDriver, err := posix.New(ctx, posix.Config{Path: logStorageDir})
	if err != nil {
		t.Fatalf("posix.New(log): %v", err)
	}

	logOpts := tessera.NewAppendOptions().
		WithCheckpointSigner(logSigner).
		WithCheckpointInterval(time.Second).
		WithCheckpointRepublishInterval(time.Minute).
		WithBatching(256, time.Second).
		WithAntispam(tessera.DefaultAntispamInMemorySize, nil).
		WithMirrors(mirrorPolicy, nil)

	appender, shutdownAppender, lr, err := tessera.NewAppender(ctx, logDriver, logOpts)
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	t.Cleanup(func() {
		_ = shutdownAppender(context.Background())
	})

	logMux := http.NewServeMux()
	logMux.HandleFunc("POST /add", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		idx, err := appender.Add(r.Context(), tessera.NewEntry(b))()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		_, _ = fmt.Fprintf(w, "%d", idx.Index)
	})

	conformanceServer := httptest.NewServer(logMux)
	t.Cleanup(conformanceServer.Close)

	// 5. Use the LiveIntegrationTest in integration_test.go with write_log_url set to the log and log_url set to the mirror.
	iLog.LogIntegrationTest(t, iLog.LogIntegrationTestConfig{
		LogReadTile:        lr.ReadTile,
		LogReadCP:          lr.ReadCheckpoint,
		LogReadEntryBundle: lr.ReadEntryBundle,
		WriteLogURL:        conformanceServer.URL,
		LogVerifier:        logSigner.Verifier(),
		EntrySize:          uint64(1024),
	})

	// 6. Run fsck on both the log and mirror, and assert that the checkpoint size and root hashes of both match.
	logFsck := fsck.New(logOrigin, logSigner.Verifier(), client.FileFetcher{Root: logStorageDir}, defaultMerkleLeafHasher, fsck.Opts{N: 1})
	if err := logFsck.Check(ctx); err != nil {
		t.Fatalf("fsck on log failed: %v", err)
	}

	mirrorFsck := fsck.New(logOrigin, logSigner.Verifier(), client.FileFetcher{Root: mirrorLogDir}, defaultMerkleLeafHasher, fsck.Opts{N: 1})
	if err := mirrorFsck.Check(ctx); err != nil {
		t.Fatalf("fsck on mirror failed: %v", err)
	}

	logCP, _, _, err := log.ParseCheckpoint(logFsck.Checkpoint(), logOrigin, logSigner.Verifier())
	if err != nil {
		t.Fatalf("Failed to parse log checkpoint: %v", err)
	}

	mirrorCP, _, _, err := log.ParseCheckpoint(mirrorFsck.Checkpoint(), logOrigin, logSigner.Verifier())
	if err != nil {
		t.Fatalf("Failed to parse mirror checkpoint: %v", err)
	}

	if logCP.Size != mirrorCP.Size {
		t.Errorf("Checkpoint size mismatch: log size=%d, mirror size=%d", logCP.Size, mirrorCP.Size)
	}
	if !bytes.Equal(logCP.Hash, mirrorCP.Hash) {
		t.Errorf("Checkpoint root hash mismatch: log hash=%x, mirror hash=%x", logCP.Hash, mirrorCP.Hash)
	}
	t.Logf("Successfully verified log and mirror matched: size=%d root=%x", logCP.Size, logCP.Hash)
}

func defaultMerkleLeafHasher(bundle []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bundle); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	r := make([][]byte, 0, len(eb.Entries))
	for _, e := range eb.Entries {
		h := rfc6962.DefaultHasher.HashLeaf(e)
		r = append(r, h[:])
	}
	return r, nil
}

func mustGenerateMLDSACosigner(t *testing.T, name string) (fnote.SubtreeSigner, string) {
	t.Helper()

	skey, vkey, err := fnote.GenerateMLDSAKey(name)
	if err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}
	signer, err := fnote.NewMLDSASigner(skey)
	if err != nil {
		t.Fatalf("Failed to generate signer: %v", err)
	}
	return signer, vkey
}

func mustCreateDriver(t *testing.T, path string) tessera.Driver {
	t.Helper()
	d, err := posix.New(t.Context(), posix.Config{Path: path})
	if err != nil {
		t.Fatalf("posix.New(mirror): %v", err)
	}
	return d
}

func mustCreateWitness(t *testing.T, ctx context.Context, dir string) (*witness.Witness, *sqlite.Persistence) {
	t.Helper()
	witnessPersistence, shutdownWitness, err := sqlite.New(ctx, sqlite.Opts{
		Path: filepath.Join(dir, "witness.db"),
	})
	if err != nil {
		t.Fatalf("Failed to create witness persistence: %v", err)
	}
	t.Cleanup(func() {
		_ = shutdownWitness()
	})

	w, err := witness.New(ctx, witness.Opts{
		Persistence: witnessPersistence,
		Signers:     []note.Signer{},
		VerifierForLog: func(ctx context.Context, origin string) (note.Verifier, bool, error) {
			log, ok, err := witnessPersistence.Log(ctx, origin)
			if err != nil || !ok {
				return nil, false, err
			}
			return log.Verifier, true, nil
		},
	})
	if err != nil {
		t.Fatalf("Failed to create witness: %v", err)
	}
	return w, witnessPersistence
}
