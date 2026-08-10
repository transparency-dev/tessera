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

package posix

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/transparency-dev/tessera/cmd/mtc/log/internal/landmark"
)

const (
	// lock must be held when checking/updating the published landmarks file.
	lock = "." + landmark.LandmarksPath + ".lock"
)

// Storage implements landmark.LandmarkStorage for a POSIX filesystem.
type Storage struct {
	path     string
	lockPath string
}

// NewStorage creates a new POSIX LandmarkStorage for storageDir.
func NewStorage(storageDir string) *Storage {
	return &Storage{
		path:     filepath.Join(storageDir, landmark.LandmarksPath),
		lockPath: filepath.Join(storageDir, lock),
	}
}

// ReadLandmarks returns the raw contents of the active landmarks file and its last modification time.
func (s *Storage) ReadLandmarks(ctx context.Context) ([]byte, time.Time, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer func() {
		_ = f.Close()
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, time.Time{}, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, time.Time{}, err
	}
	return data, info.ModTime(), nil
}

// UpdateLandmarks executes fn and writes any updated landmarks data.
// Runs under an advisory file lock to ensure distinct tasks are serialized.
func (s *Storage) UpdateLandmarks(ctx context.Context, fn func(old []byte, oldModTime time.Time) ([]byte, error)) (time.Time, error) {
	unlock, err := lockFile(ctx, s.lockPath)
	if err != nil {
		return time.Time{}, fmt.Errorf("lockFile(%s): %w", s.lockPath, err)
	}
	defer func() {
		if err := unlock(); err != nil {
			slog.WarnContext(ctx, "unlock failed", slog.String("lockpath", s.lockPath), slog.Any("error", err))
		}
	}()

	oldData, oldModTime, err := s.ReadLandmarks(ctx)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return time.Time{}, fmt.Errorf("read landmarks for update: %w", err)
	}

	newData, err := fn(oldData, oldModTime)
	if err != nil {
		return time.Time{}, err
	}

	if bytes.Equal(oldData, newData) && len(oldData) > 0 {
		slog.DebugContext(ctx, "skipping landmarks write because contents are unchanged", slog.String("path", s.path))
		return oldModTime, nil
	}

	if err := overwrite(s.path, newData); err != nil {
		return time.Time{}, err
	}
	slog.DebugContext(ctx, "wrote landmarks file", slog.String("path", s.path))
	info, err := os.Stat(s.path)
	// In case the write succeeded but we then fail to read mod time, use the
	// current time as a proxy.
	if err != nil {
		return time.Now(), nil
	}
	return info.ModTime(), nil
}

// lockFile creates/opens a lock file at the specified path, and flocks it.
// Once locked, the caller performs necessary operations before calling the
// returned function to unlock it.
//
// Note that a) this is advisory, and b) should use an non-API specified file
// (e.g. <something>.lock>) to avoid inherent brittleness of the `fcntrl` API
// (*any* `Close` operation on this file (even if it's a different FD) from
// this PID, or overwriting of the file by *any* process breaks the lock.)
func lockFile(_ context.Context, p string) (func() error, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, err
	}

	for {
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != syscall.EINTR {
			if err != nil {
				errClose := f.Close()
				return nil, errors.Join(err, errClose)
			}
			c := func() error {
				errFlock := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				errClose := f.Close()
				return errors.Join(errFlock, errClose)
			}
			return c, nil
		}
	}
}
