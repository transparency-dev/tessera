// Copyright 2025 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package posix

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"k8s.io/klog/v2"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
)

// syncDir opens the specified directory and calls op before syncing and closing the handle on the directory.
//
// This dance ensures that the inode of the specified directory cannot be evicted from the kernel inode cache while
// the operation is underway, and so any error which occurs while updating metadata about a file operation which happens
// _within_ that directory is detected.
//
// This function is intended to be used by the other functions in this file.
func syncDir(dir string, op func() error) (err error) {
	fd, err := os.OpenFile(dir, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("failed to open %q: %w", dir, err)
	}
	defer func() {
		e := fd.Close()
		if err == nil {
			err = e
		}
	}()

	if err := op(); err != nil {
		return err
	}

	if err := fd.Sync(); err != nil {
		return fmt.Errorf("failed to sync %q: %w", dir, err)
	}
	return nil
}

// mkdirAll is a reimplementation of os.mkdirAll but where we fsync the parent directory/ies
// we modify.
func mkdirAll(name string, perm os.FileMode) error {
	name = strings.TrimSuffix(name, string(filepath.Separator))
	if name == "" {
		return nil
	}

	// Finally, check and create the dir if necessary.
	dir := filepath.Dir(name)
	di, err := os.Lstat(name)
	switch {
	case errors.Is(err, syscall.ENOENT):
		// We'll see an ENOENT if there's a problem with a non-existant path element, so
		// we'll recurse and create the parent directory if necessary.
		// Don't return an error if someone else managed to get in and create the directory before us, though.
		if dir != "" {
			if err := mkdirAll(dir, perm); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
		}
		// Once we've successfully created the parent element(s), we can drop through and
		// create the final entry in the requested path.
		fallthrough
	case errors.Is(err, os.ErrNotExist):
		return syncDir(dir, func() error {
			// We'll see ErrNotExist if the final entry in the requested path doesn't exist,
			// so we simply attempt to create it in here.
			if err := os.Mkdir(name, perm); err != nil {
				return fmt.Errorf("%q: %v", name, err)
			}
			return nil
		})
	case err != nil:
		return fmt.Errorf("lstat %q: %v", name, err)
	case !di.IsDir():
		return fmt.Errorf("%s is not a directory", name)
	default:
		return nil
	}
}

// createEx atomically creates a file at the given path containing the provided data, and syncs the
// directory containing the newly created file.
//
// Returns an error if a file already exists at the specified location, or it's unable to fully write the
// data & close the file.
func createEx(name string, d []byte) error {
	dir := filepath.Dir(name)
	if err := mkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to make directory structure: %w", err)
	}
	return syncDir(dir, func() error {
		tmpName, err := createTemp(name, d)
		if err != nil {
			return fmt.Errorf("failed to create temp file: %v", err)
		}
		defer func() {
			if err := os.Remove(tmpName); err != nil {
				klog.Warningf("Failed to remove temporary file %q: %v", tmpName, err)
			}
		}()

		if err := os.Link(tmpName, name); err != nil {
			// Wrap the error here because we need to know if it's os.ErrExists at higher levels.
			return fmt.Errorf("failed to link temporary file to target %q: %w", name, err)
		}
		return nil
	})
}

// overwrite atomically creates/overwrites a file at the given path containing the provided data, and syncs
// the directory containing the overwritten/created file.
func overwrite(name string, d []byte) error {
	dir := filepath.Dir(name)
	if err := mkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("failed to make directory structure: %w", err)
	}
	return syncDir(dir, func() error {
		dir, _ := filepath.Split(name)
		if err := mkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("failed to make entries directory structure: %w", err)
		}

		tmpName, err := createTemp(name, d)
		if err != nil {
			return fmt.Errorf("failed to create temp file: %v", err)
		}

		if err := os.Rename(tmpName, name); err != nil {
			return fmt.Errorf("failed to rename temporary file to target %q: %w", name, err)
		}
		return nil
	})
}

// createTemp creates a new temporary file in the directory dir, with a name based on the provided prefix,
// and writes the provided data to it.
//
// Multiple programs or goroutines calling CreateTemp simultaneously will not choose the same file.
// It is the caller's responsibility to remove the file when it is no longer needed.
//
// Ths file data is written with O_SYNC, however the containing directory is NOT sync'd on the assumption
// that this temporary file will be linked/renamed by the caller who will also sync the directory.
func createTemp(prefix string, d []byte) (name string, err error) {
	try := 0
	var f *os.File

	for {
		name = prefix + strconv.Itoa(int(rand.Int32()))
		f, err = os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, filePerm)
		if err == nil {
			break
		} else if os.IsExist(err) {
			if try++; try < 10000 {
				continue
			}
			return "", &os.PathError{Op: "createtemp", Path: prefix + "*", Err: os.ErrExist}
		}
	}

	defer func() {
		if errC := f.Close(); errC != nil && err == nil {
			err = errC
		}
	}()

	if n, err := f.Write(d); err != nil {
		return "", fmt.Errorf("failed to write to temporary file %q: %v", name, err)
	} else if l := len(d); n < l {
		return "", fmt.Errorf("short write on %q, %d < %d", name, n, l)
	}

	return name, nil
}
