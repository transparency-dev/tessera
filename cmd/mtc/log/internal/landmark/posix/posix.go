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
	"context"
	"errors"
	"os"
	"syscall"
)

// lockFile creates/opens a lock file at the specified path, and flocks it.
// Once locked, the caller performs necessary operations before calling the
// returned function to unlock it.
//
// Note that a) this is advisory, and b) should use an non-API specified file
// (e.g. <something>.lock>) to avoid inherent brittleness of the `fcntrl` API
// (*any* `Close` operation on this file (even if it's a different FD) from
// this PID, or overwriting of the file by *any* process breaks the lock.)
func lockFile(ctx context.Context, p string) (func() error, error) {
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
