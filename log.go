// Copyright 2024 The Tessera authors. All Rights Reserved.
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

package tessera

import (
	"errors"
	"fmt"
)

var (
	// ErrPushback is returned by underlying storage implementations when a new entry cannot be accepted
	// due to overload in the system. This could be because there are too many entries with indices assigned
	// but which have not yet been integrated into the tree, or it could be because the antispam mechanism
	// is not able to keep up with recently added entries. It should always be wrapped with a more
	// specific error to provide context to clients.
	//
	// Personalities encountering this error should apply back-pressure to the source of new entries
	// in an appropriate manner (e.g. for HTTP services, return a 503 with a Retry-After header).
	//
	// Personalities should check for this error (wrapped or not) using `errors.Is(e, ErrPushback)`.
	ErrPushback = errors.New("pushback")
	// ErrPushbackAntispam is a wrapped ErrPushback. It is returned by underlying storage implementations
	// when an entry cannot be accepted becasue the antispam follower has fallen too far behind the size
	// of the integrated tree.
	ErrPushbackAntispam = fmt.Errorf("antispam %w", ErrPushback)
	// ErrPushbackIntegration is a wrapped ErrPushback. It is returned by underlying storage implementations
	// when an entry cannot be accepted becasue there are too many "in-flight" add requests - i.e. entries
	// with sequence numbers assigned, but which are not yet integrated into the log.
	ErrPushbackIntegration = fmt.Errorf("integration %w", ErrPushback)
)

// Driver is the implementation-specific parts of Tessera. No methods are on here as this is not for public use.
type Driver any
