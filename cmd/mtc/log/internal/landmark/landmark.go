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

package landmark

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ActiveLandmarks represents the active landmarks published by a CA log.
// as described in draft-ietf-plants-merkle-tree-certs section 6.4.3.
//
// ActiveLandmarks is not safe for concurrent use without external synchronization.
type ActiveLandmarks struct {
	// The number of non-zero landmark tree sizes the log has ever published.
	lastLandmark       uint64
	numActiveLandmarks uint64
	// treeSizes contains numActiveLandmarks + 1 tree sizes in strictly decreasing order.
	// It must never be empty.
	// treeSizes[i] corresponds to landmark `lastLandmark - i`.
	treeSizes []uint64
}

// newActiveLandmarks creates a new ActiveLandmarks struct with the given parameters.
// Parameters must represent a valid landmarks file as per specs, otherwise
// returns an error. Specifically, treeSizes MUST have at least one entry.
func newActiveLandmarks(lastLandmark, numActive uint64, treeSizes []uint64) (*ActiveLandmarks, error) {
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.3.
	// "num_active_landmarks <= last_landmark"
	if numActive > lastLandmark {
		return nil, fmt.Errorf("num_active_landmarks (%d) cannot exceed last_landmark (%d)", numActive, lastLandmark)
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.3.
	// "num_active_landmarks + 1 lines"
	if uint64(len(treeSizes)) != numActive+1 { // Hence, len(treeSizes) >= 1
		return nil, fmt.Errorf("expected %d tree sizes, got %d", numActive+1, len(treeSizes))
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.1.
	// "Landmark zero MUST have a tree size of zero."
	if numActive == lastLandmark { // i.e treeSizes contains landmark zero
		if treeSizes[numActive] != 0 {
			return nil, fmt.Errorf("landmark 0 (treeSizes[%d]) must have tree size 0, got %d", numActive, treeSizes[numActive])
		}
	} else if treeSizes[numActive] == 0 {
		return nil, fmt.Errorf("landmark %d (treeSizes[%d]) cannot have tree size 0", lastLandmark-numActive, numActive)
	}
	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.1.
	// "The sequence of tree sizes MUST be append-only and strictly monotonically
	// increasing."
	for i := 1; i < len(treeSizes); i++ {
		if treeSizes[i-1] <= treeSizes[i] {
			return nil, fmt.Errorf("tree sizes must be strictly decreasing: treeSizes[%d]=%d <= treeSizes[%d]=%d", i-1, treeSizes[i-1], i, treeSizes[i])
		}
	}
	return &ActiveLandmarks{
		lastLandmark:       lastLandmark,
		numActiveLandmarks: numActive,
		treeSizes:          slices.Clone(treeSizes),
	}, nil
}

// UnmarshalText parses the text representation of active landmarks.
//
// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.3.
// "The format is the following sequence of lines. Each line MUST be terminated
// by a newline character (U+000A):
//
//   - Two space-separated non-negative decimal integers: `<last_landmark> <num_active_landmarks>`.
//     This line MUST satisfy the following, otherwise it is invalid:
//   - `num_active_landmarks <= max_active_landmarks`
//   - `num_active_landmarks <= last_landmark`
//   - `num_active_landmarks + 1` lines each containing a single non-negative
//     decimal integer, representing a tree size. Numbered from zero to `num_active_landmarks`,
//     in decreasing order.
//
// For example:
// 2 2\n
// 200\n
// 100\n
// 0\n"
func (a *ActiveLandmarks) UnmarshalText(text []byte) error {
	if len(text) == 0 || text[len(text)-1] != '\n' {
		return errors.New("landmarks text must not be empty and must end with a newline")
	}

	lines := strings.Split(strings.TrimSuffix(string(text), "\n"), "\n")
	if len(lines) < 2 {
		return fmt.Errorf("expected at least header line and one tree size line, got %d lines", len(lines))
	}

	headerLine, lines := lines[0], lines[1:]
	headerParts := strings.Split(headerLine, " ")
	if len(headerParts) != 2 {
		return fmt.Errorf("invalid header line format %q: expected two space-separated integers", headerLine)
	}

	lastLM, err := strconv.ParseUint(headerParts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid last_landmark %q: %w", headerParts[0], err)
	}

	numActive, err := strconv.ParseUint(headerParts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid num_active_landmarks %q: %w", headerParts[1], err)
	}

	treeSizes := make([]uint64, len(lines))
	for i, line := range lines {
		size, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid tree size on line %d (%q): %w", i+2, line, err)
		}
		treeSizes[i] = size
	}

	lm, err := newActiveLandmarks(lastLM, numActive, treeSizes)
	if err != nil {
		return err
	}
	*a = *lm
	return nil
}

// MarshalText returns the text representation of active landmarks.
// a.treeSizes must contain at least one tree size, otherwise returns an error.
func (a *ActiveLandmarks) MarshalText() ([]byte, error) {
	if len(a.treeSizes) == 0 {
		return nil, errors.New("cannot marshal uninitialized ActiveLandmarks instance, treeSizes must not be empty")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%d %d\n", a.lastLandmark, a.numActiveLandmarks)
	for _, size := range a.treeSizes {
		fmt.Fprintf(&b, "%d\n", size)
	}
	return []byte(b.String()), nil
}

// AddLandmark appends treeSize to landmarks and prunes active landmarks using maxActive.
// a.treeSizes must contain at least one tree size, otherwise returns an error.
func (a *ActiveLandmarks) AddLandmark(treeSize, maxActive uint64) error {
	if maxActive == 0 {
		return errors.New("maxActive must be strictly positive (> 0)")
	}
	if len(a.treeSizes) == 0 {
		return errors.New("cannot add landmark to uninitialized ActiveLandmarks instance, treeSizes must not be empty")
	}
	if treeSize <= a.treeSizes[0] {
		return fmt.Errorf("new landmark tree size %d must be strictly greater than current last landmark tree size %d", treeSize, a.treeSizes[0])
	}
	a.lastLandmark++
	a.treeSizes = slices.Insert(a.treeSizes, 0, treeSize)
	if uint64(len(a.treeSizes)) > maxActive+1 {
		a.treeSizes = a.treeSizes[:maxActive+1]
	}
	a.numActiveLandmarks = uint64(len(a.treeSizes) - 1)
	return nil
}
