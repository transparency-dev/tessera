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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/transparency-dev/merkle/proof"
)

const (
	// LandmarksPath is the storage path for the published active landmarks resource.
	LandmarksPath = "landmarks"

	// publishRetryOnFailure configures when to schedule a new publication upon failure.
	publishRetryOnFailure = time.Second * 5

	// MaxActiveLandmarks is the maximum allowed number of active landmarks over
	// any certificate validity period.
	//
	// SPEC: CQRP Policy v0.2.0
	// "MTC CA Operators MUST NOT issue Subscriber certificates with a validity
	// period exceeding 47 days."
	//
	// SPEC: CQRP Policy v0.2.0
	// "For CA Cosigners with a maximum permitted certificate validity of up to 47
	// days, MTC CA landmarks SHOULD be generated approximately every four 4 hours,
	// and MUST NOT exceed a total of 370 landmarks over any 47-day period."
	MaxActiveLandmarks = 370
)

var (
	// ErrTooOld indicates that an index precedes the earliest available active landmark.
	ErrTooOld = errors.New("entry is older than earliest active landmark")

	// ErrNotYetCovered indicates that an index is not yet covered by any published active landmark.
	ErrNotYetCovered = errors.New("entry is not yet covered by active landmarks")
)

// ReadCheckpointSize returns the current log checkpoint size.
type ReadCheckpointSize func() uint64

// LandmarksStorage abstracts reading and writing the published active landmarks resource.
type LandmarksStorage interface {
	// ReadLandmarks returns the raw contents of the active landmarks resource and its last modification time.
	// Returns os.ErrNotExist if no landmark resource exists yet.
	ReadLandmarks(ctx context.Context) (data []byte, modTime time.Time, err error)

	// UpdateLandmarks passes the current stored raw landmarks and its modification time to fn under an advisory lock.
	// If fn returns new data, it is written to storage and the new modification time is returned.
	UpdateLandmarks(ctx context.Context, fn func(old []byte, oldModTime time.Time) (new []byte, err error)) (modTime time.Time, err error)
}

// ActiveLandmarks represents the active landmarks published by a CA log
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

// latestTreeSize returns the tree size of the most recently published landmark.
//
// It assumes that ActiveLandmarks is well-constructed, i.e. treeSizes contains
// at least one entry, and it is ordered in decreasing order.
func (a *ActiveLandmarks) latestTreeSize() uint64 {
	return a.treeSizes[0]
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

// GetSubtreeFor returns the subtree range [start, end) of the active landmarks covering index.
//
// It returns ErrTooOld if index precedes the earliest available active landmark.
// It returns ErrNotYetCovered if index is not yet covered by any published active landmark.
//
// It assumes that ActiveLandmarks is well-constructed, i.e. treeSizes contains
// at least one entry, and it is ordered in decreasing order.
func (a *ActiveLandmarks) GetSubtreeFor(index uint64) (start, end uint64, err error) {
	switch {
	case index >= a.treeSizes[0]:
		return 0, 0, ErrNotYetCovered
	case index < a.treeSizes[len(a.treeSizes)-1]:
		return 0, 0, ErrTooOld
	}

	// Search forward from the most recent landmark (index 0) so recently issued
	// entries are found immediately in the first iteration.
	var startLM, endLM uint64
	for i := 0; i < len(a.treeSizes)-1; i++ {
		if index >= a.treeSizes[i+1] {
			endLM = a.treeSizes[i]
			startLM = a.treeSizes[i+1]
			break
		}
	}

	s, mid, e, err := proof.FindSubtrees(startLM, endLM)
	if err != nil {
		return 0, 0, fmt.Errorf("FindSubtrees(%d, %d): %v", startLM, endLM, err)
	}

	if index < mid {
		return s, mid, nil
	}
	return mid, e, nil
}

// Publisher manages publication of the landmarks resource at regular intervals.
type Publisher struct {
	storage            LandmarksStorage
	readCheckpointSize ReadCheckpointSize
	maxActive          uint64
	pubInterval        time.Duration

	// mu protects active and pubAt during concurrent operations
	mu     sync.RWMutex
	active ActiveLandmarks // copy of published active landmarks
	pubAt  time.Time       // time at which active landmarks were last published
}

// NewPublisher creates a new Publisher instance.
func NewPublisher(ctx context.Context, readCheckpointSize ReadCheckpointSize, storage LandmarksStorage, maxCertLifetime, pubInterval time.Duration) (*Publisher, error) {
	if storage == nil {
		return nil, errors.New("storage must not be nil")
	}
	if readCheckpointSize == nil {
		return nil, errors.New("readCheckpointSize must not be nil")
	}
	if maxCertLifetime <= 0 {
		return nil, errors.New("maxCertLifetime must be strictly positive")
	}
	if pubInterval <= 0 {
		return nil, errors.New("pubInterval must be strictly positive")
	}
	if pubInterval > maxCertLifetime {
		return nil, fmt.Errorf("pubInterval (%v) must not exceed maxCertLifetime (%v)", pubInterval, maxCertLifetime)
	}

	// SPEC: draft-ietf-plants-merkle-tree-certs section 6.4.3.
	// "To ensure that only active landmarks contain unexpired certificates,
	// max_active_landmarks is set to ceil(max_cert_lifetime / time_between_landmarks) + 1,
	// where max_cert_lifetime is the CA's maximum certificate lifetime.
	// The + 1 accounts for landmarks not allocated at the exact start of their time interval,
	// which can push certificate expiry one interval further than
	// ceil(max_cert_lifetime / time_between_landmarks) alone would bound."
	maxActive := uint64(math.Ceil(float64(maxCertLifetime)/float64(pubInterval))) + 1
	if maxActive > MaxActiveLandmarks {
		return nil, fmt.Errorf("max active landmarks (%d) exceeds limit (%d); increase pubInterval or decrease maxCertLifetime", maxActive, MaxActiveLandmarks)
	}

	p := &Publisher{
		storage:            storage,
		readCheckpointSize: readCheckpointSize,
		maxActive:          maxActive,
		pubInterval:        pubInterval,
	}

	if err := p.initialise(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize landmark publisher: %w", err)
	}

	go p.start(ctx)

	return p, nil
}

// initialise loads the existing active landmarks resource from storage, or initialises it with landmark zero.
func (p *Publisher) initialise(ctx context.Context) error {
	activeLM, err := newActiveLandmarks(0, 0, []uint64{0})
	if err != nil {
		return fmt.Errorf("failed to create initial landmark 0: %w", err)
	}
	modTime, err := p.storage.UpdateLandmarks(ctx, func(old []byte, _ time.Time) ([]byte, error) {
		if len(old) == 0 {
			return activeLM.MarshalText()
		}

		if err := activeLM.UnmarshalText(old); err != nil {
			return nil, fmt.Errorf("failed to unmarshal active landmarks: %w", err)
		}
		return old, nil
	})
	if err != nil {
		return fmt.Errorf("failed to initialise active landmarks: %w", err)
	}

	p.mu.Lock()
	p.active = *activeLM
	p.pubAt = modTime
	p.mu.Unlock()
	return nil
}

// start runs the background landmark publishing loop until ctx is canceled.
func (p *Publisher) start(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
			nextIn, err := p.Update(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "landmarks update: failed", slog.Any("error", err), slog.Duration("next-in", publishRetryOnFailure))
				timer.Reset(publishRetryOnFailure)
				continue
			}
			timer.Reset(nextIn)
		}
	}
}

// Update updates the landmarks resource when needed.
//
// It checks the current landmark publication and log checkpoint size,
// and publishes a new landmark if the previous landmark is older than
// pubInterval and the checkpoint size has increased.
// It returns how long to wait before calling Update again.
func (p *Publisher) Update(ctx context.Context) (time.Duration, error) {
	cpSize := p.readCheckpointSize()

	grown := true
	active := &ActiveLandmarks{}
	modTime, err := p.storage.UpdateLandmarks(ctx, func(old []byte, oldModTime time.Time) ([]byte, error) {
		if len(old) == 0 {
			return nil, errors.New("landmarks resource is empty or missing")
		}
		if err := active.UnmarshalText(old); err != nil {
			return nil, fmt.Errorf("failed to unmarshal landmarks for update: %w", err)
		}

		if time.Since(oldModTime) < p.pubInterval {
			slog.DebugContext(ctx, "landmarks update: skipping landmarks write because last update too recent", slog.Time("lastUpdate", oldModTime))
			return old, nil
		}
		if cpSize < active.latestTreeSize() {
			return nil, fmt.Errorf("checkpoint size (%d) smaller than last landmark size (%d)", cpSize, active.latestTreeSize())
		}
		if cpSize == active.latestTreeSize() {
			grown = false
			slog.DebugContext(ctx, "landmarks update: skipping landmarks write because tree has not grown", slog.Uint64("cpSize", cpSize))
			return old, nil
		}

		slog.DebugContext(ctx, "landmarks update: adding new landmark", slog.Uint64("cpSize", cpSize))
		if err := active.AddLandmark(cpSize, p.maxActive); err != nil {
			return nil, err
		}

		return active.MarshalText()
	})
	if err != nil {
		return 0, fmt.Errorf("failed to update landmarks resource: %v", err)
	}

	p.mu.Lock()
	p.active = *active
	p.pubAt = modTime
	p.mu.Unlock()

	next := p.pubInterval
	if grown {
		next = max(time.Millisecond, time.Until(modTime.Add(p.pubInterval)))
	}

	slog.DebugContext(ctx, "landmarks update: success", slog.Duration("next-in", next))
	return next, nil
}

// GetSubtreeFor returns the subtree range [start, end) of active landmarks covering index.
//
//   - If index is not yet in published landmarks but is within the current log
//     tree size, returns retryAfter > 0 indicating estimated time until the
//     next landmark publication. This is a best effort estimate.
//   - If index precedes the earliest available active landmark, returns ErrTooOld.
//   - If index exceeds the current log tree size, it returns an error.
func (p *Publisher) GetSubtreeFor(ctx context.Context, index uint64) (start, end uint64, retryAfter time.Duration, err error) {
	p.mu.RLock()
	active := p.active
	pubAt := p.pubAt
	p.mu.RUnlock()
	if len(active.treeSizes) == 0 {
		return 0, 0, p.pubInterval, nil
	}

	start, end, err = active.GetSubtreeFor(index)
	switch {
	case err == nil:
		return start, end, 0, nil

	case errors.Is(err, ErrTooOld):
		return 0, 0, 0, ErrTooOld

	case errors.Is(err, ErrNotYetCovered):
		cpSize := p.readCheckpointSize()
		if index < cpSize {
			retry := max(time.Millisecond, time.Until(pubAt.Add(p.pubInterval)))
			return 0, 0, retry, nil
		}
		return 0, 0, 0, fmt.Errorf("index %d exceeds current log tree size %d", index, cpSize)

	default:
		return 0, 0, 0, err
	}
}
