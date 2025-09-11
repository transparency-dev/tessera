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

package fsck

import (
	"container/list"
	"fmt"
	"strings"
	"sync"

	"github.com/transparency-dev/tessera/api/layout"
)

const (
	// Unknown represents the state of resources we as yet know nothing about.
	Unknown State = iota
	// Fetching is the state of a resource being retrieved from the target log.
	Fetching
	// FetchError is the state of a failed fetch.
	FetchError
	// Fetched represents a resource which has been fetched, but not yet processed.
	Fetched
	// Calculating represents a resource being used to calculate hashes.
	Calculating
	// OK represents a resource which was successfully verified.
	OK
	// Invalid represents a resource which was determined to be incorrect or invalid somehow.
	Invalid
)

// State represents the state of a given static resource.
type State uint8

// String returns a string representation of the state.
func (s State) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Fetching:
		return "Fetching"
	case FetchError:
		return "FetchError"
	case Fetched:
		return "Fetched"
	case Calculating:
		return "Calculating"
	case OK:
		return "OK"
	case Invalid:
		return "Invalid"
	}
	panic(fmt.Errorf("unknown state %d", s))
}

// rangeTracker is a struct which knows how to maintain the state of all resources in a log.
//
// Resources in a given "level" (i.e. tile level or entry bundle) are represented as ranges with a given state.
// Resources covered by a given range must, by definition, all share the same state.
// Initially there is a single range at each level with covers the entire set of resources at that level, e.g. all entry bundles.
// When individual resources in the range have their state updated, the range which contains that resource is split (into either
// two or three ranges) such that we can continue to represent all elements while honouring the one-state-per-range invariant.
// In some situations, updates to resources will mean that there are two adjacent ranges which have the same state, and any such
// ranges are coalescent into a single larger range which covers the union.
//
// In this way, we can represent the state of all resources in the log in an efficient scheme.
// Due to the way the fsck tool works, the number of ranges is expected to be relatively small:
//   - the entire tree starts in state Unknown
//   - As the checking progresses, the left hand side of the tree will mostly be in the OK state,
//     and the right hand side will mostly be Unknown, with some range fragmentation in the middle (Fetching, Calculating, etc.)
//   - Eventually, all the ranges will (hopefully) return to being OK.
type rangeTracker struct {
	// mu guards access to the fields below.
	mu *sync.Mutex
	// tiles holds information about the tiles in each level of the log, with tiles[0] being the
	// lowest level tiles, just above the entries.
	// The elements in this list are all *Range structs.
	tiles []*list.List
	// entries holds information about the entrybundles in the log. The list elements are all
	// *Range structs.
	entries *list.List
}

// Range describes the common state of a range of bundles/tiles.
// The range covers [First, First+N) in tile-space, and all resources in the range share the same State.
type Range struct {
	// First is the index of the first resource covered by this range.
	First uint64
	// N is the number of resources covered by this range.
	N uint64
	// State is the state all resources covered by this range have.
	State State
}

// String returns a simple human readable representation of the range.
func (r *Range) String() string {
	return fmt.Sprintf("[%d, %d):%s", r.First, r.First+r.N, r.State)
}

// containts returns true iff the provided index is covered by this range.
func (r *Range) contains(idx uint64) bool {
	return idx >= r.First && idx < r.First+r.N
}

// newRangeTracker constructs a new tracker for a log of the given size.
func newRangeTracker(logSize uint64) *rangeTracker {
	el := list.New()
	t := []*list.List{}
	for level, levelSize := 0, logSize; levelSize > 0; level, levelSize = level+1, levelSize>>layout.TileHeight {
		n := levelSize >> layout.TileHeight
		if levelSize%layout.TileWidth > 0 {
			n++
		}
		if level == 0 {
			el.PushBack(&Range{
				First: 0,
				N:     n,
				State: Unknown,
			})
		}
		l := list.New()
		l.PushBack(&Range{
			First: 0,
			N:     n,
			State: Unknown,
		})
		t = append(t, l)
	}

	return &rangeTracker{
		mu:      &sync.Mutex{},
		entries: el,
		tiles:   t,
	}
}

// maybeMergeWithPrev merges the provided Range element with the Range element before it
// in the list into a single larger range, iff they share the same state.
func maybeMergeWithPrev(e *list.Element, l *list.List) {
	if e == nil {
		return
	}
	ev := e.Value.(*Range)
	if p := e.Prev(); p != nil {
		pv := p.Value.(*Range)
		if pv.State == ev.State {
			ev.N += pv.N
			ev.First = pv.First
			l.Remove(p)
		}
	}
}

// maybeMergeWithNext merges the provided Range element with the Range element after it
// in the list into a single larger range, iff they share the same state.
func maybeMergeWithNext(e *list.Element, l *list.List) {
	if e == nil {
		return
	}
	ev := e.Value.(*Range)
	if n := e.Next(); n != nil {
		nv := n.Value.(*Range)
		if nv.State == ev.State {
			ev.N += nv.N
			l.Remove(n)
		}
	}
}

// Update updates the range state representation with the new state for the static resource at the given index.
// level is the tile level if it's >= 0, or an entry bundle if it's == -1.
func (r *rangeTracker) Update(l int, idx uint64, state State) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var list *list.List
	if l == -1 {
		list = r.entries
	} else if l < len(r.tiles) {
		list = r.tiles[l]
	} else {
		panic(fmt.Errorf("no such tile level %d", l))
	}

	for p := list.Front(); p != nil; p = p.Next() {
		v := p.Value.(*Range)
		if !v.contains(idx) {
			continue
		}
		if v.State == state {
			return
		}
		r := &Range{
			First: idx,
			N:     1,
			State: state,
		}

		switch {
		case v.N == 1:
			// If this range has 1 element, then just change the state of the range.
			// This may mean we can coalesce this range with the previous and/or next range.
			v.State = state
			maybeMergeWithPrev(p, list)
			maybeMergeWithNext(p, list)
		case v.First == idx:
			// If we're changing the state of the first element in a range, then cleave that off into its
			// own range and insert it into the list before this one.
			e := list.InsertBefore(r, p)
			v.First++
			v.N--
			maybeMergeWithPrev(e, list)
		case v.First+v.N-1 == idx:
			// If we're changing the state of the last element in a range, then cleave that last element off
			// into its own range and add it after this one.
			e := list.InsertAfter(r, p)
			v.N--
			maybeMergeWithNext(e, list)
		default:
			// We're changing an element somewhere in the middle of the range, so we want to split this range
			// up into 3 parts:
			//   - a prefix range
			//   - a range consisting of one element whose state we're changing
			//   - a suffix range
			suffix := &Range{
				First: r.First + 1,
				N:     v.N - (idx - v.First) - 1,
				State: v.State,
			}
			// Turn the current range into the prefix range:
			v.N = idx - v.First
			// Add the single element range:
			e := list.InsertAfter(r, p)
			// Add the suffix range:
			list.InsertAfter(suffix, e)
		}
		return
	}
	dump := r.dumpRanges()
	panic(fmt.Errorf("walked off end of range with idx %d. List:\n%s", idx, strings.Join(dump, "\n")))
}

// Ranges returns a snapshot of the current status of resources in the log.
//
// Returns:
//   - a []Range which contains one or more contiguous Range structs which cover all the entry bundles
//   - a slice of []Range which perform the same function for the internal Merkle tree levels, with the
//     zeroth entry being the bottom-most level of the tree, just above the entry bundles.
func (r *rangeTracker) Ranges() ([]Range, [][]Range) {
	r.mu.Lock()
	defer r.mu.Unlock()

	eRange := make([]Range, 0, r.entries.Len())
	for p := r.entries.Front(); p != nil; p = p.Next() {
		v := p.Value.(*Range)
		eRange = append(eRange, *v)
	}

	tRanges := make([][]Range, 0, len(r.tiles))
	for _, t := range r.tiles {
		tr := make([]Range, 0, t.Len())
		for p := t.Front(); p != nil; p = p.Next() {
			v := p.Value.(*Range)
			tr = append(tr, *v)
		}
		tRanges = append(tRanges, tr)
	}
	return eRange, tRanges
}

// dumpRanges returns a string suitable for logging/printing to std out to help debug the internal
// state of the tracked ranges.
func (r *rangeTracker) dumpRanges() []string {
	d := []string{}
	for p := r.entries.Front(); p != nil; p = p.Next() {
		v := p.Value.(*Range)
		d = append(d, fmt.Sprintf("([%d:%d): %v", v.First, v.First+v.N, v.State))
	}
	return d

}
