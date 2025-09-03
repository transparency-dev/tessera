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

package tessera

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"maps"

	"golang.org/x/mod/sumdb/note"
)

// policyComponent describes a component that makes up a policy. This is either a
// single Witness, or a WitnessGroup.
type policyComponent interface {
	// Satisfied returns true if the checkpoint is signed by the quorum of
	// witnesses involved in this policy component.
	Satisfied(cp []byte) bool

	// Endpoints returns the details required for updating a witness and checking the
	// response. The returned result is a map from the URL that should be used to update
	// the witness with a new checkpoint, to the value which is the verifier to check
	// the response is well formed.
	Endpoints() map[string]note.Verifier
}

// NewWitnessGroupFromPolicy creates a graph of witness objects which represnts the
// policy provided via the reader.
//
// The policy is expected to be formatted as per the description in
// https://git.glasklar.is/sigsum/core/sigsum-go/-/blob/main/doc/policy.md
func NewWitnessGroupFromPolicy(r io.Reader) (WitnessGroup, error) {
	scanner := bufio.NewScanner(r)
	var components []policyComponent
	var policy *WitnessGroup

	for scanner.Scan() {
		line := scanner.Text()
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		switch fields[0] {
		case "witness":
			if len(fields) != 3 {
				return WitnessGroup{}, fmt.Errorf("invalid witness definition: %q", line)
			}
			vkey := fields[1]
			witnessURLStr := fields[2]
			witnessURL, err := url.Parse(witnessURLStr)
			if err != nil {
				return WitnessGroup{}, fmt.Errorf("invalid witness URL %q: %w", witnessURLStr, err)
			}
			w, err := NewWitness(vkey, witnessURL)
			if err != nil {
				return WitnessGroup{}, fmt.Errorf("invalid witness key %q: %w", vkey, err)
			}
			components = append(components, w)

		case "group":
			if len(fields) < 2 {
				return WitnessGroup{}, fmt.Errorf("invalid group definition: %q", line)
			}
			n, err := strconv.Atoi(fields[1])
			if err != nil {
				return WitnessGroup{}, fmt.Errorf("invalid threshold N for group %q: %w", fields[1], err)
			}

			childrenIndices := fields[2:]
			children := make([]policyComponent, len(childrenIndices))
			for i, childIndexStr := range childrenIndices {
				childIndex, err := strconv.Atoi(childIndexStr)
				if err != nil {
					return WitnessGroup{}, fmt.Errorf("invalid component index %q in group definition", childIndexStr)
				}
				if childIndex < 0 || childIndex >= len(components) {
					return WitnessGroup{}, fmt.Errorf("component index %d out of range", childIndex)
				}
				children[i] = components[childIndex]
			}

			wg := NewWitnessGroup(n, children...)
			components = append(components, wg)
			p := wg
			policy = &p
		default:
			return WitnessGroup{}, fmt.Errorf("unknown definition type: %q", fields[0])
		}
	}

	if err := scanner.Err(); err != nil {
		return WitnessGroup{}, err
	}

	if policy == nil {
		return WitnessGroup{}, fmt.Errorf("policy file must define at least one group")
	}

	return *policy, nil
}

// NewWitness returns a Witness given a verifier key and the root URL for where this
// witness can be reached.
func NewWitness(vkey string, witnessRoot *url.URL) (Witness, error) {
	v, err := note.NewVerifier(vkey)
	if err != nil {
		return Witness{}, err
	}
	// "key hash" MUST be a lowercase hex-encoded SHA-256 hash of a 32-byte Ed25519 public key.
	// This expression cuts off the identity name and hash.
	key64 := strings.SplitAfterN(vkey, "+", 3)[2]
	key, err := base64.StdEncoding.DecodeString(key64)
	if err != nil {
		return Witness{}, err
	}
	h := sha256.Sum256(key)

	u := witnessRoot.JoinPath(fmt.Sprintf("/%x/add-checkpoint", h))

	return Witness{
		Key: v,
		URL: u.String(),
	}, err
}

// Witness represents a single witness that can be reached in order to perform a witnessing operation.
// The URLs() method returns the URL where it can be reached for witnessing, and the Satisfied method
// provides a predicate to check whether this witness has signed a checkpoint.
type Witness struct {
	Key note.Verifier
	URL string
}

// Satisfied returns true if the checkpoint provided is signed by this witness.
// This will return false if there is no signature, and also if the
// checkpoint cannot be read as a valid note. It is up to the caller to ensure
// that the input value represents a valid note.
func (w Witness) Satisfied(cp []byte) bool {
	n, err := note.Open(cp, note.VerifierList(w.Key))
	if err != nil {
		return false
	}
	return len(n.Sigs) == 1
}

// Endpoints returns the details required for updating a witness and checking the
// response. The returned result is a map from the URL that should be used to update
// the witness with a new checkpoint, to the value which is the verifier to check
// the response is well formed.
func (w Witness) Endpoints() map[string]note.Verifier {
	return map[string]note.Verifier{w.URL: w.Key}
}

// NewWitnessGroup creates a grouping of Witness or WitnessGroup with a configurable threshold
// of these sub-components that need to be satisfied in order for this group to be satisfied.
//
// The threshold should only be set to less than the number of sub-components if these are
// considered fungible.
func NewWitnessGroup(n int, children ...policyComponent) WitnessGroup {
	if n < 0 || n > len(children) {
		panic(fmt.Errorf("threshold of %d outside bounds for children %s", n, children))
	}
	return WitnessGroup{
		Components: children,
		N:          n,
	}
}

// WitnessGroup defines a group of witnesses, and a threshold of
// signatures that must be met for this group to be satisfied.
// Witnesses within a group should be fungible, e.g. all of the Armored
// Witness devices form a logical group, and N should be picked to
// represent a threshold of the quorum. For some users this will be a
// simple majority, but other strategies are available.
// N must be <= len(WitnessKeys).
type WitnessGroup struct {
	Components []policyComponent
	N          int
}

// Satisfied returns true if the checkpoint provided has sufficient signatures
// from the witnesses in this group to satisfy the threshold.
// This will return false if there are insufficient signatures, and also if the
// checkpoint cannot be read as a valid note. It is up to the caller to ensure
// that the input value represents a valid note.
//
// The implementation of this requires every witness in the group to verify the
// checkpoint, which is O(N). If this is called every time a witness returns a
// checkpoint then this algorithm is O(N^2). To support large N, this may require
// some rewriting in order to maintain performance.
func (wg WitnessGroup) Satisfied(cp []byte) bool {
	if wg.N <= 0 {
		return true
	}
	satisfaction := 0
	for _, c := range wg.Components {
		if c.Satisfied(cp) {
			satisfaction++
		}
		if satisfaction >= wg.N {
			return true
		}
	}
	return false
}

// Endpoints returns the details required for updating a witness and checking the
// response. The returned result is a map from the URL that should be used to update
// the witness with a new checkpoint, to the value which is the verifier to check
// the response is well formed.
func (wg WitnessGroup) Endpoints() map[string]note.Verifier {
	endpoints := make(map[string]note.Verifier)
	for _, c := range wg.Components {
		maps.Copy(endpoints, c.Endpoints())
	}
	return endpoints
}
