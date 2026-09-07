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
	"fmt"
	"net/url"
	"sync/atomic"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/formats/policy"
	"golang.org/x/mod/sumdb/note"
)

// policyComponent describes a component that makes up a policy. This is either a
// single Witness, or a WitnessGroup.
type policyComponent interface {
	// Satisfied returns true if the checkpoint is signed by the quorum of
	// witnesses involved in this policy component.
	Satisfied(cp []byte) bool

	// WitnessEndpoints returns the details required for updating a witness and checking the
	// response. The returned result is a map from the URL that should be used to update
	// the witness with a new checkpoint, to the values which are the verifiers to check
	// the response is well formed.
	WitnessEndpoints() map[string][]note.Verifier

	name() string
}

// NewWitnessGroupFromPolicy parses a policy description and returns a WitnessGroup
// which can be passed to the WithWitnesses appender lifecycle option.
//
// The policy structure is as described at https://c2sp.org/tlog-policy.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
func NewWitnessGroupFromPolicy(p []byte) (WitnessGroup, error) {
	ret := policy.TLogPolicy{}
	if err := ret.Unmarshal(p); err != nil {
		return WitnessGroup{}, err
	}
	return fromPolicy(ret)
}

// FromPolicy converts a [policy.TLogPolicy] to a [WitnessGroup].
//
// This is only needed while we're in the process of migrating this codebase to
// TLogPolicy, and can be removed once the migration is complete and before
// we cut a new release.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
func FromPolicy(p policy.TLogPolicy) (WitnessGroup, error) {
	return fromPolicy(p)
}

func fromPolicy(p policy.TLogPolicy) (WitnessGroup, error) {
	groups := make(map[string]WitnessGroup, len(p.Groups))
	witnesses := make(map[string]Witness, len(p.Witnesses))
	for _, w := range p.Witnesses {
		v := w.Verifier
		if v == nil && w.VKey != "" {
			var err error
			v, err = f_note.NewVerifierForCosignatureV1(w.VKey)
			if err != nil {
				return WitnessGroup{}, err
			}
		}
		var urlStr string
		if w.URL != nil {
			urlStr = w.URL.String()
		}
		witnesses[w.Name] = Witness{
			vkey:      w.VKey,
			parsedURL: w.URL,
			Key:       v,
			URL:       urlStr,
		}
	}
	for _, g := range p.Groups {
		members := make([]policyComponent, 0, len(g.Members))
		for _, m := range g.Members {
			if w, ok := witnesses[m]; ok {
				members = append(members, w)
			} else if grp, ok := groups[m]; ok {
				members = append(members, grp)
			} else {
				return WitnessGroup{}, fmt.Errorf("invalid policy: member %q not defined", m)
			}
		}
		wg := NewWitnessGroup(int(g.Threshold), members...)
		wg.grpName = g.Name
		groups[g.Name] = wg
	}

	if p.Quorum == "none" || p.Quorum == "" {
		return NewWitnessGroup(0), nil
	}
	if root, ok := groups[p.Quorum]; ok {
		return root, nil
	}
	if w, ok := witnesses[p.Quorum]; ok {
		return NewWitnessGroup(1, w), nil
	}
	return WitnessGroup{}, fmt.Errorf("invalid policy: quorum %q not defined", p.Quorum)
}

// NewWitness returns a Witness given a verifier key and the root URL for where this
// witness can be reached.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
func NewWitness(vkey string, witnessRoot *url.URL) (Witness, error) {
	var v note.Verifier
	var err error
	v, err = f_note.NewMLDSAVerifier(vkey)
	if err != nil {
		var v1Err error
		v, v1Err = f_note.NewVerifierForCosignatureV1(vkey)
		if v1Err != nil {
			return Witness{}, fmt.Errorf("failed to parse verifier key %q as ML-DSA (%v) or Cosignature V1 (%w)", vkey, err, v1Err)
		}
	}

	var rootStr string
	if witnessRoot != nil {
		rootStr = witnessRoot.String()
	}
	return Witness{
		vkey:      vkey,
		parsedURL: witnessRoot,
		Key:       v,
		URL:       rootStr,
	}, nil
}

// Witness represents a single witness that can be reached in order to perform a witnessing operation.
// The URLs() method returns the URL where it can be reached for witnessing, and the Satisfied method
// provides a predicate to check whether this witness has signed a checkpoint.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
type Witness struct {
	vkey      string
	Key       note.Verifier
	URL       string
	parsedURL *url.URL
}

func (w Witness) name() string {
	if w.Key != nil {
		return w.Key.Name()
	}
	return ""
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
// response.
//
// Deprecated: Endpoints is deprecated, use WitnessEndpoints instead.
func (w Witness) Endpoints() map[string]note.Verifier {
	return map[string]note.Verifier{w.URL: w.Key}
}

// WitnessEndpoints returns the details required for updating a witness and checking the
// response. The returned result is a map from the URL that should be used to update
// the witness with a new checkpoint, to the values which are the verifiers to check
// the response is well formed.
func (w Witness) WitnessEndpoints() map[string][]note.Verifier {
	return map[string][]note.Verifier{w.URL: {w.Key}}
}

var anonGroupNameCounter atomic.Int64

// NewWitnessGroup creates a grouping of Witness or WitnessGroup with a configurable threshold
// of these sub-components that need to be satisfied in order for this group to be satisfied.
//
// The threshold should only be set to less than the number of sub-components if these are
// considered fungible.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
func NewWitnessGroup(n int, children ...policyComponent) WitnessGroup {
	if n < 0 || n > len(children) {
		panic(fmt.Errorf("threshold of %d outside bounds for children %s", n, children))
	}
	return WitnessGroup{
		grpName:    fmt.Sprintf("anonGrp-%d", anonGroupNameCounter.Add(1)),
		Components: children,
		N:          n,
	}
}

func populatePolicy(p *policy.TLogPolicy, wg WitnessGroup) {
	me := &policy.Group{
		Name:      wg.name(),
		Threshold: uint(wg.N),
		Members:   make([]string, 0, len(wg.Components)),
	}
	for _, c := range wg.Components {
		switch c := c.(type) {
		case Witness:
			u := c.parsedURL
			if u == nil && c.URL != "" {
				u, _ = url.Parse(c.URL)
			}
			alreadyAdded := false
			for _, existing := range p.Witnesses {
				if existing.Name == c.name() {
					alreadyAdded = true
					break
				}
			}
			if !alreadyAdded {
				p.Witnesses = append(p.Witnesses, policy.Witness{
					Name:     c.name(),
					URL:      u,
					VKey:     c.vkey,
					Verifier: c.Key,
				})
			}
			me.Members = append(me.Members, c.name())
		case WitnessGroup:
			populatePolicy(p, c)
			me.Members = append(me.Members, c.name())
		default:
			panic(fmt.Errorf("unexpected component type: %T", c))
		}
	}
	p.Groups = append(p.Groups, *me)
}

// ToPolicy converts a [WitnessGroup] to a [policy.TLogPolicy].
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
func (wg WitnessGroup) ToPolicy() policy.TLogPolicy {
	return wg.toPolicy()
}

func (wg WitnessGroup) toPolicy() policy.TLogPolicy {
	if wg.N == 0 || len(wg.Components) == 0 {
		return policy.TLogPolicy{
			Quorum: "none",
		}
	}
	p := policy.TLogPolicy{
		Quorum: wg.name(),
	}
	populatePolicy(&p, wg)
	return p
}

// WitnessGroup defines a group of witnesses, and a threshold of
// signatures that must be met for this group to be satisfied.
// Witnesses within a group should be fungible, e.g. all of the Armored
// Witness devices form a logical group, and N should be picked to
// represent a threshold of the quorum. For some users this will be a
// simple majority, but other strategies are available.
// N must be <= len(WitnessKeys).
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
type WitnessGroup struct {
	grpName    string
	Components []policyComponent
	N          int
}

func (wg WitnessGroup) name() string {
	if wg.grpName != "" {
		return wg.grpName
	}
	return fmt.Sprintf("anonGrp-%d", anonGroupNameCounter.Add(1))
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
// response.
//
// Deprecated: Endpoints is deprecated because it cannot handle policies with
// multiple verifiers per endpoint (e.g. witnesses which return multiple signatures).
// Use [WitnessEndpoints] instead.
func (wg WitnessGroup) Endpoints() map[string]note.Verifier {
	endpoints := make(map[string]note.Verifier)
	for _, c := range wg.Components {
		for u, v := range c.WitnessEndpoints() {
			if _, ok := endpoints[u]; ok {
				panic(fmt.Errorf("the Endpoints func cannot safely handle witnesses which return multiple signatures, use WitnessEndpoints instead"))
			}
			switch l := len(v); {
			case l > 1:
				panic(fmt.Errorf("the Endpoints func cannot safely handle witnesses which return multiple signatures, use WitnessEndpoints instead"))
			case l == 1:
				endpoints[u] = v[0]
			}
		}
	}
	return endpoints
}

// WitnessEndpoints returns the details required for updating a witness and checking the
// response. The returned result is a map from the URL that should be used to update
// the witness with a new checkpoint, to the values which are the verifiers to check
// the response is well formed.
func (wg WitnessGroup) WitnessEndpoints() map[string][]note.Verifier {
	endpoints := make(map[string][]note.Verifier)
	for _, c := range wg.Components {
		for u, vs := range c.WitnessEndpoints() {
			endpoints[u] = append(endpoints[u], vs...)
		}
	}
	return endpoints
}
