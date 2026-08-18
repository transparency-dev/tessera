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

func fromPolicy(p policy.TLogPolicy) (WitnessGroup, error) {
	groups := make(map[string]WitnessGroup, len(p.Groups))
	witnesses := make(map[string]Witness, len(p.Witnesses))
	for _, w := range p.Witnesses {
		v, err := f_note.NewVerifierForCosignatureV1(w.VKey)
		if err != nil {
			return WitnessGroup{}, err
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
		groups[g.Name] = NewWitnessGroup(int(g.Threshold), members...)
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
	v, err := f_note.NewVerifierForCosignatureV1(vkey)
	if err != nil {
		return Witness{}, err
	}

	return Witness{
		vkey:      vkey,
		parsedURL: witnessRoot,
		Key:       v,
		URL:       witnessRoot.String(),
	}, nil
}

// Witness represents a single witness that can be reached in order to perform a witnessing operation.
//
// Deprecated: Use [github.com/transparency-dev/formats/policy] directly instead.
type Witness struct {
	vkey      string
	Key       note.Verifier
	URL       string
	parsedURL *url.URL
}

func (w Witness) name() string {
	return w.Key.Name()
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
			p.Witnesses = append(p.Witnesses, policy.Witness{
				Name:     c.name(),
				URL:      c.parsedURL,
				VKey:     c.vkey,
				Verifier: c.Key,
			})
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

func (wg WitnessGroup) toPolicy() policy.TLogPolicy {
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
	return wg.grpName
}
