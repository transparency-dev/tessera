// Copyright 2025 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use a copy of the License at
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
	"strings"
	"testing"
)

func TestNewWitnessGroupFromPolicy(t *testing.T) {
	policy := `
# Witness verifier keys.
w1: witness sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
w2: witness example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/

# A group of witnesses.
all: group 2 w1 w2

# The policy is the last group defined in the file.
group 1 all
`
	r := strings.NewReader(policy)
	wg, err := NewWitnessGroupFromPolicy(r)
	if err != nil {
		t.Fatalf("NewWitnessGroupFromPolicy() failed: %v", err)
	}

	if wg.N != 1 {
		t.Errorf("Expected top-level group to have N=1, got %d", wg.N)
	}
	if len(wg.Components) != 1 {
		t.Fatalf("Expected top-level group to have 1 component, got %d", len(wg.Components))
	}

	allGroup, ok := wg.Components[0].(WitnessGroup)
	if !ok {
		t.Fatalf("Expected component to be a WitnessGroup, got %T", wg.Components[0])
	}

	if allGroup.N != 2 {
		t.Errorf("Expected 'all' group to have N=2, got %d", allGroup.N)
	}
	if len(allGroup.Components) != 2 {
		t.Fatalf("Expected 'all' group to have 2 components, got %d", len(allGroup.Components))
	}

	w1, ok := allGroup.Components[0].(Witness)
	if !ok {
		t.Fatalf("Expected component to be a Witness, got %T", allGroup.Components[0])
	}
	// Just a basic check on one of the witnesses.
	if !strings.HasPrefix(w1.Key.Name(), "sigsum.org") {
		t.Errorf("Unexpected key name for w1: %s", w1.Key.Name())
	}
	expectedURL := "https://sigsum.org/witness/cdf8b02f7bf41bbe99f67bddab675bdd089aec0af050cf6241de26ac8d6c3f7d/add-checkpoint"
	if w1.URL != expectedURL {
		t.Errorf("w1.URL = %q, want %q", w1.URL, expectedURL)
	}
}

func TestNewWitnessGroupFromPolicyErrors(t *testing.T) {
	testCases := []struct {
		desc   string
		policy string
		errStr string
	}{
		{
			desc:   "anonymous witness",
			policy: "witness key url",
			errStr: "anonymous witness definition is not allowed",
		},
		{
			desc:   "duplicate name",
			policy: `w1: witness sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
w1: witness example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/`,
			errStr: "duplicate component name",
		},
		{
			desc:   "unknown component in group",
			policy: "group 1 unknown",
			errStr: "unknown component",
		},
		{
			desc:   "no groups",
			policy: "w1: witness sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/",
			errStr: "policy file must define at least one group",
		},
		{
			desc:   "invalid witness def",
			policy: "w1: witness key",
			errStr: "invalid witness definition",
		},
		{
			desc:   "invalid group def",
			policy: "g1: group",
			errStr: "invalid group definition",
		},
		{
			desc:   "invalid group N",
			policy: "g1: group foo w1",
			errStr: "invalid threshold N",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			r := strings.NewReader(tc.policy)
			_, err := NewWitnessGroupFromPolicy(r)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errStr) {
				t.Errorf("Expected error string to contain %q, got %q", tc.errStr, err.Error())
			}
		})
	}
}
