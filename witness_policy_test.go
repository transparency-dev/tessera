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
	"strings"
	"testing"
)

func TestNewWitnessGroupFromPolicy(t *testing.T) {
	policy := `
witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
witness w2 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
group g1 all w1 w2
quorum g1
`
	r := strings.NewReader(policy)
	wg, err := NewWitnessGroupFromPolicy(r)
	if err != nil {
		t.Fatalf("NewWitnessGroupFromPolicy() failed: %v", err)
	}

	if wg.N != 2 {
		t.Errorf("Expected top-level group to have N=2, got %d", wg.N)
	}
	if len(wg.Components) != 2 {
		t.Fatalf("Expected top-level group to have 2 components, got %d", len(wg.Components))
	}
}

func TestNewWitnessGroupFromPolicy_GroupN(t *testing.T) {
	testCases := []struct {
		desc   string
		policy string
		wantN  int
	}{
		{
			desc: "group numerical",
			policy: `
witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
witness w2 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
witness w3 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
group g1 2 w1 w2 w3
quorum g1
`,
			wantN: 2,
		},
		{
			desc: "group all",
			policy: `
witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
witness w2 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
witness w3 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
group g1 all w1 w2 w3
quorum g1
`,
			wantN: 3,
		},
		{
			desc: "group any",
			policy: `
witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/
witness w2 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
witness w3 example.com+3753d3de+AebBhMcghIUoavZpjuDofa4sW6fYHyVn7gvwDBfvkvuM https://example.com/witness/
group g1 any w1
quorum g1
`,
			wantN: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			r := strings.NewReader(tc.policy)
			wg, err := NewWitnessGroupFromPolicy(r)
			if err != nil {
				t.Fatalf("NewWitnessGroupFromPolicy() failed: %v", err)
			}
			if wg.N != tc.wantN {
				t.Errorf("wg.N = %d, want %d", wg.N, tc.wantN)
			}
		})
	}
}

func TestNewWitnessGroupFromPolicy_Errors(t *testing.T) {
	testCases := []struct {
		desc   string
		policy string
		errStr string
	}{
		{
			desc:   "no quorum",
			policy: "witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/",
			errStr: "policy file must define a quorum",
		},
		{
			desc:   "unknown quorum component",
			policy: "quorum unknown",
			errStr: "quorum component \"unknown\" not found",
		},
		{
			desc:   "duplicate component name",
			policy: "witness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/\nwitness w1 sigsum.org+e4ade967+AZuUY6B08pW3QVHu8uvsrxWPcAv9nykap2Nb4oxCee+r https://sigsum.org/witness/\nquorum w1",
			errStr: "duplicate component name",
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

