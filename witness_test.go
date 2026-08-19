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
	"slices"
	"testing"

	f_note "github.com/transparency-dev/formats/note"
	"golang.org/x/mod/sumdb/note"
)

func signNote(t *testing.T, signers ...note.Signer) []byte {
	t.Helper()
	n := &note.Note{
		Text: "sign me\nI'm a\nnote\n",
	}
	cp, err := note.Sign(n, signers...)
	if err != nil {
		t.Fatalf("failed to sign note: %v", err)
	}
	return cp
}

func TestPopulatePolicy(t *testing.T) {
	u1, err := url.Parse("https://wit1.example.com")
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}
	u2, err := url.Parse("https://wit2.example.com")
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}
	u3, err := url.Parse("https://wit3.example.com")
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}

	w1, err := NewWitness(testWit1VKey, u1)
	if err != nil {
		t.Fatalf("failed to create witness 1: %v", err)
	}
	w2, err := NewWitness(testWit2VKey, u2)
	if err != nil {
		t.Fatalf("failed to create witness 2: %v", err)
	}
	w3, err := NewWitness(testWit3VKey, u3)
	if err != nil {
		t.Fatalf("failed to create witness 3: %v", err)
	}
	// w2Clash is w2 under a key whose name happens to match w1's, as would be the case for a
	// witness which has rotated its key.
	w2Clash := w2
	w2Clash.polName = w1.polName

	w1Signer, err := f_note.NewSignerForCosignatureV1(testWit1SKey)
	if err != nil {
		t.Fatalf("failed to create witness 1 signer: %v", err)
	}
	w2Signer, err := f_note.NewSignerForCosignatureV1(testWit2SKey)
	if err != nil {
		t.Fatalf("failed to create witness 2 signer: %v", err)
	}
	w3Signer, err := f_note.NewSignerForCosignatureV1(testWit3SKey)
	if err != nil {
		t.Fatalf("failed to create witness 3 signer: %v", err)
	}

	for _, test := range []struct {
		desc          string
		group         WitnessGroup
		wantWitnesses []string
		satisfyTests  []struct {
			signers []note.Signer
			wantSat bool
		}
	}{
		{
			desc:          "single witness",
			group:         NewWitnessGroup(1, w1),
			wantWitnesses: []string{w1.name()},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
				{signers: []note.Signer{w2Signer}, wantSat: false},
			},
		},
		{
			desc:          "multi witness group (2 of 3)",
			group:         NewWitnessGroup(2, w1, w2, w3),
			wantWitnesses: []string{w1.name(), w2.name(), w3.name()},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer, w2Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer}, wantSat: false},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc: "nested witness group (1 required witness and 1 of 2 subgroup)",
			group: func() WitnessGroup {
				sub := NewWitnessGroup(1, w2, w3)
				return NewWitnessGroup(2, w1, sub)
			}(),
			wantWitnesses: []string{w1.name(), w2.name(), w3.name()},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer, w2Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w2Signer, w3Signer}, wantSat: false}, // w1 missing
				{signers: []note.Signer{w1Signer}, wantSat: false},          // subgroup missing
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc: "deeply nested witness group",
			group: func() WitnessGroup {
				sub1 := NewWitnessGroup(1, w1)
				sub2 := NewWitnessGroup(1, sub1)
				return NewWitnessGroup(1, sub2)
			}(),
			wantWitnesses: []string{w1.name()},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
				{signers: []note.Signer{w2Signer}, wantSat: false},
			},
		},
		{
			desc:          "empty group with 0 threshold",
			group:         NewWitnessGroup(0),
			wantWitnesses: nil,
		},
		{
			desc:          "witnesses with colliding key names stay distinct",
			group:         NewWitnessGroup(2, w1, w2Clash),
			wantWitnesses: []string{w1.name(), w1.name() + "-2"},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer, w2Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer}, wantSat: false},
				{signers: []note.Signer{w2Signer}, wantSat: false},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			pol := test.group.toPolicy()

			if got, want := pol.Quorum, test.group.name(); got != want {
				t.Errorf("pol.Quorum = %q, want %q", got, want)
			}

			var gotWitnesses []string
			for _, w := range pol.Witnesses {
				gotWitnesses = append(gotWitnesses, w.Name)
			}
			if !slices.Equal(gotWitnesses, test.wantWitnesses) {
				t.Errorf("pol.Witnesses = %v, want %v", gotWitnesses, test.wantWitnesses)
			}

			// Verify that every group in pol.Groups has valid member names (either witness or existing subgroup)
			validNames := make(map[string]bool)
			for _, w := range pol.Witnesses {
				validNames[w.Name] = true
			}
			for _, g := range pol.Groups {
				validNames[g.Name] = true
				if int(g.Threshold) > len(g.Members) {
					t.Errorf("group %q has threshold %d > members count %d (%v)", g.Name, g.Threshold, len(g.Members), g.Members)
				}
				for _, m := range g.Members {
					if !validNames[m] {
						t.Errorf("group %q references unknown member %q", g.Name, m)
					}
				}
			}

			// Check satisfaction tests
			for _, sTest := range test.satisfyTests {
				signedNote := signNote(t, sTest.signers...)
				got := pol.Satisfied(signedNote)
				if got != sTest.wantSat {
					t.Errorf("pol.Satisfied(signers=%d) = %v, want %v", len(sTest.signers), got, sTest.wantSat)
				}
			}
		})
	}
}

func TestNewWitnessGroupFromPolicy(t *testing.T) {
	wit1CoSigVKey, err := f_note.VKeyToCosignatureV1(testWit1VKey)
	if err != nil {
		t.Fatalf("failed to convert witness 1 vkey: %v", err)
	}
	wit2CoSigVKey, err := f_note.VKeyToCosignatureV1(testWit2VKey)
	if err != nil {
		t.Fatalf("failed to convert witness 2 vkey: %v", err)
	}
	wit3CoSigVKey, err := f_note.VKeyToCosignatureV1(testWit3VKey)
	if err != nil {
		t.Fatalf("failed to convert witness 3 vkey: %v", err)
	}

	w1Signer, err := f_note.NewSignerForCosignatureV1(testWit1SKey)
	if err != nil {
		t.Fatalf("failed to create witness 1 signer: %v", err)
	}
	w2Signer, err := f_note.NewSignerForCosignatureV1(testWit2SKey)
	if err != nil {
		t.Fatalf("failed to create witness 2 signer: %v", err)
	}
	w3Signer, err := f_note.NewSignerForCosignatureV1(testWit3SKey)
	if err != nil {
		t.Fatalf("failed to create witness 3 signer: %v", err)
	}

	for _, test := range []struct {
		desc         string
		policy       string
		wantErr      bool
		wantN        int
		wantChildren int
		checkGroup   func(t *testing.T, wg WitnessGroup)
		satisfyTests []struct {
			signers []note.Signer
			wantSat bool
		}
	}{
		{
			desc: "single witness",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
group q 1 w1
quorum q
`, wit1CoSigVKey),
			wantN:        1,
			wantChildren: 1,
			checkGroup: func(t *testing.T, wg WitnessGroup) {
				w, ok := wg.Components[0].(Witness)
				if !ok {
					t.Fatalf("expected component 0 to be Witness, got %T", wg.Components[0])
				}
				if got, want := w.URL, "https://wit1.example.com"; got != want {
					t.Errorf("w.URL = %q, want %q", got, want)
				}
				if got, want := w.vkey, wit1CoSigVKey; got != want {
					t.Errorf("w.vkey = %q, want %q", got, want)
				}
				if got, want := w.Key.Name(), "Wit1"; got != want {
					t.Errorf("w.Key.Name() = %q, want %q", got, want)
				}
			},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
				{signers: []note.Signer{w2Signer}, wantSat: false},
			},
		},
		{
			desc: "single witness direct quorum",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
quorum w1
`, wit1CoSigVKey),
			wantN:        1,
			wantChildren: 1,
			checkGroup: func(t *testing.T, wg WitnessGroup) {
				w, ok := wg.Components[0].(Witness)
				if !ok {
					t.Fatalf("expected component 0 to be Witness, got %T", wg.Components[0])
				}
				if got, want := w.URL, "https://wit1.example.com"; got != want {
					t.Errorf("w.URL = %q, want %q", got, want)
				}
				if got, want := w.vkey, wit1CoSigVKey; got != want {
					t.Errorf("w.vkey = %q, want %q", got, want)
				}
				if got, want := w.Key.Name(), "Wit1"; got != want {
					t.Errorf("w.Key.Name() = %q, want %q", got, want)
				}
			},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
				{signers: []note.Signer{w2Signer}, wantSat: false},
			},
		},
		{
			desc:         "quorum none",
			policy:       `quorum none`,
			wantN:        0,
			wantChildren: 0,
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{}, wantSat: true},
				{signers: []note.Signer{w1Signer}, wantSat: true},
			},
		},
		{
			desc: "witness without URL",
			policy: fmt.Sprintf(`witness w1 %s
quorum w1
`, wit1CoSigVKey),
			wantN:        1,
			wantChildren: 1,
			checkGroup: func(t *testing.T, wg WitnessGroup) {
				w, ok := wg.Components[0].(Witness)
				if !ok {
					t.Fatalf("expected component 0 to be Witness, got %T", wg.Components[0])
				}
				if got, want := w.URL, ""; got != want {
					t.Errorf("w.URL = %q, want %q", got, want)
				}
				if got, want := w.parsedURL == nil, true; got != want {
					t.Errorf("w.parsedURL == nil is %v, want %v", got, want)
				}
				if got, want := w.vkey, wit1CoSigVKey; got != want {
					t.Errorf("w.vkey = %q, want %q", got, want)
				}
				if got, want := w.Key.Name(), "Wit1"; got != want {
					t.Errorf("w.Key.Name() = %q, want %q", got, want)
				}
			},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc: "multi witness group (2 of 3)",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
witness w2 %s https://wit2.example.com
witness w3 %s https://wit3.example.com
group q 2 w1 w2 w3
quorum q
`, wit1CoSigVKey, wit2CoSigVKey, wit3CoSigVKey),
			wantN:        2,
			wantChildren: 3,
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer, w2Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer}, wantSat: false},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc: "nested witness group (1 required and 1 of 2 subgroup)",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
witness w2 %s https://wit2.example.com
witness w3 %s https://wit3.example.com
group sub 1 w2 w3
group q 2 w1 sub
quorum q
`, wit1CoSigVKey, wit2CoSigVKey, wit3CoSigVKey),
			wantN:        2,
			wantChildren: 2,
			checkGroup: func(t *testing.T, wg WitnessGroup) {
				if _, ok := wg.Components[0].(Witness); !ok {
					t.Errorf("expected component 0 to be Witness, got %T", wg.Components[0])
				}
				sub, ok := wg.Components[1].(WitnessGroup)
				if !ok {
					t.Fatalf("expected component 1 to be WitnessGroup, got %T", wg.Components[1])
				}
				if got, want := sub.N, 1; got != want {
					t.Errorf("sub.N = %d, want %d", got, want)
				}
				if got, want := len(sub.Components), 2; got != want {
					t.Errorf("len(sub.Components) = %d, want %d", got, want)
				}
			},
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer, w2Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w1Signer, w2Signer, w3Signer}, wantSat: true},
				{signers: []note.Signer{w2Signer, w3Signer}, wantSat: false},
				{signers: []note.Signer{w1Signer}, wantSat: false},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc: "group using any and all keywords",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
witness w2 %s https://wit2.example.com
group sub any w1 w2
group q all sub
quorum q
`, wit1CoSigVKey, wit2CoSigVKey),
			wantN:        1,
			wantChildren: 1,
			satisfyTests: []struct {
				signers []note.Signer
				wantSat bool
			}{
				{signers: []note.Signer{w1Signer}, wantSat: true},
				{signers: []note.Signer{w2Signer}, wantSat: true},
				{signers: []note.Signer{}, wantSat: false},
			},
		},
		{
			desc:    "invalid policy syntax",
			policy: "invalid policy text",
			wantErr: true,
		},
		{
			desc: "invalid witness verifier key",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
group q 1 w1
quorum q
`, "invalid+verifier+key"),
			wantErr: true,
		},
		{
			desc: "group references undefined member",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
group q 1 undefined
quorum q
`, wit1CoSigVKey),
			wantErr: true,
		},
		{
			desc: "quorum references undefined group",
			policy: fmt.Sprintf(`witness w1 %s https://wit1.example.com
group q 1 w1
quorum unknown
`, wit1CoSigVKey),
			wantErr: true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			group, err := NewWitnessGroupFromPolicy([]byte(test.policy))
			if (err != nil) != test.wantErr {
				t.Fatalf("NewWitnessGroupFromPolicy() error = %v, wantErr = %v", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if got, want := group.N, test.wantN; got != want {
				t.Errorf("group.N = %d, want %d", got, want)
			}
			if got, want := len(group.Components), test.wantChildren; got != want {
				t.Errorf("len(group.Components) = %d, want %d", got, want)
			}
			if test.checkGroup != nil {
				test.checkGroup(t, group)
			}

			pol := group.toPolicy()
			for _, sTest := range test.satisfyTests {
				signedNote := signNote(t, sTest.signers...)
				got := pol.Satisfied(signedNote)
				if got != sTest.wantSat {
					t.Errorf("pol.Satisfied(signers=%d) = %v, want %v", len(sTest.signers), got, sTest.wantSat)
				}
			}
		})
	}
}

