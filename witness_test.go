package tessera_test

import (
	"net/url"
	"slices"
	"testing"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/tessera"
	"golang.org/x/mod/sumdb/note"
)

const (
	wit1_vkey = "Wit1+55ee4561+AVhZSmQj9+SoL+p/nN0Hh76xXmF7QcHfytUrI1XfSClk"
	wit1_skey = "PRIVATE+KEY+Wit1+55ee4561+AeadRiG7XM4XiieCHzD8lxysXMwcViy5nYsoXURWGrlE"
	wit2_vkey = "Wit2+85ecc407+AWVbwFJte9wMQIPSnEnj4KibeO6vSIOEDUTDp3o63c2x"
	wit2_skey = "PRIVATE+KEY+Wit2+85ecc407+AfPTvxw5eUcqSgivo2vaiC7JPOMUZ/9baHPSDrWqgdGm"
	wit3_vkey = "Wit3+d3ed3be7+ASb6Uz1+fxAcXkMvDd7nGa3FjDce7LxIKmbbTCT0MpVn"
	wit3_skey = "PRIVATE+KEY+Wit3+d3ed3be7+AR2Kg8k6ccBr5QXz5SHtnkOS4UGQGEQaWi6Gfr6Mm3X5"
)

var (
	bastion, _   = url.Parse("https://b1.example.com/")
	directURL, _ = url.Parse("https://witness.example.com/")
	wit1, _      = tessera.NewWitness(wit1_vkey, bastion.JoinPath("wit1prefix"))
	wit2, _      = tessera.NewWitness(wit2_vkey, bastion.JoinPath("wit2prefix"))
	wit3, _      = tessera.NewWitness(wit3_vkey, directURL)
	wit1Sign, _  = f_note.NewSignerForCosignatureV1(wit1_skey)
	wit2Sign, _  = f_note.NewSignerForCosignatureV1(wit2_skey)
	wit3Sign, _  = f_note.NewSignerForCosignatureV1(wit3_skey)
)

func TestWitnessGroup_Empty(t *testing.T) {
	group := tessera.WitnessGroup{}
	if !group.Satisfied([]byte("definitely a checkpoint\n")) {
		t.Error("empty group should be satisfied")
	}
	if len(group.Endpoints()) != 0 {
		t.Error("empty group should have no URLs")
	}
}

func TestWitnessGroup_Satisfied(t *testing.T) {
	testCases := []struct {
		desc            string
		group           tessera.WitnessGroup
		signers         []note.Signer
		expectSatisfied bool
	}{
		{
			desc:            "One witness, required and provided",
			group:           tessera.NewWitnessGroup(1, wit1),
			signers:         []note.Signer{wit1Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One witness, required and not provided",
			group:           tessera.NewWitnessGroup(1, wit1),
			signers:         []note.Signer{},
			expectSatisfied: false,
		},
		{
			desc:            "One witness, optional and provided",
			group:           tessera.NewWitnessGroup(0, wit1),
			signers:         []note.Signer{wit1Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One witness, optional and not provided",
			group:           tessera.NewWitnessGroup(0, wit1),
			signers:         []note.Signer{},
			expectSatisfied: true,
		},
		{
			desc:            "One witness, required and provided, in required subgroup",
			group:           tessera.NewWitnessGroup(1, tessera.NewWitnessGroup(1, wit1)),
			signers:         []note.Signer{wit1Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One witness, required and provided, in optional subgroup",
			group:           tessera.NewWitnessGroup(0, tessera.NewWitnessGroup(1, wit1)),
			signers:         []note.Signer{wit1Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One witness, required and not provided, in required subgroup",
			group:           tessera.NewWitnessGroup(1, tessera.NewWitnessGroup(1, wit1)),
			signers:         []note.Signer{},
			expectSatisfied: false,
		},
		{
			desc:            "One witness, required and not provided, in optional subgroup",
			group:           tessera.NewWitnessGroup(0, tessera.NewWitnessGroup(1, wit1)),
			signers:         []note.Signer{},
			expectSatisfied: true,
		},
		{
			desc:            "One required, one of two required, all provided",
			group:           tessera.NewWitnessGroup(2, wit1, tessera.NewWitnessGroup(1, wit2, wit3)),
			signers:         []note.Signer{wit1Sign, wit2Sign, wit3Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One required, one of two required, min provided",
			group:           tessera.NewWitnessGroup(2, wit1, tessera.NewWitnessGroup(1, wit2, wit3)),
			signers:         []note.Signer{wit1Sign, wit2Sign},
			expectSatisfied: true,
		},
		{
			desc:            "One required, one of two required, only first group satisfied",
			group:           tessera.NewWitnessGroup(2, wit1, tessera.NewWitnessGroup(1, wit2, wit3)),
			signers:         []note.Signer{wit1Sign},
			expectSatisfied: false,
		},
		{
			desc:            "One required, one of two required, only second group satisfied",
			group:           tessera.NewWitnessGroup(2, wit1, tessera.NewWitnessGroup(1, wit2, wit3)),
			signers:         []note.Signer{wit2Sign, wit3Sign},
			expectSatisfied: false,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			n := &note.Note{
				// The body needs to be 3 lines to meet the cosigner expectations.
				Text: "sign me\nI'm a\nnote\n",
			}
			cp, err := note.Sign(n, tC.signers...)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := tC.group.Satisfied(cp), tC.expectSatisfied; got != want {
				t.Errorf("Expected satisfied = %t but got %t", want, got)
			}
		})
	}
}

func TestWitnessGroup_URLs(t *testing.T) {
	testCases := []struct {
		desc         string
		group        tessera.WitnessGroup
		expectedURLs []string
	}{
		{
			desc:         "witness 1",
			group:        tessera.NewWitnessGroup(1, wit1),
			expectedURLs: []string{"https://b1.example.com/wit1prefix/add-checkpoint"},
		},
		{
			desc:         "witness 2",
			group:        tessera.NewWitnessGroup(1, wit2),
			expectedURLs: []string{"https://b1.example.com/wit2prefix/add-checkpoint"},
		},
		{
			desc:         "witness 3",
			group:        tessera.NewWitnessGroup(1, wit3),
			expectedURLs: []string{"https://witness.example.com/add-checkpoint"},
		},
		{
			desc:  "all witnesses in one group",
			group: tessera.NewWitnessGroup(1, wit1, wit2, wit3),
			expectedURLs: []string{
				"https://b1.example.com/wit1prefix/add-checkpoint",
				"https://b1.example.com/wit2prefix/add-checkpoint",
				"https://witness.example.com/add-checkpoint",
			},
		},
		{
			desc:  "all witnesses with duplicates in nests",
			group: tessera.NewWitnessGroup(2, tessera.NewWitnessGroup(1, wit1, wit2), tessera.NewWitnessGroup(1, wit1, wit3)),
			expectedURLs: []string{
				"https://b1.example.com/wit1prefix/add-checkpoint",
				"https://b1.example.com/wit2prefix/add-checkpoint",
				"https://witness.example.com/add-checkpoint",
			},
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			gotURLs := make([]string, 0)
			for u := range tC.group.Endpoints() {
				gotURLs = append(gotURLs, u)
			}
			slices.Sort(gotURLs)
			slices.Sort(tC.expectedURLs)

			if !slices.Equal(gotURLs, tC.expectedURLs) {
				t.Errorf("Expected %s but got %s", tC.expectedURLs, gotURLs)
			}
		})
	}
}

// This is benchmarked because this may well get called a number of times, and there are potentially
// other ways to implement this that don't involve so many note.Open calls.
func BenchmarkWitnessGroupSatisfaction(b *testing.B) {
	group := tessera.NewWitnessGroup(2, wit1, tessera.NewWitnessGroup(1, wit2, wit3))
	n := &note.Note{
		// Text must contain 3 lines to meet cosig expectations.
		Text: "sign me\nI'm a\nnote\n",
	}
	cp, err := note.Sign(n, wit1Sign, wit2Sign, wit3Sign)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if !group.Satisfied(cp) {
			b.Fatal("Group should have been satisfied!")
		}
	}
}
