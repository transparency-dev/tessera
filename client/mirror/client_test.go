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

package mirror

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"testing"
)

func TestParseConflict(t *testing.T) {
	for _, tc := range []struct {
		desc    string
		body    string
		wantErr bool
		want    ErrConflict
	}{
		{
			desc: "valid conflict with ticket",
			body: "123\n456\ndGlja2V0LXZhbHVl\n", // dGlja2V0LXZhbHVl is base64 for "ticket-value"
			want: ErrConflict{
				PendingSize: 123,
				NextEntry:   456,
				Ticket:      []byte("ticket-value"),
			},
		},
		{
			desc: "valid conflict with empty ticket",
			body: "999\n1000\n\n",
			want: ErrConflict{
				PendingSize: 999,
				NextEntry:   1000,
				Ticket:      []byte{},
			},
		},
		{
			desc:    "missing newline at the end",
			body:    "123\n456\ndGlja2V0LXZhbHVl",
			wantErr: true,
		},
		{
			desc:    "invalid pending size",
			body:    "abc\n456\ndGlja2V0\n",
			wantErr: true,
		},
		{
			desc:    "invalid next entry",
			body:    "123\nxyz\ndGlja2V0\n",
			wantErr: true,
		},
		{
			desc:    "invalid base64 ticket",
			body:    "123\n456\nnot-base64-%\n",
			wantErr: true,
		},
		{
			desc:    "too few lines",
			body:    "123\n456\n",
			wantErr: true,
		},
		{
			desc:    "empty body",
			body:    "",
			wantErr: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := parseConflict(bytes.NewBufferString(tc.body))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseConflict() succeeded, want error")
				}
				var ec ErrConflict
				if errors.As(err, &ec) {
					t.Fatalf("parseConflict() returned ErrConflict %v, want parsing error", ec)
				}
				return
			}
			if err == nil {
				t.Fatalf("parseConflict() returned nil, want ErrConflict")
			}
			var ec ErrConflict
			if !errors.As(err, &ec) {
				t.Fatalf("parseConflict() returned error %v, want ErrConflict", err)
			}
			if ec.PendingSize != tc.want.PendingSize {
				t.Errorf("PendingSize = %d, want %d", ec.PendingSize, tc.want.PendingSize)
			}
			if ec.NextEntry != tc.want.NextEntry {
				t.Errorf("NextEntry = %d, want %d", ec.NextEntry, tc.want.NextEntry)
			}
			if !bytes.Equal(ec.Ticket, tc.want.Ticket) {
				t.Errorf("Ticket = %q, want %q", string(ec.Ticket), string(tc.want.Ticket))
			}
		})
	}
}

func TestBuildCheckpointRequestBody(t *testing.T) {
	tests := []struct {
		desc          string
		oldSize       uint64
		proof         [][]byte
		checkpointRaw []byte
		want          string
	}{
		{
			desc:          "empty proof and empty checkpoint",
			oldSize:       0,
			proof:         nil,
			checkpointRaw: nil,
			want:          "old 0\n\n",
		},
		{
			desc:          "empty proof and non-empty checkpoint",
			oldSize:       123,
			proof:         nil,
			checkpointRaw: []byte("some-checkpoint-data"),
			want:          "old 123\n\nsome-checkpoint-data",
		},
		{
			desc:    "single 32-byte proof line",
			oldSize: 5,
			proof: [][]byte{
				bytes.Repeat([]byte{1}, 32),
			},
			checkpointRaw: []byte("checkpoint"),
			want:          "old 5\n" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)) + "\n\ncheckpoint",
		},
		{
			desc:    "multiple 32-byte proof lines",
			oldSize: 10,
			proof: [][]byte{
				bytes.Repeat([]byte{1}, 32),
				bytes.Repeat([]byte{2}, 32),
			},
			checkpointRaw: []byte("checkpoint"),
			want: "old 10\n" +
				base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)) + "\n" +
				base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32)) + "\n\ncheckpoint",
		},
	}

	c := &Client{}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			gotReader := c.buildCheckpointRequestBody(tc.oldSize, tc.proof, tc.checkpointRaw)
			gotBytes, err := io.ReadAll(gotReader)
			if err != nil {
				t.Fatalf("failed to read body: %v", err)
			}
			if got := string(gotBytes); got != tc.want {
				t.Errorf("buildCheckpointRequestBody() = %q, want %q", got, tc.want)
			}
		})
	}
}
