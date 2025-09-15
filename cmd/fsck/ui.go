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

package main

import (
	"bufio"
	"context"
	"flag"
	"io"
	"time"

	"github.com/transparency-dev/tessera/cmd/fsck/tui"
	"github.com/transparency-dev/tessera/fsck"
	"k8s.io/klog/v2"

	tea "github.com/charmbracelet/bubbletea"
)

func runUI(ctx context.Context, f *fsck.Fsck) error {
	m := tui.NewFsckAppModel()
	p := tea.NewProgram(m)

	// Redirect logging so as to appear above the UI
	_ = flag.Set("logtostderr", "false")
	_ = flag.Set("alsologtostderr", "false")
	r, w := io.Pipe()
	klog.SetOutput(w)
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			p.Send(tea.Println(s.Text())())
		}
	}()

	// Send periodic status updates to the UI from fsck.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				p.Send(tui.UpdateMsg(f.Status()))
			}
		}
	}()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}
