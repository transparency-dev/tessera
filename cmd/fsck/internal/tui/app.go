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

// Package tui provides a Bubbletea-based TUI for the fsck command.
package tui

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/transparency-dev/tessera/cmd/fsck/tui"
	"github.com/transparency-dev/tessera/fsck"
	"k8s.io/klog/v2"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// RunApp runs the TUI app, using the provided fsck instance to fetch updates from to populate the UI.
func RunApp(ctx context.Context, f *fsck.Fsck) error {
	m := newAppModel()
	p := tea.NewProgram(m)

	// Redirect logging so as to appear above the UI
	_ = flag.Set("logtostderr", "false")
	_ = flag.Set("alsologtostderr", "false")
	r, w := io.Pipe()
	klog.SetOutput(w)
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			l := lipgloss.Width(s.Text())
			txt := fmt.Sprintf("%s%s", strings.Repeat("\n", l), s.Text())
			p.Send(tea.Println(txt)())
		}
	}()

	// Send periodic status updates to the UI from fsck.
	go func() {
		for {
			select {
			case <-ctx.Done():
				// Have the UI update one last time to show where we got to (this helps ensure we see 100%
				// on the progress bars if we're exiting because the fsck has completed).
				p.Send(tui.FsckPanelUpdateCmd(f.Status())())
				// Give the UI a bit of time to render...
				<-time.After(100 * time.Millisecond)
				// And then we're out.
				p.Send(tea.Quit())
			case <-time.After(100 * time.Millisecond):
				p.Send(tui.FsckPanelUpdateCmd(f.Status())())
			}
		}
	}()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}

// newAppModel creates a new BubbleTea model for the TUI.
func newAppModel() *appModel {
	r := &appModel{
		fsckPanel: tui.NewFsckPanel(),
	}
	return r
}

// appModel represents the UI model for the FSCK TUI.
type appModel struct {
	// fsckPanel displays information about the fsck operation.
	fsckPanel *tui.FsckPanel
	// width is the width of the app window
	width int
}

// Init is called by Bubbleteam early on to set up the app.
func (m *appModel) Init() tea.Cmd {
	return nil
}

// Update is called by Bubbletea to handle events.
func (m *appModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Handle user input.
		// Quit if they pressed Q, escape, or CTRL-C.
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		var cmd tea.Cmd
		_, cmd = m.fsckPanel.Update(msg)
		return m, cmd
	case tui.FsckPanelUpdateMsg:
		// Ignore empty updates
		if len(msg.Status.TileRanges) == 0 {
			return m, nil
		}

		var cmd tea.Cmd
		_, cmd = m.fsckPanel.Update(msg)
		return m, cmd
	default:
		return m, nil
	}
}

// View is called by Bubbletea to render the UI components.
func (m *appModel) View() string {
	return m.fsckPanel.View()
}
