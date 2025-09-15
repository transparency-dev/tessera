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
	"time"

	"github.com/transparency-dev/tessera/fsck"
	"k8s.io/klog/v2"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// RunApp runs the TUI app, using the provided fsck instance to fetch updates from to populate the UI.
func RunApp(ctx context.Context, f *fsck.Fsck) error {
	m := NewFsckAppModel()
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
				p.Send(tea.Quit())
			case <-time.After(100 * time.Millisecond):
				p.Send(UpdateMsg(f.Status()))
			}
		}
	}()

	if _, err := p.Run(); err != nil {
		return err
	}
	return nil
}

// NewFsckAppModel creates a new BubbleTea model for the TUI.
func NewFsckAppModel() *FsckAppModel {
	r := &FsckAppModel{}
	return r
}

// FsckAppModel represents the UI model for the FSCK TUI.
type FsckAppModel struct {
	// entriesBar is the status/progress bar representing progress through the entry bundles.
	entriesBar *layerProgressModel
	// titlesBars is the list status/progress bars representing progress through the various levels of tiles in the log.
	// The zeroth entry corresponds to the tiles on level zero.
	tilesBars []*layerProgressModel

	// width is the width of the app window
	width int
}

// Init is called by Bubbleteam early on to set up the app.
func (m *FsckAppModel) Init() tea.Cmd {
	return nil
}

// Update is called by Bubbletea to handle events.
func (m *FsckAppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Handle user input.
		// Quit if they pressed Q or escape
		switch msg.String() {
		case "q", "esc":
			return m, tea.Quit
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width

		var cmd tea.Cmd
		cmds := []tea.Cmd{}

		if m.entriesBar != nil {
			_, cmd = m.entriesBar.Update(msg)
			cmds = append(cmds, cmd)
		}

		for _, t := range m.tilesBars {
			_, cmd = t.Update(msg)
			cmds = append(cmds, cmd)
		}
		return m, tea.Batch(cmds...)
	case updateMsg:
		// Ignore empty updates
		if len(msg.s.TileRanges) == 0 {
			return m, nil
		}

		// Create the range progress bars now that we know how details about the tree.
		if len(m.tilesBars) != len(msg.s.TileRanges) {
			m.entriesBar = NewLayerProgressBar("Entry bundles", m.width, 0)

			bs := make([]*layerProgressModel, 0, len(msg.s.TileRanges))
			for i := range msg.s.TileRanges {
				bs = append(bs, NewLayerProgressBar(fmt.Sprintf("Tiles level %02d", i), m.width, i))
			}
			m.tilesBars = bs
		}

		// Update all the range progress bars with the latest state.
		_, cmd := m.entriesBar.Update(layerUpdateMsg{r: msg.s.EntryRanges})
		cmds := []tea.Cmd{cmd}
		for i := range m.tilesBars {
			_, cmd = m.tilesBars[i].Update(layerUpdateMsg{r: msg.s.TileRanges[i]})
			cmds = append(cmds, cmd)
		}

		return m, tea.Batch(cmds...)
	default:
		return m, nil
	}
}

// View is called by Bubbletea to render the UI components.
func (m *FsckAppModel) View() string {
	// Build the progress bars, we'll use this below.
	bars := []string{}
	for i := len(m.tilesBars) - 1; i >= 0; i-- {
		t := m.tilesBars[i]
		bars = append(bars, t.View())
	}
	if m.entriesBar != nil {
		bars = append(bars, m.entriesBar.View())
	}
	barsView := lipgloss.JoinVertical(lipgloss.Bottom, bars...)

	content := lipgloss.NewStyle().
		Width(m.width).
		Height(lipgloss.Height(barsView)).
		Align(lipgloss.Center, lipgloss.Bottom).
		Border(lipgloss.NormalBorder(), false, false, true, false).
		Render(barsView)

	return lipgloss.JoinVertical(lipgloss.Top, content)
}

// updateMsg is used to tell the model about updated status from the fsck library.
type updateMsg struct {
	s fsck.Status
}

// UpdateCmd returns a Cmd which Bubbletea can execute in order to retrieve and updateMsg.
func UpdateMsg(status fsck.Status) tea.Msg {
	return updateMsg{s: status}
}
