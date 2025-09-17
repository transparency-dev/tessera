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

package tui

import (
	"fmt"

	"github.com/transparency-dev/tessera/fsck"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// NewFsckPanel creates a new TUI panel showing information about an ongoing fsck operation.
func NewFsckPanel() *FsckPanel {
	r := &FsckPanel{}
	return r
}

// FsckPanel represents the UI model for the FSCK TUI.
type FsckPanel struct {
	// entriesBar is the status/progress bar representing progress through the entry bundles.
	entriesBar *LayerProgressModel
	// titlesBars is the list status/progress bars representing progress through the various levels of tiles in the log.
	// The zeroth entry corresponds to the tiles on level zero.
	tilesBars []*LayerProgressModel

	// width is the width of the app window
	width int
}

// Init is called by Bubbleteam early on to set up the app.
func (m *FsckPanel) Init() tea.Cmd {
	return nil
}

// Update is called by Bubbletea to handle events.
func (m *FsckPanel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
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
	case FsckPanelUpdateMsg:
		// Ignore empty updates
		if len(msg.Status.TileRanges) == 0 {
			return m, nil
		}

		// Create the range progress bars now that we know how details about the tree.
		if len(m.tilesBars) != len(msg.Status.TileRanges) {
			m.entriesBar = NewLayerProgressBar("Entry bundles", m.width, 0)

			bs := make([]*LayerProgressModel, 0, len(msg.Status.TileRanges))
			for i := range msg.Status.TileRanges {
				bs = append(bs, NewLayerProgressBar(fmt.Sprintf("Tiles level %02d", i), m.width, i))
			}
			m.tilesBars = bs
		}

		// Update all the range progress bars with the latest state.
		_, cmd := m.entriesBar.Update(LayerUpdateMsg{Ranges: msg.Status.EntryRanges})
		cmds := []tea.Cmd{cmd}
		for i := range m.tilesBars {
			_, cmd = m.tilesBars[i].Update(LayerUpdateMsg{Ranges: msg.Status.TileRanges[i]})
			cmds = append(cmds, cmd)
		}

		return m, tea.Batch(cmds...)
	default:
		return m, nil
	}
}

// View is called by Bubbletea to render the UI components.
func (m *FsckPanel) View() string {
	// Build the progress bars, we'll use this below.
	bars := []string{}
	for i := len(m.tilesBars) - 1; i >= 0; i-- {
		t := m.tilesBars[i]
		bars = append(bars, t.View())
	}
	if m.entriesBar != nil {
		bars = append(bars, m.entriesBar.View())
	}
	bars = append(bars, LayerProgressKey())
	barsView := lipgloss.JoinVertical(lipgloss.Bottom, bars...)

	content := lipgloss.NewStyle().
		Width(m.width).
		Height(lipgloss.Height(barsView)+1).
		Align(lipgloss.Center, lipgloss.Top).
		Border(lipgloss.NormalBorder(), true, false, false, false).
		Render(barsView)

	return lipgloss.JoinVertical(lipgloss.Top, content)
}

// FsckPanelUpdateMsg is used to tell the FsckPanel about updated status from the fsck operation.
type FsckPanelUpdateMsg struct {
	Status fsck.Status
}

// FsckPanelUpdateCmd returns a Cmd which Bubbletea can execute in order to retrieve and updateMsg.
func FsckPanelUpdateCmd(status fsck.Status) tea.Cmd {
	return func() tea.Msg {
		return FsckPanelUpdateMsg{Status: status}
	}
}
