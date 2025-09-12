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
	"math"
	"strings"

	"github.com/muesli/termenv"
	"github.com/transparency-dev/tessera/fsck"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

var (
	percentageStyle = lipgloss.NewStyle().
			MaxWidth(len(" 100.00%")).
			Align(lipgloss.Right).
			Width(len(" 100.00%")).
			Foreground(lipgloss.Color("#f2cdcd"))

	labelStyle = lipgloss.NewStyle().
			MaxWidth(16).
			Align(lipgloss.Left).
			Width(16).
			Foreground(lipgloss.Color("#f2cdcd"))

	stateStyle = map[fsck.State]struct {
		// style is the style to use for this state in the progress bar.
		style lipgloss.Style
		// bar is the character(s) used for representing this state in the progress bar.
		bar string
		// priority maps each possible state to a relative importance of that priority.
		// This is used when displaying the overall state of a range of resources where multiple
		// states are present.
		// The highest priority state represented in the range "wins".
		priority int
	}{
		fsck.Unknown: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#313244")),
			bar:      "⠤",
			priority: 1,
		},
		fsck.Fetching: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#eeeeee")),
			bar:      "▄",
			priority: 3,
		},
		fsck.FetchError: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#fab387")),
			bar:      "E",
			priority: 9,
		},
		fsck.Fetched: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#94e2d5")),
			bar:      "▄",
			priority: 4,
		},
		fsck.Calculating: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#89dceb")),
			bar:      "C",
			priority: 5,
		},
		fsck.OK: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#a6e3a1")),
			bar:      "▄",
			priority: 2,
		},
		fsck.Invalid: {
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#f38ba8")),
			bar:      "!",
			priority: 10,
		},
	}
)

func NewAppModel() *AppModel {
	r := &AppModel{}
	return r
}

type AppModel struct {
	// components
	entriesBar *layerProgressModel
	tilesBars  []*layerProgressModel

	// styles
	appStyle lipgloss.Style

	width, height int
}

// Init is called by Bubbleteam early on to set up the app.
func (m *AppModel) Init() tea.Cmd {
	return nil
}

// Update is called by Bubbletea to handle events.
func (m *AppModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "q" {
			return m, tea.Quit
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

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
func (m *AppModel) View() string {
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

	header := lipgloss.NewStyle().
		Align(lipgloss.Center).
		Width(m.width).
		Border(lipgloss.NormalBorder(), false, false, true, false).
		Render("fsck")
	content := lipgloss.NewStyle().
		Width(m.width).
		Height(lipgloss.Height(barsView)).
		Align(lipgloss.Center, lipgloss.Bottom).
		Border(lipgloss.NormalBorder(), false, false, true, false).
		Render(barsView)
	messages := lipgloss.NewStyle().
		Align(lipgloss.Center).
		Width(m.width).
		Height(m.height-lipgloss.Height(header)-lipgloss.Height(content)).
		Border(lipgloss.NormalBorder(), true, false, true, false).
		Render("messages")

	return lipgloss.JoinVertical(lipgloss.Top, header, messages, content)
}

// updateMsg is used to tell the model about updated status from the fsck.
type updateMsg struct {
	s fsck.Status
}

// getUpdateCmd returns a Cmd which Bubbletea can execute in order to retrieve and updateMsg.
func UpdateCmd(status fsck.Status) tea.Cmd {
	return func() tea.Msg {
		return updateMsg{s: status}
	}
}

// layerUpdateMsg is a message which carries information for a specific layer in the tree (e.g. tiles for a specific level).
type layerUpdateMsg struct {
	r []fsck.Range
}

// NewLayerProgressBar returns a progress bar which can render information about the states of fsck ranges.
//
// Label is the name of the range, and will be shown in the UI.
// width is the total width available to this model.
func NewLayerProgressBar(label string, width int, level int) *layerProgressModel {
	m := &layerProgressModel{
		label:           label,
		width:           width,
		level:           level,
		colorProfile:    termenv.ColorProfile(),
		PercentageStyle: percentageStyle,
		LabelStyle:      labelStyle,
	}
	return m
}

// layerProgressModel is the UI model for a progress bar which represents fsck status through a paricular level of the tree.
type layerProgressModel struct {
	// label is a human readable name associated with this progress bar, and is shown in the UI.
	label string

	// PercentageStyle controls how the progress percentage is rendered.
	PercentageStyle lipgloss.Style
	// LabelStyle controls how the label string is rendered.
	LabelStyle lipgloss.Style

	// Color profile for the progress bar.
	colorProfile termenv.Profile

	// width available to this component.
	width int

	// level is the tile-space level this progress bar represents.
	// used to scale the bar portion of the component.
	level int

	// Current state this progress bar is representing.
	state []fsck.Range
}

func (m *layerProgressModel) Init() tea.Cmd {
	return nil
}

// Update is used to animate the progress bar during transitions. Use
// SetPercent to create the command you'll need to trigger the animation.
//
// If you're rendering with ViewAs you won't need this.
func (m *layerProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case layerUpdateMsg:
		m.state = msg.r
		return m, nil
	default:
		return m, nil
	}
}

func (m *layerProgressModel) View() string {
	if m.state == nil {
		return ""
	}
	return m.ViewAs(m.state)
}

func (m *layerProgressModel) ViewAs(rs []fsck.Range) string {
	// Don't bother trying to show empty information.
	if len(rs) == 0 {
		return ""
	}

	extent := rs[len(rs)-1].First + rs[len(rs)-1].N
	byState := make(map[fsck.State]uint64)
	for _, r := range rs {
		byState[r.State] += r.N
	}
	percentView := m.percentageView(float64(byState[fsck.OK]) / float64(extent))
	labelView := m.LabelStyle.Inline(true).Render(m.label)
	barSize := m.width - ansi.StringWidth(labelView) - 1 - ansi.StringWidth(percentView) - 1
	levelSize := barSize >> m.level
	barView := renderBar(rs, levelSize)

	return lipgloss.JoinHorizontal(
		lipgloss.Left,
		labelView,
		lipgloss.NewStyle().Width(barSize).MaxWidth(barSize).Align(lipgloss.Center).Render(barView),
		percentView)
}

// stateForRange figures out the right state style to use for the progress bar section covering range [f, f+n),
// using the provided fsck status ranges.
func stateForRange(rs []fsck.Range, f, n uint64) string {
	ret := "?"
	pri := 0
	for _, r := range rs {
		if (f >= r.First && f < r.First+r.N) ||
			(f < r.First && f+n > r.First+r.N) ||
			(f+n > r.First && f+n < r.First+r.N) {
			s := stateStyle[r.State]
			if s.priority > pri {
				pri = s.priority
				ret = s.style.Inline(true).Render(s.bar)
			}
		}
	}
	return ret
}

// renderBar builds a complete width-sized rendering for a progress bar representing the provided fsck ranges.
func renderBar(r []fsck.Range, width int) string {
	if len(r) == 0 {
		return strings.Repeat(" ", int(width))
	}

	sb := strings.Builder{}
	rangeExtent := r[len(r)-1].First + r[len(r)-1].N
	rFirst := float64(0)
	for i := range width {
		chunk := (float64(rangeExtent) - rFirst - 1) / float64(width-i)
		_, _ = sb.WriteString(stateForRange(r, uint64(math.Round(rFirst)), uint64(math.Round(chunk))))
		rFirst += chunk
	}
	return sb.String()
}

// percentageView returns a rendering of the provided percentage value.
func (m *layerProgressModel) percentageView(percent float64) string {
	percent = math.Max(0, math.Min(1, percent))
	percentage := fmt.Sprintf(" %03.2f%%", percent*100)
	percentage = m.PercentageStyle.Inline(true).Render(percentage)
	return percentage
}
