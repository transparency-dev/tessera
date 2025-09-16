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
	// percentageStyle defines the layout for the progress bar percentage.
	percentageStyle = lipgloss.NewStyle().Width(8).MaxWidth(8)

	// labelStyle defines the layout for the progress bar label.
	labelStyle = lipgloss.NewStyle().Width(16).MaxWidth(16)

	// stateStyle defines how the different fsck.State types show up in the progress bar.
	stateStyles = []stateStyle{
		{
			state:    fsck.Unknown,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#313244")),
			bar:      "◻",
			priority: 1,
		}, {
			state:    fsck.OK,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#86a381")),
			bar:      "◼",
			priority: 2,
		}, {
			state:    fsck.Calculating,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#89dceb")),
			bar:      "C",
			priority: 3,
		}, {
			state:    fsck.Fetched,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#94e2d5")),
			bar:      "◼",
			priority: 4,
		}, {
			state:    fsck.Fetching,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#eeeeee")),
			bar:      "◼",
			priority: 5,
		}, {
			state:    fsck.FetchError,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#fab387")),
			bar:      "E",
			priority: 9,
		}, {
			state:    fsck.Invalid,
			style:    lipgloss.NewStyle().Foreground(lipgloss.Color("#f38b38")),
			bar:      "!",
			priority: 10,
		},
	}

	// stateStylesByState maps styles to their corresponding fsck.State.
	stateStylesByState = func() map[fsck.State]stateStyle {
		r := make(map[fsck.State]stateStyle)
		for _, v := range stateStyles {
			r[v.state] = v
		}
		return r
	}()
)

// LayerProgressKey returns a rendered "key" which can be used in the UI to visually explain
// to the user which fsck state is associated with the various styles.
func LayerProgressKey() string {
	r := make([]string, 0, len(stateStyles))
	for _, s := range stateStyles {
		r = append(r, fmt.Sprintf("%s %s", s.Render(), s.state.String()))
	}
	return strings.Join(r, " | ")
}

type stateStyle struct {
	state fsck.State
	// style is the style to use for this state in the progress bar.
	style lipgloss.Style
	// bar is the character(s) used for representing this state in the progress bar.
	bar string
	// priority maps each possible state to a relative importance of that priority.
	// This is used when displaying the overall state of a range of resources where multiple
	// states are present.
	// The highest priority state represented in the range "wins".
	priority int
}

// Render returns a string representing a single segment of a LayerProgressBar, styled
// appropriately for the state.
func (s stateStyle) Render() string {
	return s.style.Inline(true).Render(s.bar)
}

// LayerUpdateMsg is a message which carries information for a specific layer in the tree (e.g. tiles for a specific level).
type LayerUpdateMsg struct {
	Ranges []fsck.Range
}

// NewLayerProgressBar returns a progress bar which can render information about the states of fsck ranges.
//
// Label is the name of the range, and will be shown in the UI.
// width is the total width available to this model.
func NewLayerProgressBar(label string, width int, level int) *LayerProgressModel {
	m := &LayerProgressModel{
		label:        label,
		width:        width,
		level:        level,
		colorProfile: termenv.ColorProfile(),
	}
	return m
}

// LayerProgressModel is the UI model for a progress bar which represents fsck status through a paricular level of the tree.
type LayerProgressModel struct {
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

func (m *LayerProgressModel) Init() tea.Cmd {
	return nil
}

// Update is used to animate the progress bar during transitions. Use
// SetPercent to create the command you'll need to trigger the animation.
//
// If you're rendering with ViewAs you won't need this.
func (m *LayerProgressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case LayerUpdateMsg:
		m.state = msg.Ranges
		return m, nil
	default:
		return m, nil
	}
}

func (m *LayerProgressModel) View() string {
	if m.state == nil {
		return ""
	}
	return m.ViewAs(m.state)
}

func (m *LayerProgressModel) ViewAs(rs []fsck.Range) string {
	// Don't bother trying to show empty information.
	if len(rs) == 0 {
		return ""
	}

	extent := rs[len(rs)-1].First + rs[len(rs)-1].N
	byState := make(map[fsck.State]uint64)
	for _, r := range rs {
		byState[r.State] += r.N
	}

	// Render the pieces
	percentView := m.percentageView(float64(byState[fsck.OK]) / float64(extent))
	labelView := labelStyle.Inline(true).Render(m.label)
	barWidth := m.width - ansi.StringWidth(labelView) - 1 - ansi.StringWidth(percentView) - 1
	// Squash higher levels so the bars look a bit more tree-like.
	levelSize := max(1, barWidth>>m.level)
	barView := lipgloss.NewStyle().Width(barWidth).MaxWidth(barWidth).Align(lipgloss.Center).Inline(true).Render(renderBar(rs, levelSize))

	return lipgloss.JoinHorizontal(
		lipgloss.Left,
		labelView,
		barView,
		percentView)
}

// stateForRange figures out the right state style to use for the progress bar section covering range [f, f+n),
// using the provided fsck status ranges.
func stateForRange(rs []fsck.Range, f, n uint64) stateStyle {
	ret := stateStylesByState[fsck.Unknown]
	found := false
	for _, r := range rs {
		if r.First <= f && f < r.First+r.N {
			found = true
		}
		if found {
			s := stateStylesByState[r.State]
			if s.priority > ret.priority {
				ret = s
			}
		}
		if r.First+r.N > f+n {
			break
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
		_, _ = sb.WriteString(stateForRange(r, uint64(math.Round(rFirst)), uint64(math.Round(chunk))).Render())
		rFirst += chunk
	}
	return sb.String()
}

// percentageView returns a rendering of the provided percentage value.
func (m *LayerProgressModel) percentageView(percent float64) string {
	percent = math.Max(0, math.Min(1, percent))
	percentage := fmt.Sprintf(" %03.2f%%", percent*100)
	return percentageStyle.Inline(true).Render(percentage)
}
