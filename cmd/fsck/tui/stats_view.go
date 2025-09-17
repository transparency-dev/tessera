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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/transparency-dev/tessera/fsck"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	movingaverage "github.com/RobinUS2/golang-moving-average"
)

type StatsViewUpdateMsg struct {
	Status fsck.Status
}

// NewStatsView returns a widet which displays fsck statistics.
func NewStatsView() *StatsViewModel {
	m := &StatsViewModel{
		bytesAvg:     movingaverage.New(10),
		resourcesAvg: movingaverage.New(10),
		errorsAvg:    movingaverage.New(10),
	}
	return m
}

// StatsViewModel is the UI model for a stats widget.
type StatsViewModel struct {
	// width available to this component.
	width int

	totalBytes     uint64
	totalResources uint64
	totalErrors    uint64

	bytesAvg     *movingaverage.MovingAverage
	resourcesAvg *movingaverage.MovingAverage
	errorsAvg    *movingaverage.MovingAverage

	lastUpdate time.Time
	eta        time.Duration
}

func (m *StatsViewModel) Init() tea.Cmd {
	return nil
}

// Update is used to update the widget.
func (m *StatsViewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case StatsViewUpdateMsg:
		now := time.Now()
		d := now.Sub(m.lastUpdate)
		m.lastUpdate = now

		b := msg.Status.BytesFetched
		r := msg.Status.ResourcesFetched
		e := msg.Status.ErrorsEncountered
		m.totalBytes += b
		m.totalResources += r
		m.totalErrors += e
		m.bytesAvg.Add(float64(b) / d.Seconds())
		m.resourcesAvg.Add(float64(r) / d.Seconds())
		m.errorsAvg.Add(float64(e) / d.Seconds())
		return m, nil
	default:
		return m, nil
	}
}

func (m *StatsViewModel) View() string {
	// Render the pieces

	bytesFetched := lipgloss.NewStyle().Width(27).Render(fmt.Sprintf("Bytes: %s (%s/s)", humanize.Bytes(m.totalBytes), humanize.Bytes(uint64(m.bytesAvg.Avg()))))
	rSI, rP := humanize.ComputeSI(float64(m.totalResources))
	rAvgSI, rAvgP := humanize.ComputeSI(float64(m.resourcesAvg.Avg()))
	resourcesFetched := lipgloss.NewStyle().Width(28).Render(fmt.Sprintf("Resources: %03.1f %s (%03.1f%s/s)", rSI, rP, rAvgSI, rAvgP))
	eSI, eP := humanize.ComputeSI(float64(m.totalErrors))
	eAvgSI, eAvgP := humanize.ComputeSI(float64(m.errorsAvg.Avg()))
	errorsEncountered := lipgloss.NewStyle().Width(20).Render(fmt.Sprintf("Errors: %03.1f %s (%03.1f%s/s)", eSI, eP, eAvgSI, eAvgP))

	return lipgloss.NewStyle().
		Width(m.width).Render(
		lipgloss.JoinHorizontal(
			lipgloss.Right,
			bytesFetched,
			resourcesFetched,
			errorsEncountered),
	)
}
