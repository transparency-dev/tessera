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
		m.totalBytes += b
		bytesPerSecond := float64(b) / d.Seconds()
		m.bytesAvg.Add(bytesPerSecond)

		r := msg.Status.ResourcesFetched
		m.totalResources += r
		resourcesPerSecond := float64(r) / d.Seconds()
		m.resourcesAvg.Add(resourcesPerSecond)

		e := msg.Status.ErrorsEncountered
		m.totalErrors += e
		errorsPerSecond := float64(e) / d.Seconds()
		m.errorsAvg.Add(errorsPerSecond)

		totalResources := totalSize(msg.Status.EntryRanges)
		for _, rs := range msg.Status.TileRanges {
			totalResources += totalSize(rs)
		}

		m.eta = time.Duration(float64(totalResources-m.totalResources)/resourcesPerSecond) * time.Second

		return m, nil
	default:
		return m, nil
	}
}

// totalSize returns the total number of elements covered by the provided ranges.
func totalSize(rs []fsck.Range) uint64 {
	tot := uint64(0)
	for _, r := range rs {
		tot += r.N
	}
	return tot
}

func (m *StatsViewModel) View() string {
	bytesFetched := lipgloss.NewStyle().Width(27).Render(formatTotalAndAverage("Bytes", m.totalBytes, m.bytesAvg.Avg()))
	resourcesFetched := lipgloss.NewStyle().Width(30).Render(formatTotalAndAverage("Resources", m.totalResources, m.resourcesAvg.Avg()))
	errorsEncountered := lipgloss.NewStyle().Width(23).Render(formatTotalAndAverage("Errors", m.totalErrors, m.errorsAvg.Avg()))

	eta := lipgloss.NewStyle().Width(15).Render(fmt.Sprintf("ETA: %s", m.eta))

	return lipgloss.NewStyle().
		Width(m.width).Render(
		lipgloss.JoinHorizontal(
			lipgloss.Right,
			bytesFetched,
			resourcesFetched,
			errorsEncountered,
			eta),
	)
}

func formatTotalAndAverage(label string, total uint64, avg float64) string {
	si, p := humanize.ComputeSI(float64(total))
	avgSI, avgP := humanize.ComputeSI(avg)

	return fmt.Sprintf("%s: %0.1f %s (%03.1f%s/s)", label, si, p, avgSI, avgP)
}
