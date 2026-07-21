// Copyright 2024 The Tessera authors. All Rights Reserved.
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

package loadtest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"log/slog"

	movingaverage "github.com/RobinUS2/golang-moving-average"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/transparency-dev/tessera/client"
)

type tuiController struct {
	hammer     *Hammer
	analyser   *HammerAnalyser
	tracker    *client.LogStateTracker
	app        *tview.Application
	statusView *tview.TextView
	logView    *tview.TextView
	helpView   *tview.TextView
}

func NewController(h *Hammer, a *HammerAnalyser, t *client.LogStateTracker) *tuiController {
	c := tuiController{
		hammer:   h,
		analyser: a,
		tracker:  t,
		app:      tview.NewApplication(),
	}
	grid := tview.NewGrid()
	grid.SetRows(5, 0, 10).SetColumns(0).SetBorders(true)

	// Top: status box
	statusView := tview.NewTextView()
	grid.AddItem(statusView, 0, 0, 1, 1, 0, 0, false)
	c.statusView = statusView

	// Middle: log view box
	logView := tview.NewTextView()
	logView.ScrollToEnd()
	logView.SetMaxLines(10000)
	grid.AddItem(logView, 1, 0, 1, 1, 0, 0, false)
	c.logView = logView

	// Bottom: help text
	helpView := tview.NewTextView()
	helpView.SetText("+/- to increase/decrease read load\n>/< to increase/decrease write load\nw/W to increase/decrease workers")
	grid.AddItem(helpView, 2, 0, 1, 1, 0, 0, false)
	c.helpView = helpView

	c.app.SetRoot(grid, true)
	return &c
}

func (c *tuiController) Run(ctx context.Context) {
	// Redirect logs to the view.
	slog.SetDefault(slog.New(slog.NewTextHandler(c.logView, &slog.HandlerOptions{Level: slog.LevelInfo})))

	go c.updateStatsLoop(ctx, 500*time.Millisecond)

	c.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if c.hammer != nil {
			switch event.Rune() {
			case '+':
				slog.InfoContext(ctx, "Increasing the read operations per second")
				c.hammer.readThrottle.Increase()
			case '-':
				slog.InfoContext(ctx, "Decreasing the read operations per second")
				c.hammer.readThrottle.Decrease()
			case '>':
				slog.InfoContext(ctx, "Increasing the write operations per second")
				c.hammer.writeThrottle.Increase()
			case '<':
				slog.InfoContext(ctx, "Decreasing the write operations per second")
				c.hammer.writeThrottle.Decrease()
			case 'w':
				slog.InfoContext(ctx, "Increasing the number of workers")
				c.hammer.randomReaders.Grow(ctx)
				c.hammer.fullReaders.Grow(ctx)
				c.hammer.writers.Grow(ctx)
			case 'W':
				slog.InfoContext(ctx, "Decreasing the number of workers")
				c.hammer.randomReaders.Shrink(ctx)
				c.hammer.fullReaders.Shrink(ctx)
				c.hammer.writers.Shrink(ctx)
			}
		}
		return event
	})
	if err := c.app.Run(); err != nil {
		panic(err)
	}
}

func (c *tuiController) updateStatsLoop(ctx context.Context, interval time.Duration) {
	formatMovingAverage := func(ma *movingaverage.ConcurrentMovingAverage) string {
		aMin, _ := ma.Min()
		aMax, _ := ma.Max()
		aAvg := ma.Avg()
		return fmt.Sprintf("%.0fms/%.0fms/%.0fms (min/avg/max)", aMin, aAvg, aMax)
	}

	ticker := time.NewTicker(interval)
	lastSize := c.tracker.Latest().Size
	maSlots := int((30 * time.Second) / interval)
	growth := movingaverage.New(maSlots)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s := c.tracker.Latest().Size
			growth.Add(float64(s - lastSize))
			lastSize = s
			qps := growth.Avg() * float64(time.Second/interval)

			var numR, numW int
			var rThrottle, wThrottle string
			if c.hammer != nil {
				numR = c.hammer.fullReaders.Size() + c.hammer.randomReaders.Size()
				rThrottle = c.hammer.readThrottle.String()
				numW = c.hammer.writers.Size()
				wThrottle = c.hammer.writeThrottle.String()
			}
			readWorkersLine := fmt.Sprintf("Read (%d workers): %s", numR, rThrottle)
			writeWorkersLine := fmt.Sprintf("Write (%d workers): %s", numW, wThrottle)
			treeSizeLine := fmt.Sprintf("TreeSize: %d (Δ %.0fqps over %ds)",
				s,
				qps,
				time.Duration(maSlots*int(interval))/time.Second)

			var qTime, iTime string
			if c.analyser != nil {
				qTime = formatMovingAverage(c.analyser.QueueTime)
				iTime = formatMovingAverage(c.analyser.IntegrationTime)
			}
			queueLine := fmt.Sprintf("Time-in-queue: %s", qTime)
			integrateLine := fmt.Sprintf("Observed-time-to-integrate: %s", iTime)
			text := strings.Join([]string{readWorkersLine, writeWorkersLine, treeSizeLine, queueLine, integrateLine}, "\n")
			c.statusView.SetText(text)
			c.app.Draw()
		}
	}
}
