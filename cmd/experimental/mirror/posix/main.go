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

// mirror/posix is a command-line tool for mirroring a tlog-tiles compliant log
// into a POSIX filesystem.
package main

import (
	"context"
	"flag"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	mirror "github.com/transparency-dev/tessera/cmd/experimental/mirror/internal"
)

var (
	storageDir = flag.String("storage_dir", "", "Root directory to store log data.")
	sourceURL  = flag.String("source_url", "", "Base URL for the source log.")
	numWorkers = flag.Uint("num_workers", 30, "Number of migration worker goroutines.")
	slogLevel  = flag.Int("slog_level", 0, "The cut-off threshold for structured logging. Default is 0 (INFO). See https://pkg.go.dev/log/slog#Level for other levels.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.Level(*slogLevel)})))

	srcURL, err := url.Parse(*sourceURL)
	if err != nil {
		slog.Error("Invalid --source_url", slog.String("param", *sourceURL), slog.Any("error", err))
		os.Exit(1)
	}
	src, err := client.NewHTTPFetcher(srcURL, nil)
	if err != nil {
		slog.Error("Failed to create HTTP fetcher", slog.Any("error", err))
		os.Exit(1)
	}

	m := &mirror.Mirror{
		NumWorkers: *numWorkers,
		Source:     src,
		Target:     &posixTarget{root: *storageDir},
	}

	// Print out stats.
	go func() {
		t := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				printProgress(m.Progress)
			}
		}
	}()

	if err := m.Run(ctx); err != nil {
		slog.Error("Failed to mirror log", slog.Any("error", err))
		os.Exit(1)
	}

	printProgress(m.Progress)
	slog.Info("Log mirrored successfully.")
}

func printProgress(f func() (uint64, uint64)) {
	total, done := f()
	p := float64(done*100) / float64(total)
	// Let's just say we're 100% done if we've completed no work when nothing needed doing.
	if total == done && done == 0 {
		p = 100.0
	}
	slog.Info("Progress", slog.Uint64("done", done), slog.Uint64("total", total), slog.Float64("percent", p))
}

type posixTarget struct {
	root string
}

func (s *posixTarget) ReadCheckpoint(_ context.Context) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.root, layout.CheckpointPath))
}

func (s *posixTarget) WriteCheckpoint(_ context.Context, d []byte) error {
	return s.store(layout.CheckpointPath, d)
}

func (s *posixTarget) WriteTile(_ context.Context, l, i uint64, p uint8, d []byte) error {
	return s.store(layout.TilePath(l, i, p), d)
}

func (s *posixTarget) WriteEntryBundle(_ context.Context, i uint64, p uint8, d []byte) error {
	return s.store(layout.EntriesPath(i, p), d)
}

func (s *posixTarget) store(p string, d []byte) (err error) {
	fp := filepath.Join(s.root, p)
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(fp, d, 0o644); err != nil {
		return err
	}
	return nil
}
