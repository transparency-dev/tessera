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

import "context"

type Worker interface {
	Run(ctx context.Context)
	Kill()
}

// NewWorkerPool creates a simple pool of workers.
//
// This works well enough for the simple task we ask of it at the moment.
// If we find ourselves adding more features to this, consider swapping it
// for a library such as https://github.com/alitto/pond.
func NewWorkerPool(factory func() Worker) WorkerPool {
	workers := make([]Worker, 0)
	pool := WorkerPool{
		workers: workers,
		factory: factory,
	}
	return pool
}

// WorkerPool contains a collection of _running_ workers.
type WorkerPool struct {
	workers []Worker
	factory func() Worker
}

func (p *WorkerPool) Grow(ctx context.Context) {
	w := p.factory()
	p.workers = append(p.workers, w)
	go w.Run(ctx)
}

func (p *WorkerPool) Shrink(ctx context.Context) {
	if len(p.workers) == 0 {
		return
	}
	w := p.workers[len(p.workers)-1]
	p.workers = p.workers[:len(p.workers)-1]
	w.Kill()
}

func (p *WorkerPool) Size() int {
	return len(p.workers)
}
