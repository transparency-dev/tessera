// Copyright 2026 The Tessera authors. All Rights Reserved.
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

package tessera

import "context"

type fakeLogReader struct {
	readCheckpoint  func(context.Context) ([]byte, error)
	readTile        func(context.Context, uint64, uint64, uint8) ([]byte, error)
	readEntryBundle func(context.Context, uint64, uint8) ([]byte, error)
	sizeFunc        func(context.Context) (uint64, error)
	nextIndex       func(context.Context) (uint64, error)
}

func (f *fakeLogReader) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	if f.readCheckpoint != nil {
		return f.readCheckpoint(ctx)
	}
	return nil, nil
}

func (f *fakeLogReader) ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
	if f.readTile != nil {
		return f.readTile(ctx, level, index, p)
	}
	return nil, nil
}

func (f *fakeLogReader) ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error) {
	if f.readEntryBundle != nil {
		return f.readEntryBundle(ctx, index, p)
	}
	return nil, nil
}

func (f *fakeLogReader) IntegratedSize(ctx context.Context) (uint64, error) {
	if f.sizeFunc != nil {
		return f.sizeFunc(ctx)
	}
	return 0, nil
}

func (f *fakeLogReader) NextIndex(ctx context.Context) (uint64, error) {
	if f.nextIndex != nil {
		return f.nextIndex(ctx)
	}
	return 0, nil
}
