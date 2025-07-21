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

// This file contains code snippets used in the README.md file in this
// directory.
//
// Having this code here, in a go test file, helps ensure that our examples
// and snippets actually work with the current state of Tessera.
//
// To update the markdown files with changes from in here, run:
//   go run github.com/szkiba/mdcode@latest update
//
// TODO(al): We should probably add a presubmit test to check docs have
// actually been updated.

package tessera_test

import (
	"context"
	"crypto/rand"
	"testing"

	// #region common_imports
	"github.com/transparency-dev/tessera"

	// Choose one!
	"github.com/transparency-dev/tessera/storage/posix"
	// "github.com/transparency-dev/tessera/storage/aws"
	// "github.com/transparency-dev/tessera/storage/gcp"
	// "github.com/transparency-dev/tessera/storage/mysql"

	// #endregion
	"golang.org/x/mod/sumdb/note"
)

func constructStorage() {
	ctx := context.Background()

	// #region construct_example
	driver, _ := posix.New(ctx, posix.Config{Path: "/tmp/mylog"})
	signer := createSigner()

	appender, shutdown, reader, err := tessera.NewAppender(
		ctx, driver, tessera.NewAppendOptions().WithCheckpointSigner(signer))
	// #endregion

	// use the vars so the compiler/linter doesn't complain.
	_, _, _, _ = appender, shutdown, reader, err
}

func TestConstructStorage(t *testing.T) {
	constructStorage()
}

func constructAndUseAppender() {
	ctx := context.Background()
	data := []byte("hello")

	driver, _ := posix.New(ctx, posix.Config{Path: "/tmp/mylog"})
	signer := createSigner()

	// #region use_appender_example
	appender, shutdown, reader, err := tessera.NewAppender(
		ctx, driver, tessera.NewAppendOptions().WithCheckpointSigner(signer))
	if err != nil {
		panic(err)
	}

	index, err := appender.Add(ctx, tessera.NewEntry(data))()
	// #endregion

	// use the vars so the compiler/linter doesn't complain.
	_, _, _, _ = appender, shutdown, reader, err
	_, _ = index, err
}

func TestConstructAndUseAppender(t *testing.T) {
	constructAndUseAppender()
}

func createSigner() note.Signer {
	s, _, _ := note.GenerateKey(rand.Reader, "TestKey")
	r, err := note.NewSigner(s)
	if err != nil {
		panic(err)
	}
	return r
}
