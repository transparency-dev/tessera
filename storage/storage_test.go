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

package storage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The layout.EntriesPath function should never been called by storage implementations.
// They should use `tessera.AppendOptions.EntriesPath` instead.
func TestForbiddenFunction(t *testing.T) {
	rootDir := "."
	forbiddenName := "layout.EntriesPath"

	err := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Only check Go files
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}

		// Skip test files to avoid false positives (or specifically this file)
		if strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}

		// Read the file content
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Check for the forbidden string
		if strings.Contains(string(content), forbiddenName) {
			t.Errorf("Found forbidden call %q in file %q", forbiddenName, path)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Error walking the path %q: %v", rootDir, err)
	}
}
