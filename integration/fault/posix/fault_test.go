// Copyright 2025 The Tessera Authors. All Rights Reserved.
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

//go:build linux

// Package fault_test contains a (probably Linux-specific) test which uses `strace` to inject faults
// into the Tessera posix-oneshot binary to verify that this does not result in an inconsistent or corrupt
// tree state stored on disk.
package fault_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bitfield/script"
	"github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/fsck"
	"k8s.io/klog/v2"
)

const (
	testSigner   = "PRIVATE+KEY+example.com/fault_test+7f84ee58+AajLvPzbzMafNfiFvmRw1YlP7del7DYc1cr7Yntjf8yP"
	testVerifier = "example.com/fault_test+7f84ee58+AfMY9QKMQjVcokKlKSGRvcLL8YwvpDHua/r0e64EfFIX"

	treeStateFile = "/.state/treeState"
)

var (
	// signerPath will be set to a path to a file containing a note signer key.
	signerPath string
	// verifierPath will be set to a path to a file containing a note verifier key corresponding to the signer key above.
	verifierPath string
	// oneshotPath will be set to a path to a compiled posix-oneshot binary.
	oneshotPath string

	// interestingSyscalls lists the syscalls ultimately used by posix-oneshot to manipulate the state of the log on disk.
	interestingSyscalls = []string{"openat", "linkat", "mkdirat", "renameat", "fsync", "close", "read", "write"}
)

// runFaultInjectionTest is a test which tries to cause tree corruption by injecting faults into syscalls which are
// ultimately used by the posix-oneshot application.
//
// This test builds and execs the `/cmd/examples/posix-oneshot` tool in order to manage some logs-under-test, and
// uses `strace` to inspect its operation at the syscall level and to inject failures for specific calls.
//
// Broadly, it works as follows:
//  1. use the posix-oneshot tool to create a "base" log, which will be used as an ancestor for all logs created during the test.
//  2. create a copy of the base log, and use `strace` to inspect posix-oneshot as it adds a new entry to this copy.
//  3. analyse the output from `strace` to identify a list of "interesting" (i.e. file operation related) syscalls which occurred while
//     posix-oneshat was actively managing/updating the log on disk.
//  4. for each individual interesting syscall:
//     4.1. create a new copy of the base log
//     4.2. invoke posix-oneshot using `strace`'s `-e inject=' flag to inject the provided fault for a single specific instance of a syscall.
//     We don't particularly care whether this fault causes posix-oneshot to exit or retry; what's important is that the on-disk
//     representation of the log state is never left in an inconsistent or corrupt state.
//     4.3. run `fsck` against the log-under-test to assert that the on-disk state is, indeed, self-consistent and uncorrupted.
func runFaultInjectionTest(t *testing.T, inject string) {
	tmp := t.TempDir()
	setup(t, tmp)
	baseDir := filepath.Join(tmp, "base")
	_ = os.MkdirAll(baseDir, 0o755)

	// Set up base log:
	{
		mustWrite(t, filepath.Join(baseDir, "base-0"), []byte("base first"))
		p := script.Exec(growLogCommand(baseDir, filepath.Join(baseDir, "base-0")))
		if output, err := p.String(); err != nil {
			t.Fatalf("Failed to create base log: %v\n%s", err, output)
		}
	}

	// Create an isolated "descendent" log which we'll use to inspect the syscalls being used by posix-oneshot while
	// updating it:
	count, processFn := processStraceOutput(interestingSyscalls)
	{
		traceDir := filepath.Join(tmp, "trace")
		_ = os.MkdirAll(traceDir, 0o755)
		if output, err := script.Exec(fmt.Sprintf("cp -r %s/. %s/", baseDir, traceDir)).String(); err != nil {
			t.Fatalf("Failed to copy base log into trace directory %q: %v\n%s", traceDir, err, output)
		}
		mustWrite(t, filepath.Join(traceDir, "trace-0"), []byte("trace first"))
		traceRun := script.Exec(fmt.Sprintf("strace -f -e trace=%s -e quiet=all %s", strings.Join(interestingSyscalls, ","), growLogCommand(traceDir, filepath.Join(traceDir, "trace-0"))))
		traceRun.FilterScan(processFn)
		o, err := traceRun.String()
		if err != nil {
			t.Errorf("traceRun failed: %v\n%s", err, o)
		}

		t.Logf("Syscall counts: %v", count)
	}

	// Now perform the test proper.
	// For each individual invocation of each individual syscall used by posix-oneshot, create a new copy of the base tree and
	// use `strace` to inject a fault into an invocation of the posix-oneshot command which attempts to add an entry to that copy.
	for sc, c := range count {
		for i := range c.N {
			i := c.First + i
			t.Run(fmt.Sprintf("%s-on-%s#%d", inject, sc, i), func(t *testing.T) {
				t.Parallel()

				// Fork the base log into a new directory.
				injectDir := filepath.Join(tmp, fmt.Sprintf("inject-%s-%d", sc, i))
				_ = os.MkdirAll(injectDir, 0o755)
				if output, err := script.Exec(fmt.Sprintf("cp -r %s/. %s/", baseDir, injectDir)).String(); err != nil {
					t.Fatalf("Failed to copy base log into directory %q: %v\n%s", injectDir, err, output)
				}

				// Now run posix-oneshot to add an entry, using strace to inject a fault on the ith call to the syscall named in sc:
				// We don't really care whether this results in an error or not, but it _MUST_ be the case that the on-disk tree is
				// left in a self-consistent state.
				mustWrite(t, filepath.Join(injectDir, "inject-0"), fmt.Appendf(nil, "inject-%d first", i))
				cmd := fmt.Sprintf("strace -f -e inject=%s:%s:when=%d -e quiet=all %s", sc, inject, i, growLogCommand(injectDir, filepath.Join(injectDir, "inject-0")))
				if _, err := script.Exec(cmd).String(); err != nil {
					t.Logf("Failed to growLog on %s: %v", injectDir, err)
				}

				// Verify that the tree on disk is self-consistent and uncorrupted by running fsck against it.
				// Any error here is bad news.
				if err := fsckLog(t, injectDir); err != nil {
					t.Errorf("Fsck on %s failed: %v", injectDir, err)
				}
			})
		}
	}
}

func TestInjectIOError(t *testing.T) {
	runFaultInjectionTest(t, "error=EIO")
}

func TestInjectSigKill(t *testing.T) {
	runFaultInjectionTest(t, "signal=KILL")
}

// setup configures everything necessary for running the fault test.
//
// This includes building the oneshot-posix binary, writing out test keys,
// and figuring out paths.
func setup(t *testing.T, outDir string) {
	t.Helper()

	oneshotPath = filepath.Join(outDir, "posix-oneshot")
	signerPath = filepath.Join(outDir, "test.sec")
	verifierPath = filepath.Join(outDir, "test.pub")

	if o, err := script.Exec(fmt.Sprintf("go build -o %s ../../../cmd/examples/posix-oneshot", oneshotPath)).String(); err != nil {
		t.Fatalf("Failed to build posix-oneshot: %v\n%s", err, o)
	}

	if err := os.WriteFile(signerPath, []byte(testSigner), 0o644); err != nil {
		t.Fatalf("Failed to write %s: %v", signerPath, err)
	}
	if err := os.WriteFile(verifierPath, []byte(testVerifier), 0o644); err != nil {
		t.Fatalf("Failed to write %s: %v", verifierPath, err)
	}
}

// growLogCommand returns a formatted shell command for executing posix-oneshot on a log stored at the provided location
// to integrate the provided new entry files.
func growLogCommand(storagePath string, entriesGlob string) string {
	return fmt.Sprintf("%s --private_key=%s --storage_dir=%s --entries=%s", oneshotPath, signerPath, storagePath, entriesGlob)
}

// calls represents a range of invocations of a particular syscall.
type calls struct {
	// First is the index of the first "interesting" use of the corresponding syscall - i.e. it occurred
	// while posix-oneshot was actually modifying/processing the log.
	First int
	// N is the number of invocations of the syscall which occurred during the period posix-oneshot was
	// modifying/processing the log.
	N int
}

// processStraceOutput reads the output from the strace command being used to inspect the goings-on
// inside posix-oneshot.
//
// This function returns a map which contains information about calls to the list of provided
// "interesting" syscalls, and a function which can be used to filter the output down to a heuristically
// determined subset for human-inspection purposes.
//
// The syscalls we're interested in are pretty common (e.g. Go's runtime wants to read a bunch of
// files/devices when we first start a compiled Go program, posix-oneshot wants to read keys from
// disk, etc.), so we attempt to pare down the _actually_ interesting calls by ignoring anything
// which happens before execution has reached the "actual" posix-oneshot application code. We do this by
// looking for the very first syscall referencing the ".state/treeState" file - this is tessera opening the
// tree.
func processStraceOutput(syscalls []string) (map[string]calls, func(string, io.Writer)) {
	// m is a map of syscall name to information about its invocations.
	m := make(map[string]calls)
	// oneshotStarted will be false until we've seen the string "/.state/treeState" in one of the syscalls.
	// we use this to determine whether syscalls should be skipped or not.
	oneshotStarted := false
	return m, func(line string, w io.Writer) {
		// When we see the treeStateFile referenced by a syscall we know posix-oneshot has started up
		// and is now about to perform the work on the log.
		if strings.Contains(line, treeStateFile) {
			oneshotStarted = true
		}
		// If we're in the interesting part of the strace output, then pass through the log line, otherwise
		// just drop it.
		if oneshotStarted {
			_, _ = fmt.Fprintf(w, "%s\n", line)
		}
		// Check the stract log line against our list of interesting syscalls and update the info in
		// our map.
		for _, s := range syscalls {
			if strings.Contains(line, s) {
				c := m[s]
				if !oneshotStarted {
					// oneshot still hasn't fully started up, so we're going to skip all the early calls to this
					// syscall.
					c.First++
				} else {
					// oneshot is working on the log, so count this syscall invocation as being a candidate for
					// fault injection.
					c.N++
				}
				m[s] = c
			}
		}
	}
}

// fsckLog runs fsck on the log rooted at the provided path.
func fsckLog(t *testing.T, dir string) error {
	t.Helper()

	src := &client.FileFetcher{
		Root: dir,
	}
	v, err := note.NewVerifier(testVerifier)
	if err != nil {
		klog.Exitf("Invalid verifier in %q: %v", testVerifier, err)
	}
	return fsck.Check(t.Context(), v.Name(), v, src, 1, defaultMerkleLeafHasher)
}

// defaultMerkleLeafHasher parses a C2SP tlog-tile bundle and returns the Merkle leaf hashes of each entry it contains.
func defaultMerkleLeafHasher(bundle []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bundle); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	r := make([][]byte, 0, len(eb.Entries))
	for _, e := range eb.Entries {
		h := rfc6962.DefaultHasher.HashLeaf(e)
		r = append(r, h[:])
	}
	return r, nil
}

// mustWrite writes the provided contents to a file at the provided path, or causes a fatal test failure.
func mustWrite(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatalf("Failed to write %q: %v", path, err)
	}
}
