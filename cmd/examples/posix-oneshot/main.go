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

// posix-oneshot is a command line tool for adding entries to a local
// tlog-tiles log stored on a posix filesystem.
// The command takes a list of new entries to add to the log, and exits
// when they are successfully integrated.
// See the README in this package for more detailed usage instructions.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/mod/sumdb/note"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/posix"
	"k8s.io/klog/v2"
)

var (
	storageDir        = flag.String("storage_dir", "", "Root directory to store log data.")
	entries           = flag.String("entries", "", "File path glob of entries to add to the log.")
	privKeyFile       = flag.String("private_key", "", "Location of private key file. If unset, uses the contents of the LOG_PRIVATE_KEY environment variable.")
	witnessPolicyFile = flag.String("witness_policy_file", "", "(Optional) Path to the file containing the witness policy in the format describe at https://git.glasklar.is/sigsum/core/sigsum-go/-/blob/main/doc/policy.md")
	witnessTimeout    = flag.Duration("witness_timeout", tessera.DefaultWitnessTimeout, "Maximum time to wait for witness responses.")
	witnessFailOpen   = flag.Bool("witness_fail_open", false, "Still publish a checkpoint even if witness policy could not be met")
)

// entryInfo binds the actual bytes to be added as a leaf with a
// user-recognisable name for the source of those bytes.
// The name is only used below in order to inform the user of the
// sequence numbers assigned to the data from the provided input files.
type entryInfo struct {
	name string
	f    tessera.IndexFuture
}

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	klog.V(1).Infof("Initialising driver")

	// Gather the info needed for reading/writing checkpoints
	s := getSignerOrDie()
	// Construct a new Tessera POSIX log storage, anchored at the correct directory, and initialising it if requested.
	// The options provide the checkpoint signer & verifier, and batch options.
	// In this case, we want to create a single batch containing all of the leaves being added in order to
	// add all of these leaves without creating any intermediate checkpoints.
	driver, err := posix.New(
		ctx,
		posix.Config{
			Path: *storageDir,
		},
	)
	if err != nil {
		klog.Exitf("Failed to construct storage: %v", err)
	}

	klog.V(1).Infof("Reading entries")
	// Evaluate the glob provided by the --entries flag to determine the files containing leaves
	filesToAdd := readEntriesOrDie()
	batchSize := uint(len(filesToAdd))
	if batchSize == 0 {
		// batchSize can't be zero
		batchSize = 1
	}

	klog.V(1).Infof("Configuring options")
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(s).
		// Hint to Tessera the number of entries we're about to add via the batchSize parameter below,
		// this will cause the batch to flush as soon as we've called Add on the final entry.
		WithBatching(batchSize, 100*time.Millisecond).
		// We're unlikely to ever wait this long to publish a checkpoint because of the batchSize hint
		// passed in to the option above, but we set this interval low primarily such that if the user re-runs this
		// tool to add further entries to the log, they don't have to wait for the previous checkpoint to
		// become old enough to be overwritten.
		WithCheckpointInterval(100 * time.Millisecond)

	if *witnessPolicyFile != "" {
		f, err := os.ReadFile(*witnessPolicyFile)
		if err != nil {
			klog.Exitf("failed to read witness policy file %q: %v", *witnessPolicyFile, err)
		}
		wg, err := tessera.NewWitnessGroupFromPolicy(f)
		if err != nil {
			klog.Exitf("failed to create witness group from policy: %v", err)
		}

		wOpts := &tessera.WitnessOptions{
			FailOpen: *witnessFailOpen,
			Timeout:  *witnessTimeout,
		}
		opts.WithWitnesses(wg, wOpts)
	}

	klog.V(1).Infof("Creating appender")
	appender, shutdown, r, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		klog.Exit(err)
	}

	klog.V(1).Infof("Creating awaiter")
	// We don't want to exit until our entries have been integrated into the tree, so we'll use Tessera's
	// PublicationAwaiter to help with that.
	await := tessera.NewPublicationAwaiter(ctx, r.ReadCheckpoint, 100*time.Millisecond)

	klog.V(1).Infof("Adding entries")
	// Add each of the leaves in order, and store the futures in a slice
	// that we will check once all leaves are sent to storage.
	indexFutures := make([]entryInfo, 0, len(filesToAdd))
	for _, fp := range filesToAdd {
		b, err := os.ReadFile(fp)
		if err != nil {
			klog.Exitf("Failed to read entry file %q: %q", fp, err)
		}

		f := appender.Add(ctx, tessera.NewEntry(b))
		indexFutures = append(indexFutures, entryInfo{name: fp, f: f})
	}

	klog.V(1).Infof("Awaiting entries")
	// Two options to ensure all work is done:
	// 1) Check each of the futures to ensure that the leaves are sequenced.
	for _, entry := range indexFutures {
		seq, _, err := await.Await(ctx, entry.f)
		if err != nil {
			klog.Exitf("Failed to sequence %q: %q", entry.name, err)
		}
		klog.Infof("%d: %v", seq.Index, entry.name)
	}
	klog.V(1).Infof("Futures resolved")
	klog.V(1).Infof("Shutting down")

	// 2) shutdown the appender
	if err := shutdown(ctx); err != nil {
		klog.Exitf("Failed to shut down cleanly: %v", err)
	}
	klog.V(1).Infof("Finished")
}

// Read log private key from file or environment variable
func getSignerOrDie() note.Signer {
	var privKey string
	var err error
	if len(*privKeyFile) > 0 {
		privKey, err = getKeyFile(*privKeyFile)
		if err != nil {
			klog.Exitf("Unable to get private key: %q", err)
		}
	} else {
		privKey = os.Getenv("LOG_PRIVATE_KEY")
		if len(privKey) == 0 {
			klog.Exit("Supply private key file path using --private_key or set LOG_PRIVATE_KEY environment variable")
		}
	}
	s, err := note.NewSigner(privKey)
	if err != nil {
		klog.Exitf("Failed to instantiate signer: %q", err)
	}
	return s
}

func getKeyFile(path string) (string, error) {
	k, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read key file: %w", err)
	}
	return string(k), nil
}

func readEntriesOrDie() []string {
	toAdd, err := filepath.Glob(*entries)
	if err != nil {
		klog.Exitf("Failed to glob entries %q: %q", *entries, err)
	}
	klog.V(1).Infof("toAdd: %v", toAdd)
	return toAdd
}
