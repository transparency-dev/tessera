package client

import (
	"context"
	"errors"
	"testing"
)

func TestFileFetcherContextCancellation(t *testing.T) {
	d := t.TempDir()
	
	f := FileFetcher{
		Root: d,
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	
	_, err := f.ReadCheckpoint(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReadCheckpoint: got error %v, want %v", err, context.Canceled)
	}

	_, err = f.ReadTile(ctx, 0, 0, 255)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReadTile: got error %v, want %v", err, context.Canceled)
	}

	_, err = f.ReadEntryBundle(ctx, 0, 255)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ReadEntryBundle: got error %v, want %v", err, context.Canceled)
	}
}
