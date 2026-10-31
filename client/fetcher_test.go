package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
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

// TestHTTPFetcherReleasesConnections checks that HTTPFetcher releases the
// connection after every response, so the next request can reuse it. A
// response body that is never closed keeps its connection, and the transport's
// goroutines, for as long as the request's context lives.
func TestHTTPFetcherReleasesConnections(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var conns atomic.Int64
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = fmt.Fprint(w, "body")
			}))
			srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					conns.Add(1)
				}
			}
			srv.Start()
			defer srv.Close()
			u, err := url.Parse(srv.URL + "/")
			if err != nil {
				t.Fatal(err)
			}
			f, err := NewHTTPFetcher(u, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			for range 10 {
				_, _ = f.ReadCheckpoint(t.Context())
			}
			if n := conns.Load(); n != 1 {
				t.Errorf("10 sequential requests answered %d used %d connections, want 1", status, n)
			}
		})
	}
}
