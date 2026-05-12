package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
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

func TestHTTPFetcherRetry(t *testing.T) {
	tests := []struct {
		name          string
		responses     []int
		retryAfter    string
		expectedError error
		wantAttempts  int
		minDuration   time.Duration
	}{
		{
			name:         "SuccessFirstTry",
			responses:    []int{http.StatusOK},
			wantAttempts: 1,
		},
		{
			name:         "RetryThenSuccess",
			responses:    []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK},
			wantAttempts: 3,
		},
		{
			name:          "MaxRetriesExceeded",
			responses:     []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
			expectedError: errors.New("after 5 retries"),
			wantAttempts:  6,
		},
		{
			name:          "NotFoundNoRetry",
			responses:     []int{http.StatusNotFound},
			expectedError: os.ErrNotExist,
			wantAttempts:  1,
		},
		{
			name:         "RetryAfterRespected",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "1", // 1 second
			wantAttempts: 2,
			minDuration:  time.Second,
		},
		{
			name:         "RetryAfterPastDate",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "Wed, 21 Oct 2015 07:28:00 GMT",
			wantAttempts: 2,
			minDuration:  0,
		},
		{
			name:         "RetryAfterZero",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "0",
			wantAttempts: 2,
			minDuration:  0,
		},
		{
			name:         "RetryAfterRFC850",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "Sunday, 06-Nov-94 08:49:37 GMT",
			wantAttempts: 2,
			minDuration:  0,
		},
		{
			name:         "RetryAfterANSIC",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "Sun Nov  6 08:49:37 1994",
			wantAttempts: 2,
			minDuration:  0,
		},
		{
			name:         "RetryAfterLargeCapped",
			responses:    []int{http.StatusTooManyRequests, http.StatusOK},
			retryAfter:   "3600", // 1 hour
			wantAttempts: 2,
			minDuration:  2 * time.Second, // Capped at maxBackoff (2s)
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts < len(tc.responses) {
					status := tc.responses[attempts]
					attempts++
					if tc.retryAfter != "" && status == http.StatusTooManyRequests {
						w.Header().Set("Retry-After", tc.retryAfter)
					}
					w.WriteHeader(status)
					if status == http.StatusOK {
						_, _ = w.Write([]byte("data"))
					}
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()

			u, _ := url.Parse(server.URL)
			fetcher, err := NewHTTPFetcher(u, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Decorate the fetch call using the helper
			decoratedFetch := func(ctx context.Context) ([]byte, error) {
				return retry(ctx, defaultRetryOpts(), func() ([]byte, error) {
					return fetcher.fetch(ctx, "/")
				})
			}

			ctx := context.Background()
			
			startTime := time.Now()
			_, err = decoratedFetch(ctx)
			duration := time.Since(startTime)

			if tc.expectedError != nil {
				if err == nil {
					t.Errorf("expected error %v, got nil", tc.expectedError)
				} else if !errors.Is(err, tc.expectedError) && !strings.Contains(err.Error(), tc.expectedError.Error()) {
					t.Errorf("expected error containing %v, got %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if attempts != tc.wantAttempts {
				t.Errorf("got %d attempts, want %d", attempts, tc.wantAttempts)
			}

			if tc.minDuration > 0 && duration < tc.minDuration {
				t.Errorf("expected retry delay of at least %v, took %v", tc.minDuration, duration)
			}
		})
	}
}

func TestWithTileRetry(t *testing.T) {
	tests := []struct {
		name          string
		responses     []error
		options       []RetryOption
		expectedError error
		wantAttempts  int
	}{
		{
			name:         "SuccessFirstTry",
			responses:    []error{nil},
			wantAttempts: 1,
		},
		{
			name:         "RetryThenSuccess",
			responses:    []error{TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, nil},
			wantAttempts: 3,
		},
		{
			name:          "MaxRetriesExceeded",
			responses:     []error{TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}},
			expectedError: errors.New("after 5 retries"),
			wantAttempts:  6,
		},
		{
			name:          "NonTransientErrorNoRetry",
			responses:     []error{errors.New("fatal")},
			expectedError: errors.New("fatal"),
			wantAttempts:  1,
		},
		{
			name:          "CustomMaxRetries",
			responses:     []error{TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}, TransientError{Err: errors.New("temporary")}},
			options:       []RetryOption{WithMaxRetries(2)},
			expectedError: errors.New("after 2 retries"),
			wantAttempts:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			dummyFetcher := func(ctx context.Context, level, index uint64, p uint8) ([]byte, error) {
				if attempts < len(tc.responses) {
					err := tc.responses[attempts]
					attempts++
					if err != nil {
						return nil, err
					}
					return []byte("data"), nil
				}
				return nil, errors.New("unexpected call")
			}

			decorated := WithTileRetry(dummyFetcher, tc.options...)

			_, err := decorated(context.Background(), 0, 0, 0)

			if tc.expectedError != nil {
				if err == nil {
					t.Errorf("expected error %v, got nil", tc.expectedError)
				} else if !strings.Contains(err.Error(), tc.expectedError.Error()) {
					t.Errorf("expected error containing %v, got %v", tc.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if attempts != tc.wantAttempts {
				t.Errorf("got %d attempts, want %d", attempts, tc.wantAttempts)
			}
		})
	}
}

type myNetError struct {
	timeout bool
}

func (e myNetError) Error() string   { return "error" }
func (e myNetError) Timeout() bool   { return e.timeout }
func (e myNetError) Temporary() bool { return false }

func TestIsTransientNetworkError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "NilError",
			err:  nil,
			want: false,
		},
		{
			name: "GenericError",
			err:  errors.New("generic error"),
			want: false,
		},
		{
			name: "TimeoutError",
			err:  myNetError{timeout: true},
			want: true,
		},
		{
			name: "NonTimeoutNetError",
			err:  myNetError{timeout: false},
			want: false,
		},
		{
			name: "UnexpectedEOF",
			err:  io.ErrUnexpectedEOF,
			want: true,
		},
		{
			name: "EOF",
			err:  io.EOF,
			want: true,
		},
		{
			name: "ConnReset",
			err:  syscall.ECONNRESET,
			want: true,
		},
		{
			name: "ConnRefused",
			err:  syscall.ECONNREFUSED,
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientNetworkError(tc.err); got != tc.want {
				t.Errorf("isTransientNetworkError() = %v, want %v", got, tc.want)
			}
		})
	}
}
