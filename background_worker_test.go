package frankenphp_test

import (
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dunglas/frankenphp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackgroundWorker drives the minimal set_vars/get_vars path end-to-end:
// a background worker publishes three values, then an HTTP request on a
// separate thread reads them back by name.
func TestBackgroundWorker(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("bg-basic", testDataDir+"background-worker.php", 1,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(2),
	))
	t.Cleanup(frankenphp.Shutdown)

	// Give the background worker time to boot, publish, and park on the
	// stop stream. set_vars is synchronous but the first scheduling of the
	// bg worker thread is a race with Init returning, so a short wait is
	// cheaper than a ready-channel hook for this test.
	deadline := time.Now().Add(3 * time.Second)
	var out string
	for time.Now().Before(deadline) {
		req := httptest.NewRequest("GET", "http://example.com/background-worker-reader.php", nil)
		fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
		require.NoError(t, err)

		w := httptest.NewRecorder()
		if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
			t.Fatalf("serve: %v", err)
		}

		body, _ := io.ReadAll(w.Result().Body)
		out = string(body)

		if !strings.Contains(out, "MISSING") && strings.Contains(out, "has-ready-at=1") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.Contains(t, out, "message=hello from background worker")
	assert.Contains(t, out, "count=42")
	assert.Contains(t, out, "has-ready-at=1")
}

// TestBackgroundWorkerErrorPaths covers the misuse errors that don't need
// a running worker: get_vars on a nonexistent name, set_vars from outside
// a background worker, and get_worker_handle from outside a background
// worker. Runs as a non-worker request so none of the calls happen on a
// bg-worker thread.
func TestBackgroundWorkerErrorPaths(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(frankenphp.WithNumThreads(2)))
	t.Cleanup(frankenphp.Shutdown)

	req := httptest.NewRequest("GET", "http://example.com/background-worker-errors.php", nil)
	fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	require.NoError(t, err)

	w := httptest.NewRecorder()
	if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
		t.Fatalf("serve: %v", err)
	}

	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.NotContains(t, out, "FAIL", "error-path script reported a failure:\n"+out)
	assert.Contains(t, out, "OK missing:")
	assert.Contains(t, out, "OK reject-non-bg:")
	assert.Contains(t, out, "OK reject-handle:")
}

