package frankenphp_test

import (
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/dunglas/frankenphp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnsureBackgroundWorkerBatch ensures multiple workers in one call,
// each publishing its own identity. Verifies the batch path (array arg)
// shares one deadline across all workers.
func TestEnsureBackgroundWorkerBatch(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("worker-a", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithWorkers("worker-b", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithWorkers("worker-c", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(6),
	))
	t.Cleanup(frankenphp.Shutdown)

	req := httptest.NewRequest("GET", "http://example.com/background-worker-batch-ensure.php", nil)
	fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	require.NoError(t, err)

	w := httptest.NewRecorder()
	if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
		t.Fatalf("serve: %v", err)
	}
	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.NotContains(t, out, "MISSING", "batch ensure should have started and published all workers:\n"+out)
	assert.Contains(t, out, "worker-a=worker-a")
	assert.Contains(t, out, "worker-b=worker-b")
	assert.Contains(t, out, "worker-c=worker-c")
}

// TestEnsureBackgroundWorkerBatchEmpty verifies that an empty array is
// rejected with a clear error rather than silently succeeding.
func TestEnsureBackgroundWorkerBatchEmpty(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("bg", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	php := `<?php
try {
    frankenphp_ensure_background_worker([], 1.0);
    echo "FAIL no error";
} catch (ValueError $e) {
    echo "OK ", $e->getMessage();
}`
	tmp := testDataDir + "bg-batch-empty.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-batch-empty.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	assert.Contains(t, string(body), "OK ")
	assert.Contains(t, string(body), "must not be empty")
	assert.NotContains(t, string(body), "FAIL")
}

// TestEnsureBackgroundWorkerBatchNonString verifies array-entry type
// validation: non-string elements produce a TypeError.
func TestEnsureBackgroundWorkerBatchNonString(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("bg", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	php := `<?php
try {
    frankenphp_ensure_background_worker(['bg', 42], 1.0);
    echo "FAIL no error";
} catch (TypeError $e) {
    echo "OK ", $e->getMessage();
}`
	tmp := testDataDir + "bg-batch-nonstring.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-batch-nonstring.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	assert.Contains(t, string(body), "OK ")
	assert.Contains(t, string(body), "must contain only strings")
	assert.NotContains(t, string(body), "FAIL")
}

// TestBackgroundWorkerServerFlag confirms that a bg worker sees
// FRANKENPHP_WORKER_BACKGROUND=true alongside FRANKENPHP_WORKER_NAME in
// $_SERVER, so scripts can branch without checking every function
// independently.
func TestBackgroundWorkerServerFlag(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("flag-worker", testDataDir+"background-worker-bg-flag.php", 1,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	// ensure() removes the race between Init returning and the eager
	// bg-worker thread reaching its first set_vars.
	php := `<?php
frankenphp_ensure_background_worker('flag-worker');
$vars = frankenphp_get_vars('flag-worker');
echo 'name=', $vars['name'] ?? 'MISSING', "\n";
echo 'is_background=', var_export($vars['is_background'] ?? 'MISSING', true), "\n";
`
	tmp := testDataDir + "bg-flag-reader.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-flag-reader.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.Contains(t, out, "name=flag-worker")
	assert.Contains(t, out, "is_background=true")
}
