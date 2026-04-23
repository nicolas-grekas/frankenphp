package frankenphp_test

import (
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/dunglas/frankenphp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnsureBackgroundWorkerNamedLazy drives ensure() against a declared
// named worker with num=0 (lazy). First request lazy-starts it; set_vars
// publishes; get_vars reads the published vars.
func TestEnsureBackgroundWorkerNamedLazy(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("bg-lazy", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	req := httptest.NewRequest("GET", "http://example.com/background-worker-ensure-from-handler.php", nil)
	fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	require.NoError(t, err)

	w := httptest.NewRecorder()
	if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
		t.Fatalf("serve: %v", err)
	}

	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.NotContains(t, out, "MISSING", "ensure() should have lazy-started the worker and published vars:\n"+out)
	assert.Contains(t, out, "ensured-name=bg-lazy")
}

// TestEnsureBackgroundWorkerCatchAll declares a single catch-all (no name)
// and invokes ensure() twice with distinct names. Each name should start
// its own instance from the same entrypoint and publish its own vars.
func TestEnsureBackgroundWorkerCatchAll(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		// Name-less bg worker = catch-all. max_threads on a catch-all is
		// the cap on lazy-started instances; it also drives the thread
		// budget that calculateMaxThreads reserves for the catch-all.
		frankenphp.WithWorkers("", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground(),
			frankenphp.WithWorkerMaxThreads(4)),
		frankenphp.WithNumThreads(5),
	))
	t.Cleanup(frankenphp.Shutdown)

	for _, name := range []string{"job-a", "job-b"} {
		req := httptest.NewRequest("GET", "http://example.com/background-worker-ensure-reader.php?name="+name, nil)
		fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
		require.NoError(t, err)

		w := httptest.NewRecorder()
		if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
			t.Fatalf("serve: %v", err)
		}
		body, _ := io.ReadAll(w.Result().Body)
		out := string(body)
		assert.Contains(t, out, "name="+name, "catch-all instance %s did not publish its name:\n%s", name, out)
	}
}

// TestEnsureBackgroundWorkerCatchAllCap sets max_threads on a catch-all so
// the third distinct name ensure() hits the cap error.
func TestEnsureBackgroundWorkerCatchAllCap(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground(),
			frankenphp.WithWorkerMaxThreads(2)),
		frankenphp.WithNumThreads(5),
	))
	t.Cleanup(frankenphp.Shutdown)

	for _, name := range []string{"cap-a", "cap-b"} {
		req := httptest.NewRequest("GET", "http://example.com/background-worker-ensure-reader.php?name="+name, nil)
		fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
		w := httptest.NewRecorder()
		_ = frankenphp.ServeHTTP(w, fr)
		body, _ := io.ReadAll(w.Result().Body)
		require.NotContains(t, string(body), "limit of", "first two ensures should succeed, got:\n"+string(body))
	}

	// Third should fail with a cap error.
	req := httptest.NewRequest("GET", "http://example.com/background-worker-ensure-reader.php?name=cap-c", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	assert.Contains(t, string(body), "limit of 2 reached", "third ensure must hit the catch-all cap:\n"+string(body))
}

// TestEnsureBackgroundWorkerUndeclared checks that ensure() on a name that
// is neither declared nor covered by a catch-all returns an error.
func TestEnsureBackgroundWorkerUndeclared(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("bg-lazy", testDataDir+"background-worker-named.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(2),
	))
	t.Cleanup(frankenphp.Shutdown)

	// Script tries to ensure('other-name') which is neither named nor catch-all.
	php := `<?php
try {
    frankenphp_ensure_background_worker('other-name', 2.0);
    echo "FAIL no error";
} catch (RuntimeException $e) {
    echo "OK ", $e->getMessage();
}`
	tmp := testDataDir + "bg-ensure-undeclared.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-ensure-undeclared.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	assert.Contains(t, string(body), "OK no background worker configured for name", "ensure of undeclared name should error:\n"+string(body))
	assert.NotContains(t, string(body), "FAIL")
	_ = fmt.Sprintf // keep fmt imported for potential future asserts

	_ = strings.TrimSpace // keep strings imported
}

// TestBackgroundWorkerBootFailureError confirms that an entrypoint which
// throws during boot surfaces the captured details through ensure()'s
// timeout error message: entrypoint path, attempt count, and the PHP
// RuntimeException message. Runs as a non-worker request so ensure uses
// the tolerant lazy-start path (no fail-fast).
func TestBackgroundWorkerBootFailureError(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("boot-fail-worker", testDataDir+"background-worker-boot-fail.php", 0,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	php := `<?php
try {
    frankenphp_ensure_background_worker('boot-fail-worker', 1.0);
    echo "FAIL no error";
} catch (\RuntimeException $e) {
    echo $e->getMessage();
}`
	tmp := testDataDir + "bg-boot-fail.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-boot-fail.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.NotContains(t, out, "FAIL", "ensure should have thrown:\n"+out)
	assert.Contains(t, out, `"boot-fail-worker"`)
	assert.Contains(t, out, "background-worker-boot-fail.php", "entrypoint path must appear in the error:\n"+out)
	assert.Contains(t, out, "attempt", "attempt count must appear:\n"+out)
	assert.Contains(t, out, "intentional boot failure for test", "PHP exception message must be captured:\n"+out)
}
