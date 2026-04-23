package frankenphp_test

import (
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/dunglas/frankenphp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetVarsCacheIdentity verifies that two get_vars calls within one
// request return the *same* zval (pointer identity via ===) when the
// worker hasn't published a new version in between. This is the user-
// visible guarantee that proves the per-request cache is wired.
func TestGetVarsCacheIdentity(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("cache-worker", testDataDir+"background-worker-cache-fixture.php", 1,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	req := httptest.NewRequest("GET", "http://example.com/background-worker-cache-identity.php", nil)
	fr, err := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	require.NoError(t, err)

	w := httptest.NewRecorder()
	if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
		t.Fatalf("serve: %v", err)
	}
	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.Contains(t, out, "first=cached-value")
	assert.Contains(t, out, "second=cached-value")
	assert.Contains(t, out, "identical=true", "cached zvals must be === across repeated reads:\n"+out)
}

// TestGetVarsCacheManyReads exercises the cache path under load: one
// request calls get_vars 500 times against a nested-array worker. The
// second call onward is a cache hit; the test just asserts the script
// completes without corruption.
func TestGetVarsCacheManyReads(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("cache-worker", testDataDir+"background-worker-cache-fixture.php", 1,
			frankenphp.WithWorkerBackground()),
		frankenphp.WithNumThreads(3),
	))
	t.Cleanup(frankenphp.Shutdown)

	// ensure() first so the eager-start race doesn't surface before the
	// 500-read loop even begins.
	php := `<?php
frankenphp_ensure_background_worker('cache-worker');
for ($i = 0; $i < 500; $i++) {
    $vars = frankenphp_get_vars('cache-worker');
}
echo 'ok=', $vars['marker'] ?? 'MISSING', "\n";`
	tmp := testDataDir + "bg-cache-many.php"
	require.NoError(t, os.WriteFile(tmp, []byte(php), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	req := httptest.NewRequest("GET", "http://example.com/bg-cache-many.php", nil)
	fr, _ := frankenphp.NewRequestWithContext(req, frankenphp.WithRequestDocumentRoot(testDataDir, false))
	w := httptest.NewRecorder()
	_ = frankenphp.ServeHTTP(w, fr)
	body, _ := io.ReadAll(w.Result().Body)
	out := string(body)

	assert.Contains(t, out, "ok=cached-value", "500 cached reads should all succeed:\n"+out)
	assert.False(t, strings.Contains(out, "Fatal error") || strings.Contains(out, "corrupted"), "no corruption expected:\n"+out)
}
