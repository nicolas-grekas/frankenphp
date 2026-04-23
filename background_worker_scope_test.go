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

// TestBackgroundWorkerScopeIsolation declares two bg workers with the
// *same* name in two distinct scopes. Requests scoped to each block must
// resolve to their own worker, proving per-php_server isolation works.
func TestBackgroundWorkerScopeIsolation(t *testing.T) {
	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	scopeA := frankenphp.NextBackgroundWorkerScope()
	scopeB := frankenphp.NextBackgroundWorkerScope()

	require.NoError(t, frankenphp.Init(
		frankenphp.WithWorkers("shared", testDataDir+"background-worker-scope-a.php", 1,
			frankenphp.WithWorkerBackground(),
			frankenphp.WithWorkerBackgroundScope(scopeA)),
		frankenphp.WithWorkers("shared", testDataDir+"background-worker-scope-b.php", 1,
			frankenphp.WithWorkerBackground(),
			frankenphp.WithWorkerBackgroundScope(scopeB)),
		frankenphp.WithNumThreads(4),
	))
	t.Cleanup(frankenphp.Shutdown)

	read := func(scope frankenphp.BackgroundScope) string {
		req := httptest.NewRequest("GET", "http://example.com/background-worker-scope-reader.php", nil)
		fr, err := frankenphp.NewRequestWithContext(req,
			frankenphp.WithRequestDocumentRoot(testDataDir, false),
			frankenphp.WithRequestBackgroundScope(scope),
		)
		require.NoError(t, err)
		w := httptest.NewRecorder()
		if err := frankenphp.ServeHTTP(w, fr); err != nil && !errors.As(err, &frankenphp.ErrRejected{}) {
			t.Fatalf("serve: %v", err)
		}
		body, _ := io.ReadAll(w.Result().Body)
		return string(body)
	}

	// Each scope's worker publishes its own marker under the same name
	// ("shared"). The reader script reads get_vars('shared'); scope
	// selection on the request determines which worker is resolved.
	bodyA := read(scopeA)
	bodyB := read(scopeB)

	assert.Contains(t, bodyA, "scope=A", "scopeA request should resolve to worker-scope-a:\n"+bodyA)
	assert.Contains(t, bodyB, "scope=B", "scopeB request should resolve to worker-scope-b:\n"+bodyB)
}
