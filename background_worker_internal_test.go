package frankenphp

import (
	"io"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackgroundWorkerRestartForceKillsStuckThread actually exercises the
// force-kill path: the fixture sleep()s without watching the stop pipe,
// so handler.drain() cannot wake it. RestartWorkers must go through the
// grace-period timeout and the force-kill primitive (pthread_kill on
// Linux/FreeBSD) to finish within the budget. Skips platforms where
// force-kill cannot interrupt a blocking syscall (macOS has no realtime
// signals, Windows non-alertable Sleep stays uninterruptible).
func TestBackgroundWorkerRestartForceKillsStuckThread(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "freebsd" {
		t.Skipf("force-kill cannot interrupt blocking syscalls on %s", runtime.GOOS)
	}

	prev := drainGracePeriod
	drainGracePeriod = 2 * time.Second
	t.Cleanup(func() { drainGracePeriod = prev })

	cwd, _ := os.Getwd()
	testDataDir := cwd + "/testdata/"

	require.NoError(t, Init(
		WithWorkers("bg-stuck", testDataDir+"background-worker-stuck.php", 1,
			WithWorkerBackground()),
		WithNumThreads(2),
	))
	t.Cleanup(Shutdown)

	// Wait until the bg worker published 'ready' (the line right before
	// sleep(60)) so we know it is actually parked in the blocking
	// syscall when the drain fires - that's the only way to prove the
	// force-kill code path was exercised, not the stop-pipe EOF path.
	readerPHP := `<?php
try {
    $vars = frankenphp_get_vars('bg-stuck');
    echo 'ready=', $vars['ready'] ?? 'MISSING';
} catch (\Throwable $e) {
    echo 'err=', $e->getMessage();
}`
	tmp := testDataDir + "bg-stuck-reader.php"
	require.NoError(t, os.WriteFile(tmp, []byte(readerPHP), 0644))
	t.Cleanup(func() { _ = os.Remove(tmp) })

	require.Eventually(t, func() bool {
		req := httptest.NewRequest("GET", "http://example.com/bg-stuck-reader.php", nil)
		fr, err := NewRequestWithContext(req, WithRequestDocumentRoot(testDataDir, false))
		if err != nil {
			return false
		}
		w := httptest.NewRecorder()
		_ = ServeHTTP(w, fr)
		body, _ := io.ReadAll(w.Result().Body)
		return strings.Contains(string(body), "ready=1")
	}, 5*time.Second, 25*time.Millisecond, "bg worker never entered sleep()")

	start := time.Now()
	RestartWorkers()
	elapsed := time.Since(start)

	// Test grace period (2s) + slack for signal dispatch and drain completion.
	const budget = 5 * time.Second
	assert.Less(t, elapsed, budget, "drain must force-kill the stuck bg worker within the grace period")
}
