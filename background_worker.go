package frankenphp

// #include <stdint.h>
// #include "frankenphp.h"
import "C"
import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// defaultMaxBackgroundWorkers is the default safety cap for catch-all
// background workers when the user doesn't set max_threads. Caps the
// number of distinct lazy-started instances from a single catch-all.
const defaultMaxBackgroundWorkers = 16

// backgroundLookup is the registry that resolves a worker name to either
// a named declaration or the catch-all. Single global scope in this step;
// step 6 replaces this with a per-php_server scope map.
var backgroundLookup *backgroundWorkerLookup

// backgroundWorkerLookup maps worker names to their registry, with a
// separate slot for the catch-all (name-less) declaration.
type backgroundWorkerLookup struct {
	byName   map[string]*backgroundWorkerRegistry
	catchAll *backgroundWorkerRegistry
}

func newBackgroundWorkerLookup() *backgroundWorkerLookup {
	return &backgroundWorkerLookup{
		byName: make(map[string]*backgroundWorkerRegistry),
	}
}

// Resolve returns the registry for the given name, falling back to
// catch-all. Returns nil if neither matches.
func (l *backgroundWorkerLookup) Resolve(name string) *backgroundWorkerRegistry {
	if r, ok := l.byName[name]; ok {
		return r
	}
	return l.catchAll
}

// backgroundWorkerRegistry tracks the template options from a single
// declaration plus the live instances started from it. Named declarations
// have at most one entry keyed by their declared name; the catch-all can
// have many, up to maxWorkers.
type backgroundWorkerRegistry struct {
	entrypoint string
	num        int // threads per instance; 0 means lazy with 1 thread
	maxWorkers int // cap for catch-all instances; 0 = unlimited

	mu      sync.Mutex
	workers map[string]*backgroundWorkerState

	// Template options preserved so lazy-started workers inherit the same
	// env/watch/failure policy as their eagerly-started siblings.
	env                    PreparedEnv
	watch                  []string
	maxConsecutiveFailures int
	requestOptions         []RequestOption
}

func newBackgroundWorkerRegistry(entrypoint string) *backgroundWorkerRegistry {
	return &backgroundWorkerRegistry{
		entrypoint:             entrypoint,
		workers:                make(map[string]*backgroundWorkerState),
		maxConsecutiveFailures: -1,
	}
}

func (registry *backgroundWorkerRegistry) maxThreads() int {
	if registry.num > 0 {
		return registry.num
	}
	return 1
}

// reserve atomically looks up or inserts a state for the given name. If
// the maxWorkers cap is reached for a catch-all, returns an error.
func (registry *backgroundWorkerRegistry) reserve(name string) (*backgroundWorkerState, bool, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	if bgw := registry.workers[name]; bgw != nil {
		return bgw, true, nil
	}

	if registry.maxWorkers > 0 && len(registry.workers) >= registry.maxWorkers {
		return nil, false, fmt.Errorf("cannot start background worker %q: limit of %d reached (increase max_threads on the catch-all background worker or declare it as a named worker)", name, registry.maxWorkers)
	}

	bgw := &backgroundWorkerState{
		ready:   make(chan struct{}),
		aborted: make(chan struct{}),
	}
	registry.workers[name] = bgw

	return bgw, false, nil
}

// abortStart removes a reserved-but-never-started entry and wakes any
// concurrent ensure waiters that captured the same state before the start
// was abandoned. Without this, a waiter that raced reserve() against a
// failing start would block on sk.ready until its deadline.
func (registry *backgroundWorkerRegistry) abortStart(name string, bgw *backgroundWorkerState, err error) {
	registry.mu.Lock()
	if registry.workers[name] == bgw {
		delete(registry.workers, name)
	}
	registry.mu.Unlock()
	bgw.abort(err)
}

// buildBackgroundWorkerLookup constructs the name->registry map + catch-all
// slot from declared worker options. Each declaration gets its own registry
// so shared-entrypoint declarations keep their own template options.
func buildBackgroundWorkerLookup(workers []*worker, opts []workerOpt) *backgroundWorkerLookup {
	var lookup *backgroundWorkerLookup

	for i, o := range opts {
		if !o.isBackgroundWorker {
			continue
		}
		if lookup == nil {
			lookup = newBackgroundWorkerLookup()
		}

		registry := newBackgroundWorkerRegistry(o.fileName)
		registry.env = o.env
		registry.watch = o.watch
		registry.maxConsecutiveFailures = o.maxConsecutiveFailures
		registry.requestOptions = o.requestOptions

		w := workers[i]
		phpName := strings.TrimPrefix(w.name, "m#")
		if phpName != "" && phpName != w.fileName {
			if o.num > 0 {
				registry.num = o.num
			}
			lookup.byName[phpName] = registry
		} else {
			maxW := defaultMaxBackgroundWorkers
			if o.maxThreads > 1 {
				maxW = o.maxThreads
			}
			registry.maxWorkers = maxW
			lookup.catchAll = registry
		}

		w.backgroundRegistry = registry
	}

	return lookup
}

// startBackgroundWorker lazy-starts the named worker if it is not already
// running. Safe to call concurrently; only the first caller actually
// starts the worker, the rest observe the existing state.
func startBackgroundWorker(bgWorkerName string) error {
	if bgWorkerName == "" {
		return fmt.Errorf("background worker name must not be empty")
	}
	if backgroundLookup == nil {
		return fmt.Errorf("no background worker configured")
	}
	registry := backgroundLookup.Resolve(bgWorkerName)
	if registry == nil || registry.entrypoint == "" {
		return fmt.Errorf("no background worker configured for name %q", bgWorkerName)
	}
	return startBackgroundWorkerWithRegistry(registry, bgWorkerName)
}

func startBackgroundWorkerWithRegistry(registry *backgroundWorkerRegistry, bgWorkerName string) error {
	bgw, exists, err := registry.reserve(bgWorkerName)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	numThreads := registry.maxThreads()

	// A num=0 named declaration already created a worker struct at init
	// time; reuse it instead of creating a duplicate. For catch-all
	// instances (different names, different worker structs), create fresh.
	var w *worker
	if existing := workersByName[bgWorkerName]; existing != nil && existing.isBackgroundWorker {
		w = existing
	} else {
		// Clone env and slices: newWorker mutates env (writes
		// FRANKENPHP_WORKER) and appends to requestOptions, so sharing
		// these across lazy-started instances would race with HTTP
		// threads reading the originals.
		env := make(PreparedEnv, len(registry.env)+1)
		for k, v := range registry.env {
			env[k] = v
		}
		watch := append([]string(nil), registry.watch...)
		requestOptions := append([]RequestOption(nil), registry.requestOptions...)

		w, err = newWorker(workerOpt{
			name:                   bgWorkerName,
			fileName:               registry.entrypoint,
			num:                    numThreads,
			isBackgroundWorker:     true,
			env:                    env,
			watch:                  watch,
			maxConsecutiveFailures: registry.maxConsecutiveFailures,
			requestOptions:         requestOptions,
		})
		if err != nil {
			startErr := fmt.Errorf("failed to create background worker: %w", err)
			registry.abortStart(bgWorkerName, bgw, startErr)

			return startErr
		}
	}

	w.isBackgroundWorker = true
	w.backgroundWorker = bgw
	w.backgroundRegistry = registry

	for i := 0; i < numThreads; i++ {
		t := getInactivePHPThread()
		if t == nil {
			if i == 0 {
				startErr := fmt.Errorf("no available PHP thread for background worker (increase max_threads)")
				registry.abortStart(bgWorkerName, bgw, startErr)

				return startErr
			}
			globalLogger.LogAttrs(globalCtx, slog.LevelWarn, "background worker started with fewer threads than requested (increase max_threads)",
				slog.String("worker", bgWorkerName),
				slog.Int("requested", numThreads),
				slog.Int("attached", i))
			break
		}
		if i == 0 && workersByName[bgWorkerName] != w {
			// Freshly-created worker: register it and add to the global list.
			scalingMu.Lock()
			workers = append(workers, w)
			scalingMu.Unlock()
			workersByName[bgWorkerName] = w
		}
		convertToBackgroundWorkerThread(t, w)
	}

	if globalLogger.Enabled(globalCtx, slog.LevelInfo) {
		globalLogger.LogAttrs(globalCtx, slog.LevelInfo, "background worker started",
			slog.String("name", bgWorkerName), slog.Int("threads", numThreads))
	}

	return nil
}

// isBootstrapEnsure reports whether the calling thread is inside an HTTP
// worker's boot phase (before the first frankenphp_handle_request). That
// context takes the strict fail-fast path; everywhere else (bg worker
// runtime, classic request-per-process) uses the tolerant lazy-start path.
func isBootstrapEnsure(thread *phpThread) bool {
	handler, ok := thread.handler.(*workerThread)
	return ok && handler.isBootingScript
}

// go_frankenphp_ensure_background_worker declares a dependency on a
// background worker by name. Lazy-starts it if not already running, then
// blocks until it has called set_vars (ready state) or the timeout expires.
//
// Bootstrap mode (HTTP worker before frankenphp_handle_request): fail-fast.
// Any boot failure throws immediately with captured details, without
// waiting for the restart/backoff cycle. A broken dependency visibly fails
// the HTTP worker instead of letting it serve degraded traffic.
//
// Runtime mode (inside frankenphp_handle_request, classic request path):
// tolerant. Waits up to the timeout, letting the restart-with-backoff
// cycle recover from transient boot failures.
//
//export go_frankenphp_ensure_background_worker
func go_frankenphp_ensure_background_worker(threadIndex C.uintptr_t, name *C.char, nameLen C.size_t, timeoutMs C.int) *C.char {
	thread := phpThreads[threadIndex]
	if backgroundLookup == nil {
		return C.CString("no background worker configured")
	}

	goName := C.GoStringN(name, C.int(nameLen))
	bootstrap := isBootstrapEnsure(thread)

	if err := startBackgroundWorker(goName); err != nil {
		return C.CString(err.Error())
	}
	registry := backgroundLookup.Resolve(goName)
	if registry == nil {
		return C.CString("background worker not found: " + goName)
	}
	registry.mu.Lock()
	sk := registry.workers[goName]
	registry.mu.Unlock()
	if sk == nil {
		return C.CString("background worker not found: " + goName)
	}

	deadline := time.After(time.Duration(timeoutMs) * time.Millisecond)
	if bootstrap {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-sk.ready:
				return nil
			case <-sk.aborted:
				return C.CString(sk.abortErr)
			case <-deadline:
				return C.CString(formatBackgroundWorkerTimeoutError(goName, sk))
			case <-globalCtx.Done():
				return C.CString("frankenphp is shutting down")
			case <-ticker.C:
				if sk.bootFailure.Load() != nil {
					return C.CString(formatBackgroundWorkerTimeoutError(goName, sk))
				}
			}
		}
	}

	select {
	case <-sk.ready:
		return nil
	case <-sk.aborted:
		return C.CString(sk.abortErr)
	case <-deadline:
		return C.CString(formatBackgroundWorkerTimeoutError(goName, sk))
	case <-globalCtx.Done():
		return C.CString("frankenphp is shutting down")
	}
}

func formatBackgroundWorkerTimeoutError(name string, sk *backgroundWorkerState) string {
	info := sk.bootFailure.Load()
	if info == nil {
		return fmt.Sprintf("timeout waiting for background worker %q", name)
	}
	msg := fmt.Sprintf(
		"timeout waiting for background worker %q (entrypoint: %s); last boot failure after %d attempt(s), exit status %d",
		name, info.entrypoint, info.failureCount, info.exitStatus,
	)
	if info.phpError != "" {
		msg += ": " + info.phpError
	}
	return msg
}

// go_frankenphp_set_vars is called from PHP when a background worker
// publishes its shared vars. The caller has already deep-copied the vars
// into persistent memory; here we swap the pointer under the state lock
// and hand back the old pointer so the C side can free it after the call.
//
//export go_frankenphp_set_vars
func go_frankenphp_set_vars(threadIndex C.uintptr_t, varsPtr unsafe.Pointer, oldPtr *unsafe.Pointer) *C.char {
	thread := phpThreads[threadIndex]

	bgHandler, ok := thread.handler.(*backgroundWorkerThread)
	if !ok || bgHandler.worker.backgroundWorker == nil {
		return C.CString("frankenphp_set_vars() can only be called from a background worker")
	}

	sk := bgHandler.worker.backgroundWorker

	sk.mu.Lock()
	*oldPtr = sk.varsPtr
	sk.varsPtr = varsPtr
	sk.varsVersion.Add(1)
	sk.mu.Unlock()

	bgHandler.markBackgroundReady()

	return nil
}

// go_frankenphp_get_vars resolves the named worker through the lookup
// (named or catch-all), waits on sk.ready without starting the worker,
// and copies its vars into the return value. If the caller hasn't called
// ensure() first, this returns a "not ready" error.
//
//export go_frankenphp_get_vars
func go_frankenphp_get_vars(name *C.char, nameLen C.size_t, returnValue *C.zval) *C.char {
	if backgroundLookup == nil {
		return C.CString("no background worker configured")
	}

	goName := C.GoStringN(name, C.int(nameLen))
	registry := backgroundLookup.Resolve(goName)
	if registry == nil {
		return C.CString("background worker not found: " + goName + " (call frankenphp_ensure_background_worker first)")
	}
	registry.mu.Lock()
	sk := registry.workers[goName]
	registry.mu.Unlock()
	if sk == nil {
		return C.CString("background worker not running: " + goName + " (call frankenphp_ensure_background_worker first)")
	}

	select {
	case <-sk.ready:
	default:
		return C.CString("background worker not ready: " + goName + " (no set_vars call yet)")
	}

	sk.mu.RLock()
	C.frankenphp_copy_persistent_vars(returnValue, sk.varsPtr)
	sk.mu.RUnlock()

	return nil
}
