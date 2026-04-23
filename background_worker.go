package frankenphp

// #include <stdint.h>
// #include "frankenphp.h"
import "C"
import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// defaultMaxBackgroundWorkers is the default safety cap for catch-all
// background workers when the user doesn't set max_threads. Caps the
// number of distinct lazy-started instances from a single catch-all.
const defaultMaxBackgroundWorkers = 16

// BackgroundScope identifies an isolation boundary for background workers.
// Each php_server block uses a distinct scope so that two blocks can
// declare workers with the same name without conflict. The zero value is
// the global/embed scope (used when no per-block scope was assigned).
// Representation is opaque; obtain values via NextBackgroundWorkerScope.
type BackgroundScope int

var backgroundScopeCounter atomic.Uint64

// NextBackgroundWorkerScope returns a unique scope for background worker
// isolation. Each php_server block should call this once during
// provisioning.
func NextBackgroundWorkerScope() BackgroundScope {
	return BackgroundScope(backgroundScopeCounter.Add(1))
}

// backgroundLookups maps scopes to their background worker lookup.
// Scope 0 is the global/embed scope; each php_server block gets its own.
var backgroundLookups map[BackgroundScope]*backgroundWorkerLookup

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
	// scope/env/watch/failure policy as their eagerly-started siblings.
	scope                  BackgroundScope
	env                    PreparedEnv
	watch                  []string
	maxConsecutiveFailures int
	requestOptions         []RequestOption

	// declaredWorker is the pre-existing *worker struct for a named
	// declaration (num=0 lazy or num>=1 eager). It lets the lazy-start
	// path reuse this worker instead of scanning the global
	// workersByName map, which is not scope-aware: scoped bg workers
	// with the same user-facing name would otherwise collide into a
	// single *worker and overwrite each other's live state pointers.
	// nil for catch-all registries (each lazy-started name gets a
	// fresh worker).
	declaredWorker *worker
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

// buildBackgroundWorkerLookups constructs a scope->lookup map from declared
// worker options. Each scope (php_server block, or 0 for global/embed)
// gets its own lookup so workers declared with the same name in different
// blocks don't collide. Each declaration gets its own registry so shared-
// entrypoint declarations keep their own template options.
func buildBackgroundWorkerLookups(workers []*worker, opts []workerOpt) map[BackgroundScope]*backgroundWorkerLookup {
	lookups := make(map[BackgroundScope]*backgroundWorkerLookup)

	for i, o := range opts {
		if !o.isBackgroundWorker {
			continue
		}

		scope := o.backgroundScope
		lookup, ok := lookups[scope]
		if !ok {
			lookup = newBackgroundWorkerLookup()
			lookups[scope] = lookup
		}

		w := workers[i]
		// Use the worker's normalized filename (EvalSymlinks + FastAbs
		// from newWorker) instead of the raw o.fileName so lazy-start
		// from a catch-all resolves the same entrypoint even if cwd or
		// the symlink target changes after init.
		registry := newBackgroundWorkerRegistry(w.fileName)
		registry.scope = scope
		registry.env = o.env
		registry.watch = o.watch
		registry.maxConsecutiveFailures = o.maxConsecutiveFailures
		registry.requestOptions = o.requestOptions

		w.backgroundScope = scope
		phpName := strings.TrimPrefix(w.name, "m#")
		if phpName != "" && phpName != w.fileName {
			if o.num > 0 {
				registry.num = o.num
			}
			lookup.byName[phpName] = registry
			// Named declaration: remember the *worker so lazy-start can
			// reuse it instead of scanning workersByName.
			registry.declaredWorker = w

			// Pre-reserve the live state for eager (num >= 1) named
			// declarations: the worker thread created by initWorkers
			// will reserve it in setupScript, but any ensure_background_worker
			// call from an HTTP worker bootstrap that races ahead of
			// setupScript would otherwise see a missing entry and
			// lazy-start a duplicate. Reserving here makes the race
			// impossible; setupScript picks up the existing state.
			if o.num > 0 {
				bgw := &backgroundWorkerState{
					ready:   make(chan struct{}),
					aborted: make(chan struct{}),
				}
				registry.workers[phpName] = bgw
				w.backgroundWorker = bgw
			}
		} else {
			maxW := defaultMaxBackgroundWorkers
			if o.maxThreads > 1 {
				maxW = o.maxThreads
			}
			registry.maxWorkers = maxW
			lookup.catchAll = registry
			// Catch-all declarations are strictly lazy-started: each
			// ensure() with an unmatched name spawns its own threads on
			// demand. Force num to 0 so initWorkers does not create
			// eager placeholder threads that would call reserve() under
			// the catch-all's own filename and consume one of the cap
			// slots before any real lazy-start happens.
			w.num = 0
		}

		w.backgroundRegistry = registry
	}

	if len(lookups) == 0 {
		return nil
	}
	return lookups
}

// getLookup returns the background-worker lookup for the given thread.
// The scope is resolved from the thread's handler (for worker threads
// inheriting their worker's scope) or from the request context (for
// regular HTTP threads with WithRequestBackgroundScope).
//
// If the resolved scope has no workers declared (its lookup is nil), the
// caller falls through to the global/embed scope (0) so that globally-
// declared workers remain reachable from scoped requests. Scopes that
// declared their own workers stay strictly isolated because their lookup
// is non-nil.
func getLookup(thread *phpThread) *backgroundWorkerLookup {
	if backgroundLookups == nil {
		return nil
	}
	var scope BackgroundScope
	if handler, ok := thread.handler.(*workerThread); ok {
		scope = handler.worker.backgroundScope
	} else if handler, ok := thread.handler.(*backgroundWorkerThread); ok {
		scope = handler.worker.backgroundScope
	} else if fc, ok := fromContext(thread.context()); ok {
		scope = fc.backgroundScope
	}
	if scope != 0 {
		if l := backgroundLookups[scope]; l != nil {
			return l
		}
	}
	return backgroundLookups[0]
}

// startBackgroundWorker lazy-starts the named worker if it is not already
// running. Safe to call concurrently; only the first caller actually
// starts the worker, the rest observe the existing state.
func startBackgroundWorker(thread *phpThread, bgWorkerName string) error {
	if bgWorkerName == "" {
		return fmt.Errorf("background worker name must not be empty")
	}
	lookup := getLookup(thread)
	if lookup == nil {
		return fmt.Errorf("no background worker configured")
	}
	registry := lookup.Resolve(bgWorkerName)
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

	// Named declarations (num=0 lazy or num>=1 eager) already have a
	// pre-existing *worker struct recorded on the registry. Reuse it so
	// lazy-start doesn't create a duplicate and - crucially for per-
	// php_server isolation - doesn't route through the global
	// workersByName map, which is scope-agnostic and would make two
	// scopes sharing a user-facing name collide into the same *worker.
	// Catch-all registries leave declaredWorker nil so each lazy-started
	// name gets a fresh worker struct of its own.
	var w *worker
	freshWorker := false
	if registry.declaredWorker != nil {
		w = registry.declaredWorker
	} else {
		freshWorker = true
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
			backgroundScope:        registry.scope,
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
	// Redundant with newWorker's backgroundScope opt for fresh workers,
	// but necessary for declared workers whose scope is set on the
	// registry rather than on the workerOpt struct.
	w.backgroundScope = registry.scope

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
		if i == 0 && freshWorker {
			// Freshly-created catch-all instance: add to the global list so
			// RestartWorkers/DrainWorkers iterate it. Intentionally NOT
			// registered in workersByName - bg workers are resolved per-
			// scope via backgroundLookups, not via the global name map.
			scalingMu.Lock()
			workers = append(workers, w)
			scalingMu.Unlock()
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

// go_frankenphp_ensure_background_worker declares a dependency on one or
// more background workers by name. Each named worker is lazy-started if
// not already running; the call blocks until every one has reached ready
// (set_vars called at least once) or the shared deadline expires.
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
func go_frankenphp_ensure_background_worker(threadIndex C.uintptr_t, names **C.char, nameLens *C.size_t, nameCount C.int, timeoutMs C.int) *C.char {
	thread := phpThreads[threadIndex]
	lookup := getLookup(thread)
	if lookup == nil {
		return C.CString("no background worker configured")
	}

	n := int(nameCount)
	nameSlice := unsafe.Slice(names, n)
	nameLenSlice := unsafe.Slice(nameLens, n)
	bootstrap := isBootstrapEnsure(thread)

	// Start each named worker first. Reserve their states so a shared
	// deadline applies across the whole group (the caller gets one
	// timeout value, not one per worker).
	sks := make([]*backgroundWorkerState, n)
	goNames := make([]string, n)
	for i := 0; i < n; i++ {
		goNames[i] = C.GoStringN(nameSlice[i], C.int(nameLenSlice[i]))
		if err := startBackgroundWorker(thread, goNames[i]); err != nil {
			return C.CString(err.Error())
		}
		registry := lookup.Resolve(goNames[i])
		if registry == nil {
			return C.CString("background worker not found: " + goNames[i])
		}
		registry.mu.Lock()
		sks[i] = registry.workers[goNames[i]]
		registry.mu.Unlock()
		if sks[i] == nil {
			return C.CString("background worker not found: " + goNames[i])
		}
	}

	deadline := time.After(time.Duration(timeoutMs) * time.Millisecond)
	if bootstrap {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for i, sk := range sks {
		wait:
			for {
				select {
				case <-sk.ready:
					break wait
				case <-sk.aborted:
					return C.CString(sk.abortErr)
				case <-deadline:
					return C.CString(formatBackgroundWorkerTimeoutError(goNames[i], sk))
				case <-globalCtx.Done():
					return C.CString("frankenphp is shutting down")
				case <-ticker.C:
					if sk.bootFailure.Load() != nil {
						return C.CString(formatBackgroundWorkerTimeoutError(goNames[i], sk))
					}
				}
			}
		}
		return nil
	}

	for i, sk := range sks {
		select {
		case <-sk.ready:
		case <-sk.aborted:
			return C.CString(sk.abortErr)
		case <-deadline:
			return C.CString(formatBackgroundWorkerTimeoutError(goNames[i], sk))
		case <-globalCtx.Done():
			return C.CString("frankenphp is shutting down")
		}
	}
	return nil
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
// and copies its vars into the return value.
//
// callerVersion / outVersion implement a per-request cache:
//   - If callerVersion is non-nil and equals the current varsVersion,
//     the copy is skipped; outVersion is still set so the C side can
//     reuse its cached zval with pointer equality.
//   - Otherwise returnValue receives a fresh deep copy and outVersion
//     reports the version that copy corresponds to.
//
//export go_frankenphp_get_vars
func go_frankenphp_get_vars(threadIndex C.uintptr_t, name *C.char, nameLen C.size_t, returnValue *C.zval, callerVersion *C.uint64_t, outVersion *C.uint64_t) *C.char {
	thread := phpThreads[threadIndex]
	lookup := getLookup(thread)
	if lookup == nil {
		return C.CString("no background worker configured")
	}

	goName := C.GoStringN(name, C.int(nameLen))
	registry := lookup.Resolve(goName)
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

	// Fast path: caller's cached version matches current. Skip the copy;
	// the caller will reuse its cached zval.
	if callerVersion != nil && outVersion != nil {
		v := sk.varsVersion.Load()
		*outVersion = C.uint64_t(v)
		if uint64(*callerVersion) == v {
			return nil
		}
	}

	sk.mu.RLock()
	C.frankenphp_copy_persistent_vars(returnValue, sk.varsPtr)
	if outVersion != nil {
		*outVersion = C.uint64_t(sk.varsVersion.Load())
	}
	sk.mu.RUnlock()

	return nil
}
