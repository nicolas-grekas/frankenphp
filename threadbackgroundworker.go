package frankenphp

// #include "frankenphp.h"
import "C"
import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/dunglas/frankenphp/internal/state"
)

// backgroundWorkerState holds the shared vars for a single background worker.
// Written by the background-worker thread via frankenphp_set_vars; read by
// HTTP threads via frankenphp_get_vars. The mutex serialises writer/reader
// access to varsPtr so the C side can pfree the old pointer safely after
// set_vars returns.
type backgroundWorkerState struct {
	varsPtr     unsafe.Pointer // *C.HashTable in persistent memory
	mu          sync.RWMutex
	varsVersion atomic.Uint64 // incremented on each set_vars call
	ready       chan struct{}
	readyOnce   sync.Once

	// bootFailure is set when the worker fails before reaching set_vars.
	// Used to surface actionable errors in logs and future error messages.
	bootFailure atomic.Pointer[bootFailureInfo]
}

type bootFailureInfo struct {
	entrypoint   string
	exitStatus   int
	failureCount int
}

// backgroundWorkerThread handles background worker scripts. Owns its own
// lifecycle: boot, publish vars, loop, optionally crash-restart with
// exponential backoff.
type backgroundWorkerThread struct {
	state                  *state.ThreadState
	thread                 *phpThread
	worker                 *worker
	dummyFrankenPHPContext *frankenPHPContext
	dummyContext           context.Context
	isBootingScript        bool
	failureCount           int
}

func convertToBackgroundWorkerThread(thread *phpThread, worker *worker) {
	handler := &backgroundWorkerThread{
		state:  thread.state,
		thread: thread,
		worker: worker,
	}
	thread.setHandler(handler)
	worker.attachThread(thread)
}

func (handler *backgroundWorkerThread) name() string {
	return "Background Worker PHP Thread - " + handler.worker.fileName
}

func (handler *backgroundWorkerThread) frankenPHPContext() *frankenPHPContext {
	return handler.dummyFrankenPHPContext
}

func (handler *backgroundWorkerThread) context() context.Context {
	if handler.dummyContext != nil {
		return handler.dummyContext
	}
	return globalCtx
}

// drain is called by drainWorkerThreads (and thread.shutdown) right before
// drainChan is closed. We close the stop-pipe's write end so the PHP worker
// script, which is typically parked in stream_select on the read end, wakes
// up and can finish its loop gracefully.
func (handler *backgroundWorkerThread) drain() {
	if fd := handler.worker.backgroundStopFdWrite.Swap(-1); fd >= 0 {
		C.frankenphp_worker_close_fd(C.int(fd))
	}
}

func (handler *backgroundWorkerThread) beforeScriptExecution() string {
	switch handler.state.Get() {
	case state.TransitionRequested:
		if handler.worker.onThreadShutdown != nil {
			handler.worker.onThreadShutdown(handler.thread.threadIndex)
		}
		handler.worker.detachThread(handler.thread)
		return handler.thread.transitionToNewHandler()
	case state.Restarting:
		if handler.worker.onThreadShutdown != nil {
			handler.worker.onThreadShutdown(handler.thread.threadIndex)
		}
		handler.state.Set(state.Yielding)
		handler.state.WaitFor(state.Ready, state.ShuttingDown)
		return handler.beforeScriptExecution()
	case state.Ready, state.TransitionComplete:
		handler.thread.updateContext(true)
		if handler.worker.onThreadReady != nil {
			handler.worker.onThreadReady(handler.thread.threadIndex)
		}

		handler.setupScript()

		return handler.worker.fileName
	case state.ShuttingDown:
		if handler.worker.onThreadShutdown != nil {
			handler.worker.onThreadShutdown(handler.thread.threadIndex)
		}
		handler.worker.detachThread(handler.thread)
		return ""
	}

	panic("unexpected state: " + handler.state.Name())
}

func (handler *backgroundWorkerThread) setupScript() {
	metrics.StartWorker(handler.worker.name)

	opts := append([]RequestOption(nil), handler.worker.requestOptions...)
	C.frankenphp_set_worker_name(handler.thread.pinCString(strings.TrimPrefix(handler.worker.name, "m#")), C._Bool(true))
	handler.worker.backgroundStopFdWrite.Store(int32(C.frankenphp_worker_get_stop_fd_write()))

	fc, err := newDummyContext(
		filepath.Base(handler.worker.fileName),
		opts...,
	)
	if err != nil {
		panic(err)
	}

	ctx := context.WithValue(globalCtx, contextKey, fc)

	fc.worker = handler.worker
	handler.dummyFrankenPHPContext = fc
	handler.dummyContext = ctx
	handler.isBootingScript = true

	if globalLogger.Enabled(ctx, slog.LevelDebug) {
		globalLogger.LogAttrs(ctx, slog.LevelDebug, "starting background worker", slog.String("worker", handler.worker.name), slog.Int("thread", handler.thread.threadIndex))
	}

	handler.thread.state.Set(state.Ready)
	fc.scriptFilename = handler.worker.fileName
}

func (handler *backgroundWorkerThread) afterScriptExecution(exitStatus int) {
	handler.worker.backgroundStopFdWrite.Store(-1)
	worker := handler.worker
	handler.dummyFrankenPHPContext = nil
	handler.dummyContext = nil

	// on exit status 0 we just run the worker script again
	if exitStatus == 0 && !handler.isBootingScript {
		metrics.StopWorker(worker.name, StopReasonRestart)

		if globalLogger.Enabled(globalCtx, slog.LevelDebug) {
			globalLogger.LogAttrs(globalCtx, slog.LevelDebug, "restarting background worker", slog.String("worker", worker.name), slog.Int("thread", handler.thread.threadIndex), slog.Int("exit_status", exitStatus))
		}

		return
	}

	if handler.isBootingScript {
		metrics.StopWorker(worker.name, StopReasonBootFailure)
	} else {
		metrics.StopWorker(worker.name, StopReasonCrash)
	}

	if !handler.isBootingScript {
		if globalLogger.Enabled(globalCtx, slog.LevelWarn) {
			globalLogger.LogAttrs(globalCtx, slog.LevelWarn, "background worker crashed, restarting", slog.String("worker", worker.name), slog.Int("thread", handler.thread.threadIndex), slog.Int("exit_status", exitStatus))
		}

		return
	}

	// Boot failure: capture the failure info so a future get_vars-style
	// call can surface it; log the condition and back off before the
	// next attempt.
	if worker.backgroundWorker != nil {
		worker.backgroundWorker.bootFailure.Store(&bootFailureInfo{
			entrypoint:   worker.fileName,
			exitStatus:   exitStatus,
			failureCount: handler.failureCount + 1,
		})
	}

	if worker.maxConsecutiveFailures >= 0 && startupFailChan != nil && !watcherIsEnabled && handler.failureCount >= worker.maxConsecutiveFailures {
		startupFailChan <- fmt.Errorf("too many consecutive failures: background worker %s has not reached frankenphp_set_vars()", worker.fileName)
		handler.thread.state.Set(state.ShuttingDown)
		return
	}

	if globalLogger.Enabled(globalCtx, slog.LevelWarn) {
		globalLogger.LogAttrs(globalCtx, slog.LevelWarn, "background worker boot failed, restarting", slog.String("worker", worker.name), slog.Int("thread", handler.thread.threadIndex), slog.Int("failures", handler.failureCount), slog.Int("exit_status", exitStatus))
	}

	backoffDuration := time.Duration(handler.failureCount*handler.failureCount*100) * time.Millisecond
	if backoffDuration > time.Second {
		backoffDuration = time.Second
	}
	handler.failureCount++
	time.Sleep(backoffDuration)
}

// markBackgroundReady flips isBootingScript to false on the first set_vars
// call after each (re)boot and resets failure counters. Idempotent within
// a given boot: subsequent set_vars calls before the next crash-restart
// are no-ops here.
func (handler *backgroundWorkerThread) markBackgroundReady() {
	if !handler.isBootingScript {
		return
	}

	handler.failureCount = 0
	handler.isBootingScript = false
	if handler.worker.backgroundWorker != nil {
		handler.worker.backgroundWorker.bootFailure.Store(nil)
	}

	// Close the ready channel at-most-once; a second close would panic.
	if handler.worker.backgroundWorker != nil {
		handler.worker.backgroundWorker.readyOnce.Do(func() {
			close(handler.worker.backgroundWorker.ready)
		})
	}

	metrics.ReadyWorker(handler.worker.name)
}
