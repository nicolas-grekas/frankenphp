package frankenphp

// /* closesocket() lives in ws2_32: the PHP dll links it, a cgo build does not by itself */
// #cgo windows LDFLAGS: -lws2_32
// #include "frankenphp.h"
import "C"
import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dunglas/frankenphp/internal/state"
)

// backgroundWorkerThread is the threadHandler of background worker scripts.
// It owns their lifecycle: boot the script, re-run it when it exits, restart
// it with a quadratic backoff when it crashes. Background workers share the
// PHP runtime with HTTP threads but never receive HTTP requests. The script
// can park on the stream returned by frankenphp_get_worker_handle(), which
// reaches EOF when the thread is drained, to exit gracefully on shutdown,
// reboot or handler transition, and which carries a line per task sent to
// the worker, see frankenphp_send_task().
type backgroundWorkerThread struct {
	state                  *state.ThreadState
	thread                 *phpThread
	worker                 *worker
	dummyFrankenPHPContext *frankenPHPContext
	failureCount           int // number of consecutive failed runs

	// isBootingScript is true until the current run waits on its handle, a
	// stream_select() or a read on the stream returned by
	// frankenphp_get_worker_handle(), the background analog of an HTTP
	// worker reaching frankenphp_handle_request(). Only touched on the PHP
	// thread (setup, the C callback during execution, teardown).
	isBootingScript bool

	// bootTimer warns when a run has not waited on its handle after
	// backgroundBootWarnDelay; only touched on the PHP thread
	bootTimer *time.Timer

	// openTasks counts the tasks picked up and not closed yet: the thread
	// is busy rather than waiting on the threads endpoint meanwhile. Only
	// touched on the PHP thread, pickup and close both happen there.
	openTasks int

	// stopSock holds the Go side's end of this thread's stop socket pair
	// (per thread so pool workers drain independently); the other end is
	// exposed to the script via frankenphp_get_worker_handle(). Wide enough
	// for a Windows SOCKET, -1 when not held. Guarded by worker.tasks.mu:
	// frankenphp_send_task() writes its wake-up line to it.
	stopSock int64
}

// backgroundBootWarnDelay is how long a run may go without waiting on its
// handle before a warning: Init() and Shutdown() wait for that point, so a
// script that never gets there hangs both silently
const backgroundBootWarnDelay = 10 * time.Second

func convertToBackgroundWorkerThread(thread *phpThread, worker *worker) {
	handler := &backgroundWorkerThread{
		state:    thread.state,
		thread:   thread,
		worker:   worker,
		stopSock: -1,
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

// drain closes the Go side's end of the stop socket pair so a script
// parked on the other end wakes up with EOF and can exit its loop. Called
// right before drainChan is closed on shutdown and reboot; also reused
// internally to release the socket on the other exit paths.
func (handler *backgroundWorkerThread) drain() {
	q := &handler.worker.tasks
	q.mu.Lock()
	s := handler.stopSock
	handler.stopSock = -1
	q.mu.Unlock()

	if s >= 0 {
		C.frankenphp_close_sock(C.intptr_t(s))
	}
}

// beforeScriptExecution returns the name of the script or an empty string on shutdown
func (handler *backgroundWorkerThread) beforeScriptExecution() string {
	switch handler.state.Get() {
	case state.TransitionRequested:
		if handler.worker.onThreadShutdown != nil {
			handler.worker.onThreadShutdown(handler.thread.threadIndex)
		}
		handler.worker.detachThread(handler.thread)
		return handler.thread.transitionToNewHandler()
	case state.Ready, state.TransitionComplete:
		handler.thread.updateContext(true)
		if handler.worker.onThreadReady != nil {
			handler.worker.onThreadReady(handler.thread.threadIndex)
		}

		for {
			err := handler.setupScript()
			if err == nil {
				return handler.worker.fileName
			}

			if globalLogger.Enabled(globalCtx, slog.LevelError) {
				globalLogger.LogAttrs(globalCtx, slog.LevelError, "failed to start background worker", slog.String("worker", handler.worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex), slog.Any("error", err))
			}

			// fail fast during startup so Init() surfaces the error to the
			// operator; past startup, back off and retry like a crash
			if startupFailChan != nil {
				startupFailChan <- err
				handler.thread.state.Set(state.ShuttingDown)
				return handler.beforeScriptExecution()
			}

			handler.backoff()
			if !handler.state.Is(state.Ready) && !handler.state.Is(state.TransitionComplete) {
				// drained during the backoff (shutdown, reboot, transition)
				return handler.beforeScriptExecution()
			}
		}
	case state.Rebooting, state.ForceRebooting:
		return ""
	case state.RebootReady:
		handler.state.Set(state.Ready)
		return handler.beforeScriptExecution()
	case state.ShuttingDown:
		if handler.worker.onThreadShutdown != nil {
			handler.worker.onThreadShutdown(handler.thread.threadIndex)
		}
		handler.worker.detachThread(handler.thread)

		// signal to stop
		return ""
	default:
		panic("unexpected state: " + handler.state.Name())
	}
}

// setupScript marks the thread as a background worker on the C side and
// takes ownership of the Go side's end of its stop socket pair.
func (handler *backgroundWorkerThread) setupScript() error {
	s := int64(C.frankenphp_set_background_worker_and_get_stop_sock())
	if s < 0 {
		return fmt.Errorf("failed to create the stop socket pair of background worker %q", handler.worker.qualifiedName)
	}
	q := &handler.worker.tasks
	q.mu.Lock()
	handler.stopSock = s
	// tasks sent between two runs were signaled on the previous pair
	for range q.pending {
		C.frankenphp_worker_signal_task(C.intptr_t(s))
	}
	q.mu.Unlock()

	switch handler.state.Get() {
	case state.ShuttingDown, state.Rebooting, state.ForceRebooting, state.TransitionRequested:
		// a concurrent drain may have run before the socket was published;
		// close it now so the script observes EOF immediately
		handler.drain()
	}

	fc, err := newWorkerDummyContext(handler.worker)
	if err != nil {
		handler.drain()
		return err
	}
	handler.dummyFrankenPHPContext = fc

	handler.isBootingScript = true
	handler.openTasks = 0
	metrics.StartWorker(handler.worker.qualifiedName)
	handler.bootTimer = time.AfterFunc(backgroundBootWarnDelay, func() {
		if globalLogger.Enabled(globalCtx, slog.LevelWarn) {
			globalLogger.LogAttrs(globalCtx, slog.LevelWarn, "background worker has not waited on its handle yet, Init() and Shutdown() wait for it, see frankenphp_get_worker_handle()", slog.String("worker", handler.worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex))
		}
	})

	if fc.logger.Enabled(fc.ctx, slog.LevelDebug) {
		fc.logger.LogAttrs(fc.ctx, slog.LevelDebug, "starting background worker", slog.String("worker", handler.worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex))
	}

	// the thread stays in TransitionComplete until the script waits on its
	// handle, see go_frankenphp_background_worker_ready

	return nil
}

func (handler *backgroundWorkerThread) afterScriptExecution(exitStatus int) {
	// the Go side's end of the stop socket pair belongs to this thread;
	// release it on every exit path so the next run gets a fresh pair
	// (drain() already took it when the exit was drain-triggered)
	handler.drain()
	worker := handler.worker
	handler.dummyFrankenPHPContext = nil

	handler.stopBootTimer()
	handler.state.MarkAsWaiting(false)

	// cooperative exit: the script waited on its handle and returned cleanly,
	// re-run it, unless the thread is being drained (beforeScriptExecution
	// checks the state)
	if exitStatus == 0 && !handler.isBootingScript {
		metrics.StopWorker(worker.qualifiedName, StopReasonRestart)

		if globalLogger.Enabled(globalCtx, slog.LevelDebug) {
			globalLogger.LogAttrs(globalCtx, slog.LevelDebug, "restarting background worker", slog.String("worker", worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex))
		}

		return
	}

	// crash after the ready point: like an HTTP worker, restart right away;
	// only boot failures count toward max_consecutive_failures
	if !handler.isBootingScript {
		metrics.StopWorker(worker.qualifiedName, StopReasonCrash)

		if globalLogger.Enabled(globalCtx, slog.LevelWarn) {
			globalLogger.LogAttrs(globalCtx, slog.LevelWarn, "background worker crashed, restarting", slog.String("worker", worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex), slog.Int("exit_status", exitStatus))
		}

		return
	}

	// boot failure: the script exited before waiting on its handle, a clean
	// exit included, which would otherwise respawn in a tight loop.
	// StopReasonBootFailure skips the ready-gauge decrement, matching the
	// ReadyWorker call that never happened
	metrics.StopWorker(worker.qualifiedName, StopReasonBootFailure)

	// max_consecutive_failures only fails hard during startup, where it
	// surfaces on startupFailChan so Init() returns the error to the
	// operator. Past startup, a failing background worker keeps
	// restarting with a louder log line: silently giving up would leave
	// the server in a broken half-state with no clear way to recover.
	pastCap := worker.maxConsecutiveFailures >= 0 && handler.failureCount >= worker.maxConsecutiveFailures
	if pastCap && startupFailChan != nil && !watcherIsEnabled {
		if exitStatus == 0 {
			startupFailChan <- fmt.Errorf("background worker %s exits without waiting on its handle, see frankenphp_get_worker_handle()", worker.fileName)
		} else {
			startupFailChan <- fmt.Errorf("too many consecutive failures: background worker %s keeps crashing", worker.fileName)
		}
		handler.thread.state.Set(state.ShuttingDown)
		return
	}

	logLevel := slog.LevelWarn
	logMsg := "background worker failed before waiting on its handle, restarting"
	if exitStatus == 0 {
		logMsg = "background worker exited without waiting on its handle, restarting"
	}
	if pastCap {
		logLevel = slog.LevelError
		logMsg = "background worker exceeded max_consecutive_failures, still restarting"
	}
	if globalLogger.Enabled(globalCtx, logLevel) {
		globalLogger.LogAttrs(globalCtx, logLevel, logMsg, slog.String("worker", worker.qualifiedName), slog.Int("thread", handler.thread.threadIndex), slog.Int("failures", handler.failureCount), slog.Int("exit_status", exitStatus))
	}

	handler.backoff()
}

func (handler *backgroundWorkerThread) stopBootTimer() {
	if handler.bootTimer != nil {
		handler.bootTimer.Stop()
		handler.bootTimer = nil
	}
}

//export go_frankenphp_background_worker_ready
func go_frankenphp_background_worker_ready(threadIndex C.uintptr_t) {
	// called on the PHP thread on the first wait on the handle; the handler
	// is a backgroundWorkerThread because frankenphp_get_worker_handle()
	// throws on every other thread kind
	if handler, ok := phpThreads[threadIndex].handler.(*backgroundWorkerThread); ok && handler.isBootingScript {
		handler.isBootingScript = false
		// the boot succeeded, only consecutive boot failures count
		handler.failureCount = 0
		handler.stopBootTimer()
		handler.worker.markReady()
		metrics.ReadyWorker(handler.worker.qualifiedName)
		// parked from now on as far as the threads state endpoint is concerned
		handler.state.MarkAsWaiting(true)

		// like an HTTP worker reaching frankenphp_handle_request(), the thread
		// is ready only now: initWorkers() waits for this state, so a script
		// that fails before waiting on its handle still fails Init()
		if handler.state.Is(state.TransitionComplete) {
			handler.state.Set(state.Ready)
		}
	}
}

// backoff waits before the next run of a crashed script: quadratic in the
// number of consecutive failures, capped at 1 second.
func (handler *backgroundWorkerThread) backoff() {
	backoffDuration := time.Duration(handler.failureCount*handler.failureCount*100) * time.Millisecond
	if backoffDuration > time.Second {
		backoffDuration = time.Second
	}
	handler.failureCount++
	time.Sleep(backoffDuration)
}
