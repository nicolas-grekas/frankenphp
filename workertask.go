package frankenphp

// #include "frankenphp.h"
import "C"
import (
	"os"
	"runtime/cgo"
	"slices"
	"strconv"
	"sync"
	"time"
)

// taskUpdatesMax bounds the updates buffered per task: past it,
// frankenphp_update_task() waits for the sender to read
const taskUpdatesMax = 16

// taskSignalEscalation bounds how long a task waits on the one thread it was
// signaled to: past it every thread gets the line, so a script that parked
// its handle without reading it does not hold the task
var taskSignalEscalation = 10 * time.Millisecond

// bench only: FRANKENPHP_TASK_ESCALATION_MS overrides the escalation delay
func init() {
	if ms, err := strconv.Atoi(os.Getenv("FRANKENPHP_TASK_ESCALATION_MS")); err == nil && ms > 0 {
		taskSignalEscalation = time.Duration(ms) * time.Millisecond
	}
}

// workerTask is a unit of work handed by a PHP thread to a thread of a
// background worker, see frankenphp_send_task(). The payload and the
// updates flowing back are persistent HashTables, copied into request
// memory on arrival. A socket pair per task wakes the sender: one byte per
// update, EOF once the receiver closed its stream.
type workerTask struct {
	handle   cgo.Handle
	worker   *worker
	payload  *C.HashTable  // owned by the task until a thread picks it up
	pickedUp chan struct{} // closed when a thread picks the task up
	// cancelled is closed when the sender gave up before any pickup, ending
	// the watcher; abortReason is set by the watcher, under the queue mutex,
	// when the wait must end without a pickup
	cancelled   chan struct{}
	abortReason string
	// receiver and pickedUpAt are set by the thread that picked the task
	// up and read by its close, on the same thread
	receiver   *backgroundWorkerThread
	pickedUpAt time.Time
	// socks[0] is the sender's end and socks[1] the receiver's: each moves
	// to the stream of its side when that side gets the task, -1 from then
	// on; the ones still here are closed with the task
	socks [2]int64
	// nudgeSock is the receiver's end, written to on each update while the
	// receiver's stream owns it: updates come from the receiver's thread
	// before its close, so the socket is open
	nudgeSock int64

	mu         sync.Mutex
	cond       *sync.Cond // signaled on pop and close
	updates    []*C.HashTable
	closed     bool // the receiver closed its stream
	aborted    bool // ...during request shutdown: the script ended with the task open
	senderGone bool // the sender closed its stream
	retired    int  // sides done with the task, freed at 2
}

// taskQueue holds the tasks sent to a background worker until a thread picks
// them up. Its mutex also guards the stop sockets of the worker's threads:
// senders write the wake-up line to them, so they must not be closed
// meanwhile.
type taskQueue struct {
	mu      sync.Mutex
	pending []*workerTask
	next    int // thread to signal first, spreads tasks over a pool
}

// remove takes t out of the queue; false if a thread picked it up already
func (q *taskQueue) remove(t *workerTask) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	i := slices.Index(q.pending, t)
	if i < 0 {
		return false
	}
	q.pending = slices.Delete(q.pending, i, i+1)

	return true
}

// signalTask wakes one parked thread of the worker with the line its script
// reads on the handle, round-robin over the pool; a thread claimed this way
// is no longer parked. False when no thread is parked: the task then waits
// in the queue for a thread to drain it or to park, see
// go_frankenphp_background_worker_wait. Called with tasks.mu held
func (worker *worker) signalTask() bool {
	worker.threadMutex.RLock()
	defer worker.threadMutex.RUnlock()

	n := len(worker.threads)
	for i := range n {
		thread := worker.threads[(worker.tasks.next+i)%n]
		if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.parked && handler.stopSock >= 0 {
			handler.parked = false
			worker.tasks.next = (worker.tasks.next + i + 1) % n
			C.frankenphp_worker_signal_task(C.intptr_t(handler.stopSock))

			return true
		}
	}

	return false
}

// signalAllThreads is the fallback of taskSignalEscalation: every thread of
// the worker gets the line, parked or not; called with tasks.mu held
func (worker *worker) signalAllThreads() {
	worker.threadMutex.RLock()
	for _, thread := range worker.threads {
		if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.stopSock >= 0 {
			handler.parked = false
			C.frankenphp_worker_signal_task(C.intptr_t(handler.stopSock))
		}
	}
	worker.threadMutex.RUnlock()
}

// retire counts a side done with the task; the last one frees it
func (t *workerTask) retire() {
	t.mu.Lock()
	t.retired++
	last := t.retired == 2
	t.mu.Unlock()

	if last {
		t.free()
	}
}

// free releases whatever the task still holds: called by the last side to
// close its stream, or by the sender when no thread picked the task up
func (t *workerTask) free() {
	if t.payload != nil {
		C.frankenphp_vars_free(t.payload)
	}
	for _, update := range t.updates {
		C.frankenphp_vars_free(update)
	}
	for _, s := range t.socks {
		if s >= 0 {
			C.frankenphp_close_sock(C.intptr_t(s))
		}
	}
	t.handle.Delete()
}

//export go_frankenphp_send_task
func go_frankenphp_send_task(threadIndex C.uintptr_t, name *C.char, nameLen C.size_t, payload *C.HashTable) (C.uintptr_t, C.intptr_t, *C.char) {
	thread := phpThreads[threadIndex]
	workerName := C.GoStringN(name, C.int(nameLen))
	w := backgroundWorkerByName(thread.handler.frankenPHPContext(), workerName)
	if w == nil {
		C.frankenphp_vars_free(payload)

		return 0, -1, C.CString("frankenphp_send_task(): unknown background worker " + strconv.Quote(workerName))
	}
	if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.worker == w && w.countThreads() == 1 {
		C.frankenphp_vars_free(payload)

		return 0, -1, C.CString("frankenphp_send_task(): background worker " + strconv.Quote(workerName) + " has a single thread and cannot send a task to itself")
	}
	// closed when this thread is drained for a restart or the shutdown: the
	// target's threads are drained too, nobody would pick the task up
	drainChan := thread.drainChan

	var socks [2]C.intptr_t
	if C.frankenphp_task_open_sock_pair(&socks[0]) != 0 {
		C.frankenphp_vars_free(payload)

		return 0, -1, C.CString("frankenphp_send_task(): failed to create the socket pair of the task")
	}

	t := &workerTask{
		worker:    w,
		payload:   payload,
		pickedUp:  make(chan struct{}),
		cancelled: make(chan struct{}),
		socks:     [2]int64{int64(socks[0]), int64(socks[1])},
		nudgeSock: int64(socks[1]),
	}
	t.cond = sync.NewCond(&t.mu)
	t.handle = cgo.NewHandle(t)

	q := &w.tasks
	q.mu.Lock()
	q.pending = append(q.pending, t)
	w.signalTask()
	// queued like a request would be: a background worker has no other queue
	metrics.QueuedWorkerRequest(w.qualifiedName)
	q.mu.Unlock()

	// the C side waits for the pickup on the sender's end of the pair, in the
	// kernel rather than in a Go select: waking a thread parked inside a Go
	// callback costs the scheduler a hand-off, a byte on a socket does not.
	// The thread taking the task writes that byte, the watcher does when the
	// wait must end without a pickup
	go t.watch(drainChan)

	// the sender's end now belongs to the stream returned to the script
	s := t.socks[0]
	t.socks[0] = -1

	return C.uintptr_t(t.handle), C.intptr_t(s), nil
}

// watch escalates the wake-up when the thread signaled first does not come
// and ends the sender's wait when its thread is drained or FrankenPHP shuts
// down; it returns once the task is picked up or the sender gave up
func (t *workerTask) watch(drainChan <-chan struct{}) {
	escalate := time.NewTimer(taskSignalEscalation)
	defer escalate.Stop()

	for {
		select {
		case <-t.pickedUp:
			return
		case <-t.cancelled:
			return
		case <-escalate.C:
			q := &t.worker.tasks
			q.mu.Lock()
			if slices.Contains(q.pending, t) {
				t.worker.signalAllThreads()
			}
			q.mu.Unlock()
		case <-drainChan:
			t.abort("frankenphp_send_task(): the calling thread is restarting or shutting down")

			return
		case <-mainThread.done:
			t.abort("frankenphp_send_task(): FrankenPHP is shutting down")

			return
		}
	}
}

// abort ends the sender's wait for a pickup that must not happen anymore:
// the receiver's end of the pair is still the task's own before the pickup,
// a byte on it wakes the sender's poll
func (t *workerTask) abort(reason string) {
	q := &t.worker.tasks
	q.mu.Lock()
	if slices.Contains(q.pending, t) {
		t.abortReason = reason
		C.frankenphp_task_nudge(C.intptr_t(t.nudgeSock))
	}
	q.mu.Unlock()
}

// go_frankenphp_task_await tells the sender, woken on its socket, where its
// task stands: 1 picked up, 2 aborted with the reason, 0 neither
//
//export go_frankenphp_task_await
func go_frankenphp_task_await(handle C.uintptr_t) (C.int, *C.char) {
	t := cgo.Handle(handle).Value().(*workerTask)

	select {
	case <-t.pickedUp:
		return 1, nil
	default:
	}

	q := &t.worker.tasks
	q.mu.Lock()
	reason := t.abortReason
	q.mu.Unlock()
	if reason != "" {
		return 2, C.CString(reason)
	}

	return 0, nil
}

// go_frankenphp_task_cancel takes a task nobody picked up out of the queue
// and releases the receiver's side of it, the sender's stream close releases
// the rest; false when a thread got the task first
//
//export go_frankenphp_task_cancel
func go_frankenphp_task_cancel(handle C.uintptr_t, timedOut C.bool) C.bool {
	t := cgo.Handle(handle).Value().(*workerTask)
	if !t.worker.tasks.remove(t) {
		return false
	}
	close(t.cancelled)

	name := t.worker.qualifiedName
	metrics.DequeuedWorkerRequest(name)
	if bool(timedOut) {
		metrics.WorkerTaskOutcome(name, TaskOutcomeTimeout)
	}

	C.frankenphp_vars_free(t.payload)
	t.payload = nil
	C.frankenphp_close_sock(C.intptr_t(t.socks[1]))
	t.socks[1] = -1
	t.mu.Lock()
	// nothing for the sender's close to settle
	t.closed = true
	t.mu.Unlock()
	t.retire()

	return true
}

// go_frankenphp_background_worker_wait is called before a script blocks on
// its handle, a read or a select cast: the thread parks unless tasks are
// queued, in which case the script must dequeue them first. For a read the
// line then comes from the read op itself; a select needs a real one on the
// socket. Under tasks.mu, so a task queued after the check finds the thread
// parked and signals it: no wake-up is lost either way.
//
//export go_frankenphp_background_worker_wait
func go_frankenphp_background_worker_wait(threadIndex C.uintptr_t, forSelect C.bool) C.bool {
	handler, ok := phpThreads[threadIndex].handler.(*backgroundWorkerThread)
	if !ok {
		return false
	}

	q := &handler.worker.tasks
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		handler.parked = true

		return false
	}
	if bool(forSelect) && handler.stopSock >= 0 {
		C.frankenphp_worker_signal_task(C.intptr_t(handler.stopSock))
	}

	return true
}

// go_frankenphp_background_worker_woke is called once a read on the handle
// returned: the script is running again
//
//export go_frankenphp_background_worker_woke
func go_frankenphp_background_worker_woke(threadIndex C.uintptr_t) {
	handler, ok := phpThreads[threadIndex].handler.(*backgroundWorkerThread)
	if !ok {
		return
	}

	q := &handler.worker.tasks
	q.mu.Lock()
	handler.parked = false
	q.mu.Unlock()
}

//export go_frankenphp_receive_task
func go_frankenphp_receive_task(threadIndex C.uintptr_t) (C.uintptr_t, *C.HashTable, C.intptr_t) {
	handler, ok := phpThreads[threadIndex].handler.(*backgroundWorkerThread)
	if !ok {
		// refused on the C side already
		return 0, nil, -1
	}

	q := &handler.worker.tasks
	q.mu.Lock()
	if len(q.pending) == 0 {
		q.mu.Unlock()

		return 0, nil, -1
	}
	t := q.pending[0]
	q.pending = slices.Delete(q.pending, 0, 1)
	metrics.DequeuedWorkerRequest(handler.worker.qualifiedName)
	// the payload moves to request memory and the receiver's end of the
	// pair to the receiver's stream, both on the C side
	payload := t.payload
	t.payload = nil
	sock := t.socks[1]
	t.socks[1] = -1
	q.mu.Unlock()
	close(t.pickedUp)
	// wakes the sender's wait for the pickup, see go_frankenphp_send_task;
	// after the channel, so the sender finds it closed once woken
	C.frankenphp_task_nudge(C.intptr_t(sock))

	t.receiver = handler
	t.pickedUpAt = time.Now()
	metrics.StartWorkerTask(handler.worker.qualifiedName)
	// busy on the threads endpoint while it holds a task
	if handler.openTasks++; handler.openTasks == 1 {
		handler.state.MarkAsWaiting(false)
	}

	return C.uintptr_t(t.handle), payload, C.intptr_t(sock)
}

//export go_frankenphp_update_task
func go_frankenphp_update_task(handle C.uintptr_t, update *C.HashTable) *C.char {
	t := cgo.Handle(handle).Value().(*workerTask)

	t.mu.Lock()
	for len(t.updates) >= taskUpdatesMax && !t.senderGone {
		t.cond.Wait()
	}
	if t.senderGone {
		t.mu.Unlock()
		C.frankenphp_vars_free(update)

		return C.CString("frankenphp_update_task(): the sender closed the task")
	}
	t.updates = append(t.updates, update)
	t.mu.Unlock()

	// one byte per update on the sender's stream
	C.frankenphp_task_nudge(C.intptr_t(t.nudgeSock))

	return nil
}

//export go_frankenphp_read_task
func go_frankenphp_read_task(handle C.uintptr_t) (*C.HashTable, C.int) {
	t := cgo.Handle(handle).Value().(*workerTask)

	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.updates) > 0 {
		update := t.updates[0]
		t.updates = slices.Delete(t.updates, 0, 1)
		t.cond.Signal()

		return update, C.int(C.FRANKENPHP_TASK_READ_UPDATE)
	}
	switch {
	case t.aborted:
		return nil, C.int(C.FRANKENPHP_TASK_READ_ABORTED)
	case t.closed:
		return nil, C.int(C.FRANKENPHP_TASK_READ_COMPLETED)
	}

	return nil, C.int(C.FRANKENPHP_TASK_READ_PENDING)
}

//export go_frankenphp_task_receiver_close
func go_frankenphp_task_receiver_close(handle C.uintptr_t, aborted C.bool) {
	t := cgo.Handle(handle).Value().(*workerTask)

	// the receiver's stream closes its end right after this, which lands as
	// EOF on the sender's, behind the bytes of the updates still queued
	t.mu.Lock()
	t.closed = true
	t.aborted = bool(aborted)
	// the first side to close settles the outcome
	settled := !t.senderGone
	t.cond.Broadcast()
	t.mu.Unlock()

	name := t.worker.qualifiedName
	metrics.StopWorkerTask(name, time.Since(t.pickedUpAt))
	if settled {
		outcome := TaskOutcomeCompleted
		if aborted {
			outcome = TaskOutcomeAborted
		}
		metrics.WorkerTaskOutcome(name, outcome)
	}
	handler := t.receiver
	handler.openTasks--
	if handler.openTasks == 0 && !handler.isBootingScript {
		handler.state.MarkAsWaiting(true)
	}

	t.retire()
}

//export go_frankenphp_task_sender_close
func go_frankenphp_task_sender_close(handle C.uintptr_t) {
	t := cgo.Handle(handle).Value().(*workerTask)

	t.mu.Lock()
	t.senderGone = true
	// the first side to close settles the outcome
	settled := !t.closed
	updates := t.updates
	t.updates = nil
	t.cond.Broadcast()
	t.mu.Unlock()

	if settled {
		metrics.WorkerTaskOutcome(t.worker.qualifiedName, TaskOutcomeAbandoned)
	}
	for _, update := range updates {
		C.frankenphp_vars_free(update)
	}
	t.retire()
}
