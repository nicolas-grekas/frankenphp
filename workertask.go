package frankenphp

// #include "frankenphp.h"
import "C"
import (
	"runtime/cgo"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// taskUpdatesMax bounds the updates buffered per task: past it,
// frankenphp_update_task() waits for the sender to read
const taskUpdatesMax = 16

// workerTask is a unit of work handed by a PHP thread to a thread of a
// background worker, see frankenphp_send_task(). The payload and the
// updates flowing back are persistent HashTables, copied into request
// memory on arrival. Each side waits on its descriptor of the task's channel
// and is signaled there by the other, one signal per event: pickup, update,
// completion and abort for the sender, abandonment for the receiver.
type workerTask struct {
	handle   cgo.Handle
	worker   *worker
	payload  *C.HashTable  // owned by the task until a thread picks it up
	pickedUp chan struct{} // closed when a thread picks the task up
	// cancelled is closed when the sender gave up before any pickup, ending
	// the watcher; drainChan and shutdown are the channels the watcher ends
	// the wait on, read on the sender's thread at send time
	cancelled           chan struct{}
	drainChan, shutdown <-chan struct{}
	// state is a FRANKENPHP_TASK_* word in C memory, written here before the
	// sender is signaled and read by its wait loop, in C, without a callback
	state *C.int32_t
	// receiver and pickedUpAt are set by the thread that picked the task
	// up and read by its close, on the same thread
	receiver   *backgroundWorkerThread
	pickedUpAt time.Time
	// fds[0] is the sender's descriptor, fds[1] the receiver's; the streams
	// wait on them but the task owns them, until both sides closed and the
	// pair goes back to the pool
	fds [2]int64

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

// pendingWord is the count of queued tasks the scripts' threads read in C
// before parking, see frankenphp_worker_handle_read; kept in step with
// pending under the mutex, with sequentially consistent stores so a thread
// storing its parked flag then loading the count, and a sender adding to the
// count then claiming the flag, cannot both miss each other
func (worker *worker) addPending(delta int32) {
	atomic.AddInt32((*int32)(unsafe.Pointer(worker.taskWords)), delta)
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

// claimParkedThread picks one parked thread of the worker, round-robin over
// the pool, and returns its stop socket to write the wake-up line to, or -1
// when no thread is parked: the task then waits in the queue for a thread to
// drain it or to park, see go_frankenphp_background_worker_wait. The thread
// is no longer parked once claimed. Called with tasks.mu held; the caller
// writes after releasing it, see signalThreads
func (worker *worker) claimParkedThread() (*backgroundWorkerThread, int64) {
	worker.threadMutex.RLock()
	defer worker.threadMutex.RUnlock()

	n := len(worker.threads)
	for i := range n {
		thread := worker.threads[(worker.tasks.next+i)%n]
		if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.stopSock >= 0 && handler.claimParked() {
			handler.signaling.Add(1)
			worker.tasks.next = (worker.tasks.next + i + 1) % n

			return handler, handler.stopSock
		}
	}

	return nil, -1
}

// claimAllThreads is the fallback of taskSignalEscalation: every thread of
// the worker gets the line, parked or not. Called with tasks.mu held, the
// caller writes to the sockets after releasing it
func (worker *worker) claimAllThreads() (handlers []*backgroundWorkerThread, socks []int64) {
	worker.threadMutex.RLock()
	for _, thread := range worker.threads {
		if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.stopSock >= 0 {
			handler.setParked(0)
			handler.signaling.Add(1)
			handlers = append(handlers, handler)
			socks = append(socks, handler.stopSock)
		}
	}
	worker.threadMutex.RUnlock()

	return handlers, socks
}

// signalThreads writes the wake-up line to sockets claimed under tasks.mu,
// after it was released: the write is a syscall, and a thread contending
// for the mutex meanwhile would park at the price of a scheduler hand-off
func signalThreads(handlers []*backgroundWorkerThread, socks []int64) {
	for i, s := range socks {
		C.frankenphp_worker_signal_task(C.intptr_t(s))
		handlers[i].signaling.Add(-1)
	}
}

// taskChanPool keeps the descriptor pairs of finished tasks for the next
// ones: drained, they are as good as new, and creating and closing them was
// most of a task's syscalls. Bounded so an idle server does not hold the
// descriptors of a past peak.
var taskChanPool struct {
	mu   sync.Mutex
	free [][2]int64
}

const taskChanPoolMax = 256

// taskChanGet returns a drained pair from the pool, or a new one
func taskChanGet() ([2]int64, bool) {
	taskChanPool.mu.Lock()
	if n := len(taskChanPool.free); n > 0 {
		fds := taskChanPool.free[n-1]
		taskChanPool.free = taskChanPool.free[:n-1]
		taskChanPool.mu.Unlock()

		return fds, true
	}
	taskChanPool.mu.Unlock()

	var fds [2]C.intptr_t
	if C.frankenphp_task_chan_open(&fds[0]) != 0 {
		return [2]int64{}, false
	}

	return [2]int64{int64(fds[0]), int64(fds[1])}, true
}

// taskChanPut returns a pair to the pool, closed if the pool is full; the
// syscalls happen outside of the pool mutex
func taskChanPut(fds [2]int64) {
	C.frankenphp_task_chan_drain(C.intptr_t(fds[0]))
	C.frankenphp_task_chan_drain(C.intptr_t(fds[1]))

	taskChanPool.mu.Lock()
	if len(taskChanPool.free) < taskChanPoolMax {
		taskChanPool.free = append(taskChanPool.free, fds)
		taskChanPool.mu.Unlock()

		return
	}
	taskChanPool.mu.Unlock()

	C.frankenphp_close_sock(C.intptr_t(fds[0]))
	C.frankenphp_close_sock(C.intptr_t(fds[1]))
}

// freeTaskChans closes the pooled pairs on shutdown
func freeTaskChans() {
	taskChanPool.mu.Lock()
	free := taskChanPool.free
	taskChanPool.free = nil
	taskChanPool.mu.Unlock()

	for _, fds := range free {
		C.frankenphp_close_sock(C.intptr_t(fds[0]))
		C.frankenphp_close_sock(C.intptr_t(fds[1]))
	}
}

// signalSender wakes the sender's wait: a pickup, an update, the end of
// the task or an abort
func (t *workerTask) signalSender() {
	C.frankenphp_task_chan_signal(C.intptr_t(t.fds[0]), C.intptr_t(t.fds[1]), 0)
}

// signalReceiver wakes the receiver's stream_select(): the sender is gone
func (t *workerTask) signalReceiver() {
	C.frankenphp_task_chan_signal(C.intptr_t(t.fds[0]), C.intptr_t(t.fds[1]), 1)
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
	taskChanPut(t.fds)
	C.free(unsafe.Pointer(t.state))
	t.handle.Delete()
}

// setState publishes where the task stands to the sender's wait loop
func (t *workerTask) setState(state int32) {
	atomic.StoreInt32((*int32)(unsafe.Pointer(t.state)), state)
}

//export go_frankenphp_send_task
func go_frankenphp_send_task(threadIndex C.uintptr_t, name *C.char, nameLen C.size_t, payload *C.HashTable) (C.uintptr_t, C.intptr_t, *C.int32_t, *C.char) {
	thread := phpThreads[threadIndex]
	workerName := C.GoStringN(name, C.int(nameLen))
	w := backgroundWorkerByName(thread.handler.frankenPHPContext(), workerName)
	if w == nil {
		C.frankenphp_vars_free(payload)

		return 0, -1, nil, C.CString("frankenphp_send_task(): unknown background worker " + strconv.Quote(workerName))
	}
	if handler, ok := thread.handler.(*backgroundWorkerThread); ok && handler.worker == w && w.countThreads() == 1 {
		C.frankenphp_vars_free(payload)

		return 0, -1, nil, C.CString("frankenphp_send_task(): background worker " + strconv.Quote(workerName) + " has a single thread and cannot send a task to itself")
	}
	fds, ok := taskChanGet()
	if !ok {
		C.frankenphp_vars_free(payload)

		return 0, -1, nil, C.CString("frankenphp_send_task(): failed to create the channel of the task")
	}

	t := &workerTask{
		worker:    w,
		payload:   payload,
		pickedUp:  make(chan struct{}),
		cancelled: make(chan struct{}),
		fds:       fds,
		state:     (*C.int32_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.int32_t(0))))),
		// closed when this thread is drained for a restart or the shutdown:
		// the target's threads are drained too, nobody would pick the task
		// up. Read here, on the PHP thread: a goroutine may only get to run
		// after Shutdown() replaced them
		drainChan: thread.drainChan,
		shutdown:  mainThread.done,
	}
	t.cond = sync.NewCond(&t.mu)
	t.handle = cgo.NewHandle(t)

	// queued like a request would be: a background worker has no other queue
	metrics.QueuedWorkerRequest(w.qualifiedName)
	q := &w.tasks
	q.mu.Lock()
	q.pending = append(q.pending, t)
	w.addPending(1)
	handler, sock := w.claimParkedThread()
	q.mu.Unlock()
	if handler != nil {
		signalThreads([]*backgroundWorkerThread{handler}, []int64{sock})
	}

	// the C side waits for the pickup on the sender's descriptor, in the
	// kernel rather than in a Go select: waking a thread parked inside a Go
	// callback costs the scheduler a hand-off, a signal on a descriptor does
	// not. The thread taking the task sends it; a pickup that takes longer
	// than the first wait slice brings in go_frankenphp_task_linger

	return C.uintptr_t(t.handle), C.intptr_t(t.fds[0]), t.state, nil
}

// go_frankenphp_task_linger is called by a sender whose first wait slice
// passed without a pickup, the uncommon case: the thread signaled first did
// not come, so every thread gets the line, and a watcher starts to end the
// wait if the sender's thread is drained or FrankenPHP shuts down. Neither
// costs the common case, a pickup within microseconds, a goroutine
//
//export go_frankenphp_task_linger
func go_frankenphp_task_linger(handle C.uintptr_t) {
	t := cgo.Handle(handle).Value().(*workerTask)

	q := &t.worker.tasks
	q.mu.Lock()
	var handlers []*backgroundWorkerThread
	var socks []int64
	pending := slices.Contains(q.pending, t)
	if pending {
		handlers, socks = t.worker.claimAllThreads()
	}
	q.mu.Unlock()
	signalThreads(handlers, socks)

	if pending {
		go t.watch()
	}
}

// watch ends the sender's wait when its thread is drained or FrankenPHP
// shuts down; it returns once the task is picked up or the sender gave up
func (t *workerTask) watch() {
	select {
	case <-t.pickedUp:
	case <-t.cancelled:
	case <-t.drainChan:
		t.abort(C.FRANKENPHP_TASK_ABORTED_DRAIN)
	case <-t.shutdown:
		t.abort(C.FRANKENPHP_TASK_ABORTED_SHUTDOWN)
	}
}

// abort ends the sender's wait for a pickup that must not happen anymore
func (t *workerTask) abort(state int32) {
	q := &t.worker.tasks
	q.mu.Lock()
	if slices.Contains(q.pending, t) {
		t.setState(state)
		t.signalSender()
	}
	q.mu.Unlock()
}

// go_frankenphp_task_side_gone tells a stream whether the other side closed
// its own: what feof() reports on the task streams
//
//export go_frankenphp_task_side_gone
func go_frankenphp_task_side_gone(handle C.uintptr_t, sender C.bool) C.bool {
	t := cgo.Handle(handle).Value().(*workerTask)

	t.mu.Lock()
	defer t.mu.Unlock()
	if bool(sender) {
		return C.bool(t.closed)
	}

	return C.bool(t.senderGone)
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
	t.worker.addPending(-1)
	close(t.cancelled)

	name := t.worker.qualifiedName
	metrics.DequeuedWorkerRequest(name)
	if bool(timedOut) {
		metrics.WorkerTaskOutcome(name, TaskOutcomeTimeout)
	}

	C.frankenphp_vars_free(t.payload)
	t.payload = nil
	t.mu.Lock()
	// nothing for the sender's close to settle
	t.closed = true
	t.mu.Unlock()
	t.retire()

	return true
}

// go_frankenphp_background_worker_wait is called before a script casts its
// handle for a select: the thread parks unless tasks are queued, in which
// case a line on the socket makes the select return at once. A read parks
// without a callback, see frankenphp_worker_handle_read. Under tasks.mu, so
// a task queued after the check finds the thread parked and signals it. The
// flag stays set when the read returns for another reason than a claim, a
// stale line or EOF: a claim meanwhile writes a line the script reads on
// its next pass.
//
//export go_frankenphp_background_worker_wait
func go_frankenphp_background_worker_wait(threadIndex C.uintptr_t) {
	handler, ok := phpThreads[threadIndex].handler.(*backgroundWorkerThread)
	if !ok {
		return
	}

	q := &handler.worker.tasks
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		handler.setParked(1)

		return
	}
	if handler.stopSock >= 0 {
		C.frankenphp_worker_signal_task(C.intptr_t(handler.stopSock))
	}
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
	handler.worker.addPending(-1)
	// the payload moves to request memory on the C side
	payload := t.payload
	t.payload = nil
	q.mu.Unlock()
	metrics.DequeuedWorkerRequest(handler.worker.qualifiedName)
	close(t.pickedUp)
	// wakes the sender's wait for the pickup, see go_frankenphp_send_task;
	// after the state, so the sender finds it once woken
	t.setState(C.FRANKENPHP_TASK_PICKED_UP)
	t.signalSender()

	t.receiver = handler
	t.pickedUpAt = time.Now()
	metrics.StartWorkerTask(handler.worker.qualifiedName)
	// busy on the threads endpoint while it holds a task
	if handler.openTasks++; handler.openTasks == 1 {
		handler.state.MarkAsWaiting(false)
	}

	return C.uintptr_t(t.handle), payload, C.intptr_t(t.fds[1])
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

	// one signal per update, after the push: the sender consumes one per
	// update it reads
	t.signalSender()

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

	t.mu.Lock()
	t.closed = true
	t.aborted = bool(aborted)
	// the first side to close settles the outcome
	settled := !t.senderGone
	t.cond.Broadcast()
	t.mu.Unlock()
	// the sender finds the end of the task behind the updates still queued;
	// nobody waits on its descriptor once it closed
	if settled {
		t.signalSender()
	}

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
		// the receiver's stream_select() and feof() see it; once the
		// receiver closed, nobody waits on its descriptor
		t.signalReceiver()
		metrics.WorkerTaskOutcome(t.worker.qualifiedName, TaskOutcomeAbandoned)
	}
	for _, update := range updates {
		C.frankenphp_vars_free(update)
	}
	t.retire()
}
