package frankenphp

// #include <stdint.h>
// #include "frankenphp.h"
import "C"
import (
	"unsafe"
)

// go_frankenphp_set_vars is called from PHP when a background worker publishes
// its shared vars. The caller has already deep-copied the vars into persistent
// memory; here we swap the pointer under the state lock and hand back the old
// pointer so the C side can free it after the call returns.
//
// Returns an error string (malloc'd C string) on misuse, NULL on success.
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

// go_frankenphp_get_vars looks up the named background worker and copies its
// shared vars into the return value. Pure read: never starts a worker, never
// waits. If the worker is not declared, not running, or has not reached
// ready, an error string is returned.
//
//export go_frankenphp_get_vars
func go_frankenphp_get_vars(name *C.char, nameLen C.size_t, returnValue *C.zval) *C.char {
	goName := C.GoStringN(name, C.int(nameLen))

	w := workersByName[goName]
	if w == nil || !w.isBackgroundWorker {
		return C.CString("background worker not found: " + goName)
	}
	sk := w.backgroundWorker
	if sk == nil {
		return C.CString("background worker not running: " + goName)
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
