<?php

// Long-lived bg worker: disable PHP max_execution_time so the 30s default
// cannot interrupt the stream_select park. The C side calls
// zend_unset_timeout() too, but the belt-and-suspenders here covers PHP
// builds where that path does not fully disarm the timer.
set_time_limit(0);

// Pool worker: num > 1 means multiple threads share this worker name.
// Each thread is a separate instance. We publish the thread's $_SERVER
// identifying fields so the test can see both instances are live.
frankenphp_set_vars([
    'name' => $_SERVER['FRANKENPHP_WORKER_NAME'] ?? 'unknown',
    'pid' => getmypid(),
]);

$stream = frankenphp_get_worker_handle();
if ($stream !== null) {
    $read = [$stream];
    $write = null;
    $except = null;
    stream_select($read, $write, $except, null);
}
