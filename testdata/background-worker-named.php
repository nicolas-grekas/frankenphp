<?php

// Long-lived bg worker: disable PHP max_execution_time so the 30s default
// cannot interrupt the stream_select park. The C side calls
// zend_unset_timeout() too, but the belt-and-suspenders here covers PHP
// builds where that path does not fully disarm the timer.
set_time_limit(0);

// Named lazy-start worker used by ensure(). Different from the step-4
// fixture in that it echoes its FRANKENPHP_WORKER_NAME, so tests can
// confirm $_SERVER injection.
$name = $_SERVER['FRANKENPHP_WORKER_NAME'] ?? 'unknown';
frankenphp_set_vars([
    'FRANKENPHP_WORKER_NAME' => $name,
    'count' => 1,
]);

$stream = frankenphp_get_worker_handle();
if ($stream !== null) {
    $read = [$stream];
    $write = null;
    $except = null;
    stream_select($read, $write, $except, null);
}
