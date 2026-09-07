<?php

// Sends n tasks in a row to the worker named name, each carrying a string of
// size bytes and elements small entries, asking for updates updates and
// reading them all back; prints JSON with the per-task times in ns, split
// between the send (wait for the pickup) and the reads, plus the timings the
// worker published for itself.
$n = (int) ($_GET['n'] ?? 1000);
$name = $_GET['name'] ?? 'echo';
$payload = ['updates' => (int) ($_GET['updates'] ?? 1)];
if ($size = (int) ($_GET['size'] ?? 0)) {
    $payload['data'] = str_repeat('x', $size);
}
if ($elements = (int) ($_GET['elements'] ?? 0)) {
    $payload['elements'] = range(1, $elements);
}
$send = $read = 0.0;
$start = hrtime(true);
for ($i = 0; $i < $n; ++$i) {
    $t0 = hrtime(true);
    $payload['sent_at'] = $t0;
    $task = frankenphp_send_task($name, $payload);
    $t1 = hrtime(true);
    while (null !== frankenphp_read_task($task)) {
    }
    fclose($task);
    $t2 = hrtime(true);
    $send += $t1 - $t0;
    $read += $t2 - $t1;
}
$total = hrtime(true) - $start;
try {
    $worker = frankenphp_get_vars($name);
} catch (\Throwable) {
    $worker = null;
}
// a plain cgo call for scale: frankenphp_get_vars() copies a small snapshot
$cgo = 0;
if (null !== $worker) {
    $t0 = hrtime(true);
    for ($i = 0; $i < 1000; ++$i) {
        frankenphp_get_vars($name);
    }
    $cgo = (int) ((hrtime(true) - $t0) / 1000);
}
echo json_encode([
    'n' => $n,
    'per_task_ns' => (int) ($total / $n),
    'send_ns' => (int) ($send / $n),
    'read_ns' => (int) ($read / $n),
    'cgo_call_ns' => $cgo,
    'worker' => $worker,
]);
