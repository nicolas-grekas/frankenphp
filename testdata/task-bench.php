<?php

// Sends n tasks in a row to the worker receiving the namespace ns, or n
// batches of fanout tasks, each carrying a string of size bytes and elements
// small entries, asking for updates updates and reading them all back; prints
// JSON with the times per task or batch in ns, split between the send (the
// constructors), the reads and the time a thread took to pick the last task
// up, plus the timings the worker keeps for itself, asked for with a last
// task.
$n = (int) ($_GET['n'] ?? 1000);
$ns = $_GET['ns'] ?? 'echo';
$payload = ['updates' => (int) ($_GET['updates'] ?? 1)];
if ($size = (int) ($_GET['size'] ?? 0)) {
    $payload['data'] = str_repeat('x', $size);
}
if ($elements = (int) ($_GET['elements'] ?? 0)) {
    $payload['elements'] = range(1, $elements);
}
// work_us of CPU work per task on the worker's side, for every task or, with
// mix, for every other one
$work = (int) ($_GET['work_us'] ?? 0);
$mix = isset($_GET['mix']);
// fanout tasks are sent before the first one is read, to as many threads
$fanout = max(1, (int) ($_GET['fanout'] ?? 1));
$send = $read = $pickup = 0.0;
$start = hrtime(true);
for ($i = 0; $i < $n; ++$i) {
    $t0 = hrtime(true);
    $payload['sent_at'] = $t0;
    $payload['work_us'] = $mix && $i % 2 ? 0 : $work;
    $tasks = [];
    for ($k = 0; $k < $fanout; ++$k) {
        $tasks[] = new \FrankenPHP\SentTaskHandle($ns, $payload);
    }
    $t1 = hrtime(true);
    $last = null;
    foreach ($tasks as $task) {
        while (null !== $update = $task->read()) {
            $last = $update;
        }
    }
    unset($tasks, $task);
    $t2 = hrtime(true);
    $send += $t1 - $t0;
    $read += $t2 - $t1;
    // hrtime() is CLOCK_MONOTONIC, comparable across threads
    $pickup += ($last['picked_at'] ?? $t0) - $t0;
}
$total = hrtime(true) - $start;

$task = new \FrankenPHP\SentTaskHandle($ns, ['stats' => true]);
$worker = $task->read();
unset($task);

echo json_encode([
    'n' => $n,
    'per_task_ns' => (int) ($total / $n),
    'send_ns' => (int) ($send / $n),
    'read_ns' => (int) ($read / $n),
    'pickup_ns' => (int) ($pickup / $n),
    'worker' => $worker,
]);
