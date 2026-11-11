<?php

// Bg worker of the benchmarks: answers each task with the number of updates
// its payload asks for, after work_us microseconds of CPU work when it asks
// for some, the last one echoing the payload back with the time of its pickup, and answers a "stats" task with the average per-call timings
// (ns) of the thread that took it. wakeups counts the wake-ups it waited for,
// empty_wakeups those that found no task (a pool sibling took it).
// BG_RECEIVE=wait makes it wait in receive() where that exists, instead of
// waiting on its stream.
set_time_limit(0);
$handle = new \FrankenPHP\WorkerHandle();
$waits = 'wait' === ($_SERVER['BG_RECEIVE'] ?? '') && (new ReflectionMethod($handle, 'receive'))->getNumberOfParameters() > 0;
$tasks = $wakeups = $emptyWakeups = 0;
$receive = $update = $complete = 0.0;

$work = static function (\FrankenPHP\ReceivedTaskHandle $task, int $t0) use (&$tasks, &$wakeups, &$emptyWakeups, &$receive, &$update, &$complete, $waits): void {
    $t1 = hrtime(true);
    $payload = $task->getPayload();
    if (isset($payload['stats'])) {
        $n = max(1, $tasks);
        $task->complete([
            'tasks' => $tasks,
            'waits' => $waits,
            'wakeups' => $wakeups,
            'empty_wakeups' => $emptyWakeups,
            'receive_ns' => (int) ($receive / $n),
            'update_ns' => (int) ($update / $n),
            'complete_ns' => (int) ($complete / $n),
        ]);

        return;
    }
    if (!empty($payload['work_us'])) {
        // CPU work, not a sleep: the thread stays on its core
        for ($until = hrtime(true) + 1000 * $payload['work_us']; hrtime(true) < $until;) {
        }
    }
    for ($i = 1, $updates = $payload['updates'] ?? 1; $i < $updates; ++$i) {
        $task->update(['i' => $i]);
    }
    $t2 = hrtime(true);
    $payload['picked_at'] = $t1;
    $task->complete($payload);
    $t3 = hrtime(true);
    $receive += $t1 - $t0;
    $update += $t2 - $t1;
    $complete += $t3 - $t2;
    ++$tasks;
};

if ($waits) {
    while ($handle->tick()) {
        while ($task = $handle->receive(null)) {
            // the wait is part of the call: no receive time to tell apart
            $work($task, hrtime(true));
        }
    }

    return;
}

$stream = $handle->getStream();
while ($handle->tick()) {
    $read = [$stream];
    $write = $except = null;
    stream_select($read, $write, $except, null);
    ++$wakeups;
    $t0 = hrtime(true);
    $task = $handle->receive();
    if (null === $task) {
        ++$emptyWakeups;
        continue;
    }
    while (null !== $task) {
        $work($task, $t0);
        $t0 = hrtime(true);
        $task = $handle->receive();
    }
}
