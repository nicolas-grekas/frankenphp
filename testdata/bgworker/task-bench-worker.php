<?php

// Bg worker of the benchmarks: answers each task with the number of updates
// its payload asks for, the last one echoing the payload back, and publishes
// its average per-call timings (ns) with setVars() every 100 tasks, for the
// sender to report. wakeups counts the wake-ups it waited for, empty_wakeups
// those that found no task (a pool sibling took it).
set_time_limit(0);
$handle = new \FrankenPHP\WorkerHandle();
$stream = $handle->getStream();
$tasks = $wakeups = $emptyWakeups = 0;
$receive = $update = $complete = $wake = $pickup = 0.0;
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
        $t1 = hrtime(true);
        $receive += $t1 - $t0;
        $payload = $task->getPayload();
        // hrtime() is CLOCK_MONOTONIC, comparable across threads: the sender
        // stamps the payload, so the wait for the pickup splits into our
        // wake-up and the dequeue
        if (isset($payload['sent_at'])) {
            $wake += $t0 - $payload['sent_at'];
            $pickup += $t1 - $payload['sent_at'];
        }
        $t2 = hrtime(true);
        for ($i = 1, $updates = $payload['updates'] ?? 1; $i < $updates; ++$i) {
            $task->update(['i' => $i]);
        }
        $t3 = hrtime(true);
        $task->complete($payload);
        $t4 = hrtime(true);
        $update += $t3 - $t2;
        $complete += $t4 - $t3;
        if (0 === ++$tasks % 100) {
            $handle->setVars([
                'tasks' => $tasks,
                'wakeups' => $wakeups,
                'empty_wakeups' => $emptyWakeups,
                'receive_ns' => (int) ($receive / $tasks),
                'update_ns' => (int) ($update / $tasks),
                'complete_ns' => (int) ($complete / $tasks),
                'wake_ns' => (int) ($wake / $tasks),
                'pickup_ns' => (int) ($pickup / $tasks),
            ]);
        }
        $t0 = hrtime(true);
        $task = $handle->receive();
    }
}
