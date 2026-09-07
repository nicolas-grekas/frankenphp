<?php

// Bg worker of the benchmarks: answers each task with the number of updates
// its payload asks for, the last one echoing the payload back, and publishes
// its average per-call timings (ns) with frankenphp_set_vars() every 1000
// tasks, for the sender to report. wakeups counts the lines read on the
// handle, empty_wakeups those that found no task (a pool sibling took it).
set_time_limit(0);
$handle = frankenphp_get_worker_handle();
$tasks = $wakeups = $emptyWakeups = 0;
$receive = $update = $close = $wake = $pickup = 0.0;
while (false !== fgets($handle)) {
    ++$wakeups;
    $t0 = hrtime(true);
    $task = frankenphp_receive_task();
    if (null === $task) {
        ++$emptyWakeups;
        continue;
    }
    while (null !== $task) {
        $t1 = hrtime(true);
        $receive += $t1 - $t0;
        [$stream, $payload] = $task;
        // hrtime() is CLOCK_MONOTONIC, comparable across threads: the sender
        // stamps the payload, so the wait for the pickup splits into our
        // wake-up and the dequeue
        if (isset($payload['sent_at'])) {
            $wake += $t0 - $payload['sent_at'];
            $pickup += $t1 - $payload['sent_at'];
        }
        for ($i = 1, $updates = $payload['updates'] ?? 1; $i < $updates; ++$i) {
            frankenphp_update_task($stream, ['i' => $i]);
        }
        $t2 = hrtime(true);
        frankenphp_update_task($stream, $payload);
        $t3 = hrtime(true);
        fclose($stream);
        $t4 = hrtime(true);
        $update += $t3 - $t2;
        $close += $t4 - $t3;
        if (0 === ++$tasks % 1000) {
            frankenphp_set_vars([
                'tasks' => $tasks,
                'wakeups' => $wakeups,
                'empty_wakeups' => $emptyWakeups,
                'receive_ns' => (int) ($receive / $tasks),
                'update_ns' => (int) ($update / $tasks),
                'close_ns' => (int) ($close / $tasks),
                'wake_ns' => (int) ($wake / $tasks),
                'pickup_ns' => (int) ($pickup / $tasks),
            ]);
        }
        $t0 = hrtime(true);
        $task = frankenphp_receive_task();
    }
}
