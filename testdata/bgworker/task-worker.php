<?php

// Bg worker processing tasks. fgets() on the handle returns "task\n" for
// each task sent to the worker and false on drain; receive_task() dequeues
// them, null when another thread got there first. The payload drives the
// answer: input echoes back in the last update, steps sends that many
// progress updates first, sleep_ms simulates that much work, cut short if
// the sender closes its stream meanwhile, mark touches a file at pickup,
// crash exits without completing the task. Exceptions land in BG_SENTINEL.
set_time_limit(0);
$handle = frankenphp_get_worker_handle();
while (false !== fgets($handle)) {
    while ($task = frankenphp_receive_task()) {
        [$stream, $payload] = $task;
        if (!empty($payload['crash'])) {
            exit(1);
        }
        try {
            if (!empty($payload['mark'])) {
                touch($payload['mark']);
            }
            if (!empty($payload['sleep_ms'])) {
                $read = [$stream];
                $write = $except = null;
                if (stream_select($read, $write, $except, intdiv($payload['sleep_ms'], 1000), 1000 * ($payload['sleep_ms'] % 1000)) > 0 && feof($stream)) {
                    throw new \RuntimeException('the sender closed the task before the update');
                }
            }
            for ($i = 1, $steps = $payload['steps'] ?? 0; $i <= $steps; ++$i) {
                frankenphp_update_task($stream, ['step' => $i, 'of' => $steps]);
            }
            frankenphp_update_task($stream, [
                'result' => 'processed:' . ($payload['input'] ?? ''),
                'worker' => $_SERVER['FRANKENPHP_WORKER'],
                'tag' => $_SERVER['BG_TAG'] ?? '',
                'thread' => $threadId ??= bin2hex(random_bytes(4)),
            ]);
        } catch (\Throwable $e) {
            if (!empty($_SERVER['BG_SENTINEL'])) {
                file_put_contents($_SERVER['BG_SENTINEL'], get_class($e) . ': ' . $e->getMessage());
            }
        } finally {
            fclose($stream);
        }
    }
}
