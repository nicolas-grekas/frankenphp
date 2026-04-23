# Background Workers

Background workers are long-running PHP scripts that run outside the HTTP request cycle.
They observe their environment and publish variables that HTTP threads (both [workers](worker.md) and classic requests) can read in real time.

## How It Works

1. A background worker runs its own event loop (subscribe to Redis, watch files, poll an API, etc.).
2. It calls `frankenphp_set_vars()` to publish a snapshot of key-value pairs.
3. HTTP threads call `frankenphp_ensure_background_worker()` to declare a dependency and make sure the worker is running (lazy-started if needed, blocks until it has published at least once).
4. HTTP threads then call `frankenphp_get_vars()` to read the latest snapshot (pure read, no blocking, identical zval across repeated reads in one request).

## Configuration

Add `worker` directives with `background` to your [`php_server` or `php` block](config.md#caddyfile-config):

```caddyfile
example.com {
    php_server {
        # Named background workers
        worker /app/bin/console {
            background
            name config-watcher
        }
        worker /app/bin/console {
            background
            name feature-flags
        }

        # Catch-all: handles any unlisted name via ensure_background_worker()
        worker /app/bin/console {
            background
        }
    }
}
```

- **Named** (with `name`): lazy-started on first `ensure_background_worker()` call. If `num` is set to a positive integer, that many threads start eagerly at boot (pool mode); with `num 0` (default) the first `ensure()` starts one thread.
- **Catch-all** (no `name`): lazy-started on demand for any name not matched by a `name` directive. `max_threads` caps the number of distinct names it can lazy-start (default 16). Without a catch-all, only declared names can be ensured.
- Each `php_server` block has its own isolated scope: two blocks can use the same worker names without conflict.
- `max_consecutive_failures`, `env`, and `watch` work the same as for HTTP workers.

## PHP API

### `frankenphp_ensure_background_worker(string|array $name, float $timeout = 30.0): void`

Declares a dependency on one or more background workers. Pass a single name or an array of names for batch dependency declaration; the timeout applies across all names in one call. Behaviour depends on the caller context:

- **In an HTTP worker script, before `frankenphp_handle_request()` (bootstrap)**: lazy-starts the worker (at-most-once) if not already running and blocks until it has called `set_vars()` at least once. Fails fast on boot failure (no exponential-backoff tolerance): if the first boot attempts fail, the exception is thrown right away with the captured details. Use this to declare dependencies up front so broken deps visibly fail the HTTP worker rather than let it serve degraded traffic.
- **Everywhere else (inside `frankenphp_handle_request()`, or classic request-per-process)**: lazy-starts the worker and waits up to `$timeout`, tolerating transient boot failures via exponential backoff. The first caller pays the startup cost; subsequent callers in the same FrankenPHP process see the worker already reserved and return almost immediately. This supports the common pattern of library code loaded after bootstrap declaring its own dependencies lazily.

```php
// HTTP worker, bootstrap phase
frankenphp_ensure_background_worker('redis-watcher'); // fail-fast

while (frankenphp_handle_request(function () {
    $cfg = frankenphp_get_vars('redis-watcher'); // pure read
})) { gc_collect_cycles(); }

// Non-worker mode, every request
frankenphp_ensure_background_worker('redis-watcher'); // tolerant
$cfg = frankenphp_get_vars('redis-watcher');

// Batch form, shared deadline across workers
frankenphp_ensure_background_worker(['redis-watcher', 'config-watcher'], 5.0);
```

- Throws `RuntimeException` on timeout, missing entrypoint, or boot failure. The exception contains the captured failure details when available: resolved entrypoint path, exit status, number of attempts, and the last PHP error (message, file, line).
- Pick a short `$timeout` (e.g. `1.0`) to fail fast; pick a longer one to tolerate slow/flaky startups.
- `ValueError` is raised for an empty names array; `TypeError` is raised if the array contains non-strings.

### `frankenphp_get_vars(string $name): array`

Pure read: returns the latest published vars from a running background worker. Does not start workers or wait for readiness.

```php
$redis = frankenphp_get_vars('redis-watcher');
// ['MASTER_HOST' => '10.0.0.1', 'MASTER_PORT' => 6379]
```

- Throws `RuntimeException` if the worker isn't running or hasn't called `set_vars()` yet. Call `frankenphp_ensure_background_worker()` first to ensure readiness.
- Within a single HTTP request, repeated calls with the same name return the **same** cached array: `$a === $b` holds, and the lookup is O(1) after the first call.
- Works in both worker and non-worker mode.

### `frankenphp_set_vars(array $vars): void`

Publishes vars from inside a background worker. Each call **replaces** the entire vars array atomically.

Allowed value types: `null`, scalars (`bool`, `int`, `float`, `string`), nested `array`s whose values are also allowed types, and **enum** instances. Objects (other than enum cases), resources, and references are rejected.

- Throws `RuntimeException` if not called from a background worker thread.
- Throws `ValueError` if values contain unsupported types.

### `frankenphp_get_worker_handle(): resource`

Returns a readable stream for receiving signals from FrankenPHP. On shutdown or restart the write end of the underlying pipe is closed, so `fgets()` returns `false` (EOF). Use `stream_select()` to wait between iterations instead of `sleep()`:

```php
function background_worker_should_stop(float $timeout = 0): bool
{
    static $stream;
    $stream ??= frankenphp_get_worker_handle();
    $s = (int) $timeout;

    return match (@stream_select(...[[$stream], [], [], $s, (int) (($timeout - $s) * 1e6)])) {
        0 => false,           // timeout, keep going
        false => true,        // error, stop
        default => false === fgets($stream), // EOF = stop
    };
}
```

> [!WARNING]
> Avoid `sleep()` or `usleep()` in background workers: they block at the C level and cannot be interrupted cleanly. Use `stream_select()` with the signaling stream instead. If a worker ignores the signal, FrankenPHP force-kills it on Linux, FreeBSD and Windows after a 30-second grace period (see `Runtime Behaviour`).

## Examples

### Simple polling worker

```php
<?php
// bin/console dispatches based on worker name

$command = $_SERVER['FRANKENPHP_WORKER_NAME'] ?? '';

match ($command) {
    'config-watcher' => run_config_watcher(),
    'feature-flags'  => run_feature_flags(),
    default          => throw new \RuntimeException("Unknown background worker: $command"),
};

function run_config_watcher(): void
{
    $redis = new Redis();
    $redis->pconnect('127.0.0.1');

    do {
        frankenphp_set_vars([
            'maintenance'   => (bool) $redis->get('maintenance_mode'),
            'feature_flags' => json_decode($redis->get('features'), true),
        ]);
    } while (!background_worker_should_stop(5.0)); // check every 5s
}
```

### Event-driven worker

For real-time subscriptions (Redis pub/sub, SSE, WebSocket), use an async library and register the signaling stream on the event loop:

```php
function run_redis_watcher(): void
{
    $signalingStream = frankenphp_get_worker_handle();
    $sentinel = Amp\Redis\createRedisClient('tcp://sentinel-host:26379');

    $subscription = $sentinel->subscribe('+switch-master');

    Amp\async(function () use ($subscription) {
        foreach ($subscription as $message) {
            [$name, $oldIp, $oldPort, $newIp, $newPort] = explode(' ', $message);
            frankenphp_set_vars([
                'MASTER_HOST' => $newIp,
                'MASTER_PORT' => (int) $newPort,
            ]);
        }
    });

    $master = $sentinel->rawCommand('SENTINEL', 'get-master-addr-by-name', 'mymaster');
    frankenphp_set_vars([
        'MASTER_HOST' => $master[0],
        'MASTER_PORT' => (int) $master[1],
    ]);

    Amp\EventLoop::onReadable($signalingStream, function ($id) use ($signalingStream) {
        if (false === fgets($signalingStream)) {
            Amp\EventLoop::cancel($id); // EOF = stop
        }
    });
    Amp\EventLoop::run();
}
```

### HTTP worker depending on a background worker

```php
<?php
// public/index.php

$app = new App();
$app->boot();

// Declare dependencies once at bootstrap (fail-fast)
frankenphp_ensure_background_worker(['config-watcher', 'feature-flags']);

while (frankenphp_handle_request(function () use ($app) {
    $config = frankenphp_get_vars('config-watcher'); // pure read

    $_SERVER += [
        'APP_REDIS_HOST' => $config['MASTER_HOST'],
        'APP_REDIS_PORT' => $config['MASTER_PORT'],
    ];
    $app->handle($_GET, $_POST, $_COOKIE, $_FILES, $_SERVER);
})) {
    gc_collect_cycles();
}
```

### Non-worker mode

```php
<?php
// public/index.php, classic request-per-process

frankenphp_ensure_background_worker('config-watcher');
$config = frankenphp_get_vars('config-watcher');
// ... handle the request
```

### Graceful degradation in CLI mode

Running scripts with `frankenphp php-cli ...` does not load the `frankenphp` PHP module, so the background-worker functions do not exist. `function_exists()` returns `false` and library code can fall back to alternative sources:

```php
if (function_exists('frankenphp_get_vars')) {
    frankenphp_ensure_background_worker('config-watcher');
    $config = frankenphp_get_vars('config-watcher');
} else {
    $config = ['MASTER_HOST' => getenv('REDIS_HOST') ?: '127.0.0.1'];
}
```

## Runtime Behaviour

- Background workers get dedicated threads: they do not reduce HTTP capacity.
- `max_execution_time` is automatically disabled for background workers.
- `$_SERVER['FRANKENPHP_WORKER_NAME']` carries the worker's declared (or catch-all-resolved) name.
- `$_SERVER['FRANKENPHP_WORKER_BACKGROUND']` is `true` for background workers.
- `$_SERVER['argv'] = [$entrypoint, $name]` in background workers (for `bin/console`-style dispatching).
- Crash recovery: workers are automatically restarted with exponential backoff. During the restart window, `get_vars()` returns the last published data (stale but available) because vars are held in persistent memory across crashes. A warning is logged on crash.
- On shutdown/restart the signaling stream is closed (EOF). Well-behaved workers that check the stream exit within the 30-second grace period. Stuck workers are force-killed on Linux, FreeBSD, and Windows.

## Scoping

Each `php_server` block gets its own isolated background-worker scope, so workers declared with the same `name` in different blocks do not collide. Resolution rules for `ensure()` / `get_vars()`:

- A request inside a `php_server` block resolves first against that block's own declarations. If the block declares any background workers of its own, that lookup is authoritative and scope-isolated from every other block.
- A request inside a `php_server` block that declares **no** background workers falls back to the global/embed scope (workers declared at the top-level `frankenphp` directive or via the Go library). This makes a single globally-declared worker reachable from all otherwise-unconfigured blocks.
- Requests made outside any `php_server` block (e.g. when embedding FrankenPHP as a library) always resolve to the global/embed scope.

## Limits

- Named background workers with `num > 1` spin up a pool of threads that share the same published vars; `get_vars()` sees one consistent snapshot.
- Multiple named background workers in the same block can share the same entrypoint file. Each declaration keeps its own `env`, `watch`, and failure policy.
- Calling `ensure()` on a name that isn't declared and isn't covered by a catch-all raises `RuntimeException`.
