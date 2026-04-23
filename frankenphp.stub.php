<?php

/** @generate-class-entries */

/** @var int */
const FRANKENPHP_LOG_LEVEL_DEBUG = -4;

/** @var int */
const FRANKENPHP_LOG_LEVEL_INFO = 0;

/** @var int */
const FRANKENPHP_LOG_LEVEL_WARN = 4;

/** @var int */
const FRANKENPHP_LOG_LEVEL_ERROR = 8;

function frankenphp_handle_request(callable $callback): bool {}

function headers_send(int $status = 200): int {}

function frankenphp_finish_request(): bool {}

/**
 * @alias frankenphp_finish_request
 */
function fastcgi_finish_request(): bool {}

function frankenphp_request_headers(): array {}

/**
 * @alias frankenphp_request_headers
 */
function apache_request_headers(): array {}

/**
 * @alias frankenphp_request_headers
*/
function getallheaders(): array {}

function frankenphp_response_headers(): array|bool {}

/**
 * @alias frankenphp_response_headers
 */
function apache_response_headers(): array|bool {}

/**
 * @param string|string[] $topics
 */
function mercure_publish(string|array $topics, string $data = '', bool $private = false, ?string $id = null, ?string $type = null, ?int $retry = null): string {}

/**
 * @param int $level The importance or severity of a log event. The higher the level, the more important or severe the event. For more details, see: https://pkg.go.dev/log/slog#Level
 * array<string, any> $context Values of the array will be converted to the corresponding Go type (if supported by FrankenPHP) and added to the context of the structured logs using https://pkg.go.dev/log/slog#Attr
 */
function frankenphp_log(string $message, int $level = 0, array $context = []): void {}

/**
 * Publish the given vars from a background worker. Only callable from a
 * worker started with the `background` flag. Values must be null, scalars,
 * arrays of allowed values, or enum cases.
 */
function frankenphp_set_vars(array $vars): void {}

/**
 * Read the shared vars published by the named background worker. Throws if
 * the worker is not declared, not running, or has not yet called set_vars.
 */
function frankenphp_get_vars(string $name): array {}

/**
 * Return the stop-signal stream for the current background worker. The
 * stream closes when FrankenPHP is draining the worker so the script can
 * exit its loop gracefully. Only callable from inside a background worker.
 */
function frankenphp_get_worker_handle() {}
