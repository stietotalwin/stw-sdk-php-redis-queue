<?php

namespace StieTotalWin\RedisQueue;

/**
 * Simple PSR-3 compatible logger interface
 * 
 * Implement this interface to integrate with your application's logging system.
 * If no logger is provided, the library falls back to error_log().
 */
interface LoggerInterface
{
    public function emergency(string $message, array $context = []): void;
    public function alert(string $message, array $context = []): void;
    public function critical(string $message, array $context = []): void;
    public function error(string $message, array $context = []): void;
    public function warning(string $message, array $context = []): void;
    public function notice(string $message, array $context = []): void;
    public function info(string $message, array $context = []): void;
    public function debug(string $message, array $context = []): void;
}
