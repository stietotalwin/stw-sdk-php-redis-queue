<?php

namespace StieTotalWin\RedisQueue;

/**
 * Default logger that uses PHP's error_log()
 * 
 * Replace with your own LoggerInterface implementation for production use.
 */
class DefaultLogger implements LoggerInterface
{
    public function emergency(string $message, array $context = []): void
    {
        $this->writeLog('EMERGENCY', $message, $context);
    }

    public function alert(string $message, array $context = []): void
    {
        $this->writeLog('ALERT', $message, $context);
    }

    public function critical(string $message, array $context = []): void
    {
        $this->writeLog('CRITICAL', $message, $context);
    }

    public function error(string $message, array $context = []): void
    {
        $this->writeLog('ERROR', $message, $context);
    }

    public function warning(string $message, array $context = []): void
    {
        $this->writeLog('WARNING', $message, $context);
    }

    public function notice(string $message, array $context = []): void
    {
        $this->writeLog('NOTICE', $message, $context);
    }

    public function info(string $message, array $context = []): void
    {
        $this->writeLog('INFO', $message, $context);
    }

    public function debug(string $message, array $context = []): void
    {
        $this->writeLog('DEBUG', $message, $context);
    }

    private function writeLog(string $level, string $message, array $context): void
    {
        $logMessage = "[{$level}] {$message}";
        if (!empty($context)) {
            $logMessage .= " | " . json_encode($context);
        }
        error_log($logMessage);
    }
}
