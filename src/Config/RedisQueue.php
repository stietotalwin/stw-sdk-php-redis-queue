<?php

namespace StieTotalWin\RedisQueue\Config;

use CodeIgniter\Config\BaseConfig;

/**
 * Redis Queue Configuration
 */
class RedisQueue extends BaseConfig
{
    /**
     * Default Redis connection host
     *
     * @var string
     */
    public $host = '127.0.0.1';

    /**
     * Default Redis connection port
     *
     * @var int
     */
    public $port = 6379;

    /**
     * Default Redis connection password
     *
     * @var string|null
     */
    public $password = null;

    /**
     * Default Redis database number
     *
     * @var int
     */
    public $database = 0;

    /**
     * Default Redis connection timeout
     *
     * @var int
     */
    public $timeout = 5;

    /**
     * Cache Redis connection host
     *
     * @var string
     */
    public $cacheHost = '127.0.0.1';

    /**
     * Cache Redis connection port
     *
     * @var int
     */
    public $cachePort = 6379;

    /**
     * Cache Redis connection password
     *
     * @var string|null
     */
    public $cachePassword = null;

    /**
     * Cache Redis database number
     *
     * @var int
     */
    public $cacheDatabase = 1;

    /**
     * Cache Redis connection timeout
     *
     * @var int
     */
    public $cacheTimeout = 5;

    /**
     * Queue Redis connection host
     *
     * @var string
     */
    public $queueHost = '127.0.0.1';

    /**
     * Queue Redis connection port
     *
     * @var int
     */
    public $queuePort = 6379;

    /**
     * Queue Redis connection password
     *
     * @var string|null
     */
    public $queuePassword = null;

    /**
     * Queue Redis database number
     *
     * @var int
     */
    public $queueDatabase = 2;

    /**
     * Queue Redis connection timeout
     *
     * @var int
     */
    public $queueTimeout = 5;

    /**
     * Default queue name
     *
     * @var string
     */
    public $defaultQueue = 'default';

    /**
     * Maximum retry attempts for failed jobs
     *
     * @var int
     */
    public $maxAttempts = 3;

    /**
     * Base retry delay in seconds
     *
     * @var int
     */
    public $retryDelay = 60;

    /**
     * Job Time-To-Live in seconds
     *
     * @var int
     */
    public $jobTtl = 86400;

    /**
     * Cleanup interval in seconds
     *
     * @var int
     */
    public $cleanupInterval = 3600;

    /**
     * Get queue Redis connection configuration
     *
     * @return array
     */
    public function getQueueConfig(): array
    {
        return [
            'host' => $this->queueHost,
            'port' => $this->queuePort,
            'password' => $this->queuePassword,
            'database' => $this->queueDatabase,
            'timeout' => $this->queueTimeout,
        ];
    }

    /**
     * Get queue options
     *
     * @return array
     */
    public function getOptions(): array
    {
        return [
            'default_queue' => $this->defaultQueue,
            'max_attempts' => $this->maxAttempts,
            'retry_delay' => $this->retryDelay,
            'job_ttl' => $this->jobTtl,
            'cleanup_interval' => $this->cleanupInterval,
        ];
    }
}