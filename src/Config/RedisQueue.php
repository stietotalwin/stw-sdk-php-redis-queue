<?php

namespace StieTotalWin\RedisQueue\Config;

use CodeIgniter\Config\BaseConfig;

/**
 * Redis Queue Configuration
 */
class RedisQueue extends BaseConfig
{
    /**
     * Redis connection scheme (tcp, tls, unix)
     *
     * @var string
     */
    public $scheme = 'tcp';

    /**
     * Redis connection host
     *
     * @var string
     */
    public $host = '127.0.0.1';

    /**
     * Redis connection port
     *
     * @var int
     */
    public $port = 6379;

    /**
     * Redis connection username
     *
     * @var string|null
     */
    public $username = null;

    /**
     * Redis connection password
     *
     * @var string|null
     */
    public $password = null;

    /**
     * Redis database number
     *
     * @var int
     */
    public $database = 0;

    /**
     * Redis connection timeout
     *
     * @var int
     */
    public $timeout = 5;

    /**
     * Redis read/write timeout
     *
     * @var int
     */
    public $readWriteTimeout = 30;

    /**
     * Additional Redis parameters
     *
     * @var array
     */
    public $parameters = [];

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
     * Get Redis connection configuration
     *
     * @return array
     */
    public function getRedisConfig(): array
    {
        $config = [
            'scheme' => $this->scheme,
            'host' => $this->host,
            'port' => $this->port,
            'database' => $this->database,
            'timeout' => $this->timeout,
        ];

        if (!empty($this->username)) {
            $config['username'] = $this->username;
        }

        if (!empty($this->password)) {
            $config['password'] = $this->password;
        }

        if (!empty($this->readWriteTimeout)) {
            $config['read_write_timeout'] = $this->readWriteTimeout;
        }

        if (!empty($this->parameters)) {
            $config['parameters'] = $this->parameters;
        }

        return $config;
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