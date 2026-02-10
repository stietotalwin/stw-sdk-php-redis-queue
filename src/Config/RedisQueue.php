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
     * Redis connection user (used by cloud services like Upstash)
     *
     * @var string|null
     */
    public $user = null;

    /**
     * Redis connection password
     *
     * @var string|null
     */
    public $password = null;

    /**
     * Redis database number (optional for cloud services)
     *
     * @var int|null
     */
    public $database = null;

    /**
     * Additional Redis parameters including timeouts
     *
     * @var array
     */
    public $parameters = [
        'timeout' => 30,
        'read_write_timeout' => 30,
    ];

    /**
     * SSL/TLS verification settings for secure connections
     *
     * @var array
     */
    public $ssl = [
        'verify_peer' => true,
        'verify_peer_name' => true,
        'allow_self_signed' => false,
    ];

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
        ];

        // Add database only if specified
        if ($this->database !== null) {
            $config['database'] = $this->database;
        }

        if (!empty($this->user)) {
            $config['user'] = $this->user;
        }

        if (!empty($this->password)) {
            $config['password'] = $this->password;
        }

        if (!empty($this->parameters)) {
            $config['parameters'] = $this->parameters;
        }

        if (!empty($this->ssl)) {
            $config['ssl'] = $this->ssl;
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