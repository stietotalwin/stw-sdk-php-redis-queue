<?php

namespace StieTotalWin\RedisQueue;

use Predis\Client;

class RedisConnection
{
    private static $instances = [];
    private $client;
    private $config;

    private function __construct(array $config)
    {
        $this->config = $config;
        $this->connect();
    }

    public static function getInstance(array $config): self
    {
        $hash = md5(serialize($config));
        
        if (!isset(self::$instances[$hash])) {
            self::$instances[$hash] = new self($config);
        }
        
        return self::$instances[$hash];
    }

    /**
     * Remove a specific instance from the pool
     */
    public static function removeInstance(array $config): void
    {
        $hash = md5(serialize($config));

        if (isset(self::$instances[$hash])) {
            self::$instances[$hash]->client->disconnect();
            unset(self::$instances[$hash]);
        }
    }

    /**
     * Remove all instances from the pool (useful for long-running workers)
     */
    public static function resetAll(): void
    {
        foreach (self::$instances as $instance) {
            if ($instance->client) {
                $instance->client->disconnect();
            }
        }
        self::$instances = [];
    }

    private function connect(): void
    {
        // If redis_url exists, use it directly (tcp)
        if (!empty($this->config['redis_url'])) {
            $this->client = new Client($this->config['redis_url']);
            return;
        }

        $redisConfig = [
            'scheme' => $this->config['scheme'] ?? 'tcp',
            'host' => $this->config['host'] ?? '127.0.0.1',
            'port' => $this->config['port'] ?? 6379,
        ];

        // Add database only if specified (optional for cloud services)
        if (isset($this->config['database'])) {
            $redisConfig['database'] = $this->config['database'];
        }

        // Support 'user' field (used by Upstash and other cloud services)
        if (isset($this->config['user']) && !empty($this->config['user'])) {
            $redisConfig['username'] = $this->config['user'];
        }

        if (isset($this->config['password']) && !empty($this->config['password'])) {
            $redisConfig['password'] = $this->config['password'];
        }

        // Use parameters array directly as per Predis documentation
        if (isset($this->config['parameters']) && is_array($this->config['parameters'])) {
            $redisConfig['parameters'] = $this->config['parameters'];
        }

        // Add SSL context for TLS connections (required for cloud services like Upstash)
        if ($redisConfig['scheme'] === 'tls' || $redisConfig['scheme'] === 'rediss') {
            if (!isset($redisConfig['parameters'])) {
                $redisConfig['parameters'] = [];
            }

            $sslConfig = $this->config['ssl'] ?? [];
            $redisConfig['parameters']['ssl'] = [
                'verify_peer' => $sslConfig['verify_peer'] ?? true,
                'verify_peer_name' => $sslConfig['verify_peer_name'] ?? true,
                'allow_self_signed' => $sslConfig['allow_self_signed'] ?? false,
                'crypto_method' => STREAM_CRYPTO_METHOD_TLS_CLIENT
            ];
        }

        $this->client = new Client($redisConfig);
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    public function isConnected(): bool
    {
        try {
            $this->client->ping();
            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    public function reconnect(): void
    {
        $this->connect();
    }

    public function __destruct()
    {
        if ($this->client) {
            $this->client->disconnect();
        }
    }
}