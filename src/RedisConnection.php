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

    private function connect(): void
    {
        $redisConfig = [
            'scheme' => $this->config['scheme'] ?? 'tcp',
            'host' => $this->config['host'] ?? '127.0.0.1',
            'port' => $this->config['port'] ?? 6379,
            'database' => $this->config['database'] ?? 0,
        ];

        if (isset($this->config['username']) && !empty($this->config['username'])) {
            $redisConfig['username'] = $this->config['username'];
        }

        if (isset($this->config['password']) && !empty($this->config['password'])) {
            $redisConfig['password'] = $this->config['password'];
        }

        if (isset($this->config['timeout'])) {
            $redisConfig['timeout'] = $this->config['timeout'];
        }

        if (isset($this->config['read_write_timeout'])) {
            $redisConfig['read_write_timeout'] = $this->config['read_write_timeout'];
        }

        if (isset($this->config['parameters']) && is_array($this->config['parameters'])) {
            $redisConfig['parameters'] = $this->config['parameters'];
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