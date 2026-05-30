<?php

namespace StieTotalWin\RedisQueue\Test;

require_once __DIR__ . '/../vendor/autoload.php';

use StieTotalWin\RedisQueue\Config\RedisQueue;
use StieTotalWin\RedisQueue\Consumer;

class ConsumerFailureRecoveryTest
{
    private $queueName = 'test_failure_recovery_queue';

    public function testRetryableFailureStaysInJobsHashAndReturnsToQueue(): void
    {
        $now = time();
        $redis = new FailureRecoveryFakeRedis();
        $redis->jobs[$this->queueName . ':jobs']['job-retry'] = json_encode([
            'id' => 'job-retry',
            'type' => 'email',
            'data' => '{"message":"retry"}',
            'created_at' => $now,
            'process_at' => $now,
            'attempts' => 0,
            'max_attempts' => 3,
            'status' => 'processing',
            'error' => null,
            'updated_at' => $now,
        ]);

        $consumer = $this->consumerWithRedis($redis);

        $result = $consumer->markFailed('job-retry', $this->queueName, 'temporary failure');

        $storedJob = json_decode($redis->jobs[$this->queueName . ':jobs']['job-retry'], true);

        $this->assertSame(true, $result, 'Retryable failure should be acknowledged');
        $this->assertSame(1, $storedJob['attempts'], 'Retryable failure should increment attempts');
        $this->assertSame('retrying', $storedJob['status'], 'Retryable failure should stay retrying');
        $this->assertSame('temporary failure', $storedJob['error'], 'Retryable failure should store the error');
        $this->assertTrue(isset($redis->zsets[$this->queueName]['job-retry']), 'Retryable failure should be returned to the main queue');
        $this->assertTrue($redis->zsets[$this->queueName]['job-retry'] > $now, 'Retryable failure should be delayed before retry');
        $this->assertSame([], $redis->lists[$this->queueName . ':failed'] ?? [], 'Retryable failure should not be pushed to failed jobs');
    }

    public function testTerminalFailureMovesToFailedListAndLeavesJobsHash(): void
    {
        $now = time();
        $redis = new FailureRecoveryFakeRedis();
        $redis->jobs[$this->queueName . ':jobs']['job-terminal'] = json_encode([
            'id' => 'job-terminal',
            'type' => 'bni_callback',
            'data' => '{"trx_id":"TRX-1"}',
            'created_at' => $now,
            'process_at' => $now,
            'attempts' => 2,
            'max_attempts' => 3,
            'status' => 'processing',
            'error' => null,
            'updated_at' => $now,
        ]);

        $consumer = $this->consumerWithRedis($redis);

        $result = $consumer->markFailed('job-terminal', $this->queueName, 'permanent failure');
        $failedJob = json_decode($redis->lists[$this->queueName . ':failed'][0], true);

        $this->assertSame(true, $result, 'Terminal failure should be acknowledged');
        $this->assertSame('job-terminal', $failedJob['id'], 'Terminal failure should be pushed to failed jobs');
        $this->assertSame(3, $failedJob['attempts'], 'Terminal failure should record final attempt count');
        $this->assertSame('failed', $failedJob['status'], 'Terminal failure should be marked failed');
        $this->assertSame('permanent failure', $failedJob['error'], 'Terminal failure should store the error');
        $this->assertFalse(isset($redis->jobs[$this->queueName . ':jobs']['job-terminal']), 'Terminal failure should be removed from jobs hash');
        $this->assertFalse(isset($redis->zsets[$this->queueName . ':processing']['job-terminal']), 'Terminal failure should be removed from processing queue');
    }

    public function testRecoverySkipsAndCleansStaleTerminalFailedJobs(): void
    {
        $now = time();
        $redis = new FailureRecoveryFakeRedis();
        $redis->exists[$this->queueName] = 0;
        $redis->exists[$this->queueName . ':jobs'] = 1;
        $redis->rawResponse = null;
        $redis->jobs[$this->queueName . ':jobs']['job-stale-failed'] = json_encode([
            'id' => 'job-stale-failed',
            'type' => 'bni_callback',
            'data' => '{"trx_id":"TRX-2"}',
            'created_at' => $now,
            'process_at' => $now,
            'attempts' => 3,
            'max_attempts' => 3,
            'status' => 'failed',
            'error' => 'old permanent failure',
            'updated_at' => $now,
        ]);

        $consumer = $this->consumerWithRedis($redis);

        $job = $consumer->consume($this->queueName, 1);

        $this->assertSame(null, $job, 'No job should be consumed from an empty queue');
        $this->assertFalse(isset($redis->zsets[$this->queueName]['job-stale-failed']), 'Recovery should not restore stale terminal failures');
        $this->assertFalse(isset($redis->jobs[$this->queueName . ':jobs']['job-stale-failed']), 'Recovery should clean stale terminal failures from jobs hash');
    }

    private function consumerWithRedis(FailureRecoveryFakeRedis $redis): Consumer
    {
        $config = new class extends RedisQueue {
            public function __construct()
            {
                $this->redis_url = 'tcp://127.0.0.1:6379';
            }
        };

        $consumer = new Consumer($config);

        $property = new \ReflectionProperty(Consumer::class, 'redis');
        $property->setAccessible(true);
        $property->setValue($consumer, $redis);

        return $consumer;
    }

    private function assertSame($expected, $actual, string $message): void
    {
        if ($expected !== $actual) {
            throw new \Exception($message . '. Expected ' . var_export($expected, true) . ', got ' . var_export($actual, true));
        }
    }

    private function assertTrue(bool $actual, string $message): void
    {
        if (!$actual) {
            throw new \Exception($message);
        }
    }

    private function assertFalse(bool $actual, string $message): void
    {
        if ($actual) {
            throw new \Exception($message);
        }
    }

    public function runAllTests(): bool
    {
        $tests = [
            'Retryable failure remains retryable' => 'testRetryableFailureStaysInJobsHashAndReturnsToQueue',
            'Terminal failure leaves jobs hash' => 'testTerminalFailureMovesToFailedListAndLeavesJobsHash',
            'Recovery skips stale terminal failures' => 'testRecoverySkipsAndCleansStaleTerminalFailedJobs',
        ];

        foreach ($tests as $name => $method) {
            try {
                $this->$method();
                echo "PASS: {$name}\n";
            } catch (\Throwable $e) {
                echo "FAIL: {$name} - " . $e->getMessage() . "\n";
                return false;
            }
        }

        return true;
    }
}

class FailureRecoveryFakeRedis
{
    public $exists = [];
    public $jobs = [];
    public $lists = [];
    public $zsets = [];
    public $rawResponse = null;

    public function hget(string $key, string $field)
    {
        return $this->jobs[$key][$field] ?? null;
    }

    public function hset(string $key, string $field, string $value): int
    {
        $this->jobs[$key][$field] = $value;

        return 1;
    }

    public function hdel(string $key, array $fields): int
    {
        $removed = 0;

        foreach ($fields as $field) {
            if (isset($this->jobs[$key][$field])) {
                unset($this->jobs[$key][$field]);
                $removed++;
            }
        }

        return $removed;
    }

    public function lpush(string $key, array $values): int
    {
        if (!isset($this->lists[$key])) {
            $this->lists[$key] = [];
        }

        foreach ($values as $value) {
            array_unshift($this->lists[$key], $value);
        }

        return count($this->lists[$key]);
    }

    public function zadd(string $key, array $members): int
    {
        if (!isset($this->zsets[$key])) {
            $this->zsets[$key] = [];
        }

        foreach ($members as $member => $score) {
            $this->zsets[$key][$member] = $score;
        }

        return count($members);
    }

    public function zrem(string $key, string $member): int
    {
        if (isset($this->zsets[$key][$member])) {
            unset($this->zsets[$key][$member]);

            return 1;
        }

        return 0;
    }

    public function zscore(string $key, string $member)
    {
        return $this->zsets[$key][$member] ?? null;
    }

    public function exists(string $key): int
    {
        return $this->exists[$key] ?? 0;
    }

    public function hscan(string $key, string &$cursor, array $options)
    {
        $cursor = '0';

        return ['0', $this->jobs[$key] ?? []];
    }

    public function createCommand(string $commandId, array $arguments): FailureRecoveryFakeRedisCommand
    {
        $keys = $arguments[0];
        $timeout = $arguments[1];

        return new FailureRecoveryFakeRedisCommand([$keys[0], (string) $timeout]);
    }

    public function executeRaw(array $arguments, &$error = null)
    {
        $error = false;

        return $this->rawResponse;
    }
}

class FailureRecoveryFakeRedisCommand
{
    private $arguments;

    public function __construct(array $arguments)
    {
        $this->arguments = $arguments;
    }

    public function getArguments(): array
    {
        return $this->arguments;
    }
}

if (basename(__FILE__) === basename($_SERVER['SCRIPT_NAME'] ?? '')) {
    $test = new ConsumerFailureRecoveryTest();
    exit($test->runAllTests() ? 0 : 1);
}
