<?php

namespace StieTotalWin\RedisQueue\Test;

require_once __DIR__ . '/../vendor/autoload.php';

use StieTotalWin\RedisQueue\Consumer;
use StieTotalWin\RedisQueue\Config\RedisQueue;

class ConsumerTimeoutTest
{
    private $queueName = 'test_consumer_timeout_queue';

    public function testTimeoutUsesRawCommandAndReturnsNull(): void
    {
        $redis = new FakeRedisClient(null);
        $consumer = $this->consumerWithRedis($redis);

        $job = $consumer->consume($this->queueName, 2);

        $this->assertSame(null, $job, 'Timeout should return null');
        $this->assertSame(1, $redis->executeRawCalls, 'executeRaw should be used for blocking pop');
        $this->assertSame(0, $redis->bzpopminCalls, 'bzpopmin should not be called directly');
        $this->assertSame(['BZPOPMIN', $this->queueName, '2'], $redis->lastRawArguments, 'Raw command arguments should match BZPOPMIN signature');
    }

    public function testSingleNullArrayTimeoutReturnsNull(): void
    {
        $redis = new FakeRedisClient([null]);
        $consumer = $this->consumerWithRedis($redis);

        $job = $consumer->consume($this->queueName, 2);

        $this->assertSame(null, $job, 'Single-null BZPOPMIN response should be treated as timeout');
        $this->assertSame(1, $redis->executeRawCalls, 'executeRaw should be used for blocking pop');
        $this->assertSame(0, $redis->bzpopminCalls, 'bzpopmin should not be called directly');
    }

    public function testValidRawResponseConsumesJob(): void
    {
        $now = time();
        $jobData = json_encode([
            'id' => 'job-1',
            'type' => 'email',
            'data' => '{"message":"hello"}',
            'created_at' => $now,
            'process_at' => $now,
            'attempts' => 0,
            'max_attempts' => 3,
            'status' => 'pending',
            'error' => null,
            'updated_at' => $now,
        ]);

        $redis = new FakeRedisClient([$this->queueName, 'job-1', (string) $now]);
        $redis->evalResult = ['consumed', 'job-1', $jobData];
        $consumer = $this->consumerWithRedis($redis);

        $job = $consumer->consume($this->queueName, 2);

        $this->assertSame('job-1', $job->getId(), 'Job ID should come from raw response and job hash data');
        $this->assertSame('email', $job->getType(), 'Job type should be decoded from job data');
        $this->assertSame(1, $redis->evalCalls, 'Atomic validation Lua script should still run');
        $this->assertSame(1, $redis->hsetCalls, 'Consumed job data should be updated');
    }

    public function testRedisErrorResponseThrowsRuntimeException(): void
    {
        $redis = new FakeRedisClient('ERR invalid timeout');
        $redis->rawError = true;
        $consumer = $this->consumerWithRedis($redis);

        try {
            $consumer->consume($this->queueName, 2);
        } catch (\RuntimeException $e) {
            $this->assertSame('BZPOPMIN failed: ERR invalid timeout', $e->getMessage(), 'Redis error should surface as RuntimeException');
            return;
        }

        throw new \Exception('Expected RuntimeException was not thrown');
    }

    private function consumerWithRedis(FakeRedisClient $redis): Consumer
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

    public function runAllTests(): bool
    {
        $tests = [
            'Timeout uses raw command' => 'testTimeoutUsesRawCommandAndReturnsNull',
            'Single-null timeout response' => 'testSingleNullArrayTimeoutReturnsNull',
            'Valid raw response consumes job' => 'testValidRawResponseConsumesJob',
            'Redis error response throws' => 'testRedisErrorResponseThrowsRuntimeException',
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

class FakeRedisClient
{
    public $executeRawCalls = 0;
    public $bzpopminCalls = 0;
    public $evalCalls = 0;
    public $hsetCalls = 0;
    public $lastRawArguments = [];
    public $evalResult = null;
    public $rawError = false;
    private $rawResponse;

    public function __construct($rawResponse)
    {
        $this->rawResponse = $rawResponse;
    }

    public function exists(string $key): int
    {
        return 0;
    }

    public function createCommand(string $commandId, array $arguments): FakeRedisCommand
    {
        if ($commandId !== 'BZPOPMIN') {
            throw new \Exception('Unexpected command: ' . $commandId);
        }

        $keys = $arguments[0];
        $timeout = $arguments[1];

        return new FakeRedisCommand([$keys[0], (string) $timeout]);
    }

    public function executeRaw(array $arguments, &$error = null)
    {
        $this->executeRawCalls++;
        $this->lastRawArguments = $arguments;
        $error = $this->rawError;

        return $this->rawResponse;
    }

    public function bzpopmin(array $keys, int $timeout)
    {
        $this->bzpopminCalls++;
        throw new \Exception('bzpopmin should not be called directly');
    }

    public function eval(string $script, int $numKeys, string $queueName, string $jobId, int $score, int $currentTime)
    {
        $this->evalCalls++;

        return $this->evalResult;
    }

    public function hset(string $key, string $field, string $value): int
    {
        $this->hsetCalls++;

        return 1;
    }
}

class FakeRedisCommand
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
    $test = new ConsumerTimeoutTest();
    exit($test->runAllTests() ? 0 : 1);
}
