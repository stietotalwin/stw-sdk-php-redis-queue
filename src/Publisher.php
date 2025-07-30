<?php

namespace StieTotalWin\RedisQueue;

use Ramsey\Uuid\Uuid;

class Publisher
{
    private $redisConnection;
    private $redis;
    private $defaultQueue;
    private $currentQueue;

    public function __construct(\StieTotalWin\RedisQueue\Config\RedisQueue $config, string $defaultQueue = 'default')
    {
        $redisConfig = $config->getRedisConfig();
        $this->redisConnection = RedisConnection::getInstance($redisConfig);
        $this->redis = $this->redisConnection->getClient();
        $this->defaultQueue = $defaultQueue;
        $this->currentQueue = $defaultQueue;
    }

    public function setQueue(string $queueName): self
    {
        $this->currentQueue = $queueName;
        return $this;
    }

    public function publish(string $type, array $data, int $delay = 0, string $queue = null): string
    {
        $queueName = $queue ?? $this->currentQueue;
        $jobId = Uuid::uuid4()->toString();
        $timestamp = time();

        $job = new Job(
            $jobId,
            $type,
            $data,
            $timestamp,
            $timestamp + $delay
        );

        $this->addJobToQueue($job, $queueName);

        $this->currentQueue = $this->defaultQueue;

        return $jobId;
    }

    public function publishBulk(array $jobs, string $queue = null): array
    {
        $queueName = $queue ?? $this->currentQueue;
        $jobIds = [];

        foreach ($jobs as $jobData) {
            $type = $jobData['type'] ?? '';
            $data = $jobData['data'] ?? [];
            $delay = $jobData['delay'] ?? 0;

            if (empty($type)) {
                continue;
            }

            $jobIds[] = $this->publish($type, $data, $delay, $queueName);
        }

        $this->currentQueue = $this->defaultQueue;

        return $jobIds;
    }

    public function getQueueStats(string $queue = null): array
    {
        $queueName = $queue ?? $this->currentQueue;

        $pendingJobs = $this->redis->zcard($queueName);
        $processingJobs = $this->redis->zcard($queueName . ':processing');
        $failedJobs = $this->redis->llen($queueName . ':failed');
        $totalJobData = $this->redis->hlen($queueName . ':jobs');

        return [
            'queue_name' => $queueName,
            'pending_jobs' => $pendingJobs,
            'processing_jobs' => $processingJobs,
            'failed_jobs' => $failedJobs,
            'total_job_data' => $totalJobData
        ];
    }

    public function clearQueue(string $queue = null): bool
    {
        $queueName = $queue ?? $this->currentQueue;

        $this->redis->del($queueName);
        $this->redis->del($queueName . ':jobs');
        $this->redis->del($queueName . ':failed');
        $this->redis->del($queueName . ':processing');

        return true;
    }

    public function getJobCount(string $queue = null): int
    {
        $queueName = $queue ?? $this->currentQueue;
        return $this->redis->zcard($queueName);
    }

    public function getQueueNames(): array
    {
        $keys = $this->redis->keys('*');
        $queueNames = [];

        foreach ($keys as $key) {
            if (strpos($key, ':') === false) {
                $queueNames[] = $key;
            }
        }

        return array_unique($queueNames);
    }

    public function deleteJob(string $jobId, string $queue = null): bool
    {
        $queueName = $queue ?? $this->currentQueue;

        $removed = $this->redis->zrem($queueName, $jobId);
        $this->redis->hdel($queueName . ':jobs', [$jobId]);

        return $removed > 0;
    }

    public function getJob(string $jobId, string $queue = null): ?Job
    {
        $queueName = $queue ?? $this->currentQueue;
        $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

        if (!$jobData) {
            return null;
        }

        return Job::fromArray(json_decode($jobData, true));
    }

    private function addJobToQueue(Job $job, string $queueName): void
    {
        $this->redis->zadd($queueName, [$job->getId() => $job->getProcessAt()]);
        $this->redis->hset($queueName . ':jobs', $job->getId(), json_encode($job->toArray()));
    }
}