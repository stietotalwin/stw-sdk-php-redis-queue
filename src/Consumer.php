<?php

namespace StieTotalWin\RedisQueue;

class Consumer
{
    private $redisConnection;
    private $redis;

    public function __construct(\StieTotalWin\RedisQueue\Config\RedisQueue $config)
    {
        $redisConfig = $config->getRedisConfig();
        $this->redisConnection = RedisConnection::getInstance($redisConfig);
        $this->redis = $this->redisConnection->getClient();
    }

    public function consume(string $queueName): ?Job
    {
        $currentTime = time();
        $maxRetries = 5;

        for ($i = 0; $i < $maxRetries; $i++) {
            $jobs = $this->redis->zrangebyscore(
                $queueName,
                0,
                $currentTime,
                ['limit' => [0, 1]]
            );

            if (empty($jobs)) {
                return null;
            }

            $jobId = $jobs[0];

            $removed = $this->redis->zrem($queueName, $jobId);

            if ($removed === 0) {
                continue;
            }

            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

            if (!$jobData) {
                continue;
            }

            $job = Job::fromArray(json_decode($jobData, true));

            $this->redis->zadd($queueName . ':processing', [$jobId => time()]);

            $job->setStatus('processing');
            $this->updateJobData($job, $queueName);

            return $job;
        }

        return null;
    }

    public function markCompleted(string $jobId, string $queueName): bool
    {
        $this->redis->zrem($queueName . ':processing', $jobId);
        $this->redis->hdel($queueName . ':jobs', [$jobId]);

        return true;
    }

    public function markFailed(string $jobId, string $queueName, string $errorMessage = null): bool
    {
        $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

        if (!$jobData) {
            return false;
        }

        $job = Job::fromArray(json_decode($jobData, true));
        $job->incrementAttempts();
        $job->setStatus('failed');

        if ($errorMessage) {
            $job->setError($errorMessage);
        }

        if ($job->canRetry()) {
            $retryDelay = pow(2, $job->getAttempts()) * 60;
            $job->setProcessAt(time() + $retryDelay);
            $job->setStatus('retrying');

            $this->redis->zadd($queueName, [$job->getId() => $job->getProcessAt()]);
        } else {
            $this->redis->lpush($queueName . ':failed', $job->toArray());
            $this->redis->zrem($queueName . ':processing', $jobId);
        }

        $this->updateJobData($job, $queueName);

        return true;
    }

    public function getFailedJobs(string $queueName, int $limit = 10): array
    {
        $failedJobs = $this->redis->lrange($queueName . ':failed', 0, $limit - 1);
        $jobs = [];

        foreach ($failedJobs as $jobData) {
            $jobs[] = Job::fromArray(json_decode($jobData, true));
        }

        return $jobs;
    }

    public function retryFailedJobs(string $queueName, int $limit = null): int
    {
        $count = 0;

        while (true) {
            $jobData = $this->redis->rpop($queueName . ':failed');
            if (!$jobData) {
                break;
            }

            $job = Job::fromArray(json_decode($jobData, true));
            $job->setStatus('pending');
            $job->setProcessAt(time());

            $this->redis->zadd($queueName, [$job->getId() => $job->getProcessAt()]);
            $this->updateJobData($job, $queueName);

            $count++;
            if ($limit && $count >= $limit) {
                break;
            }
        }

        return $count;
    }

    public function getProcessingJobs(string $queueName, int $limit = 10): array
    {
        $processingJobIds = $this->redis->zrange($queueName . ':processing', 0, $limit - 1);
        $jobs = [];

        foreach ($processingJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
            if ($jobData) {
                $jobs[] = Job::fromArray(json_decode($jobData, true));
            }
        }

        return $jobs;
    }

    public function cleanupExpiredJobs(string $queueName, int $maxAge = 86400): int
    {
        $expiredTime = time() - $maxAge;
        $removedFromProcessing = $this->redis->zremrangebyscore($queueName . ':processing', 0, $expiredTime);

        $allJobIds = $this->redis->hkeys($queueName . ':jobs');
        $expiredCount = 0;

        foreach ($allJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
            if ($jobData) {
                $job = Job::fromArray(json_decode($jobData, true));
                if ($job->isExpired($maxAge)) {
                    $this->redis->hdel($queueName . ':jobs', [$jobId]);
                    $this->redis->zrem($queueName, $jobId);
                    $expiredCount++;
                }
            }
        }

        return $expiredCount;
    }

    public function getPendingJobs(string $queueName, int $limit = 10): array
    {
        $pendingJobIds = $this->redis->zrange($queueName, 0, $limit - 1);
        $jobs = [];

        foreach ($pendingJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
            if ($jobData) {
                $jobs[] = Job::fromArray(json_decode($jobData, true));
            }
        }

        return $jobs;
    }

    public function getQueueStats(string $queueName): array
    {
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

    private function updateJobData(Job $job, string $queueName): void
    {
        $this->redis->hset($queueName . ':jobs', $job->getId(), json_encode($job->toArray()));
    }
}