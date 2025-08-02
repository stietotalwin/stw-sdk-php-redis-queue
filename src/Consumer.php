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

    public function consume(string $queueName, int $timeout = 10): ?Job
    {
        // Check for data structure consistency and attempt recovery if needed
        $this->ensureQueueConsistency($queueName);
        
        // Suppress warnings from Predis when bzpopmin returns null on timeout
        $result = @$this->redis->bzpopmin([$queueName], $timeout);

        if ($result === null || empty($result) || !is_array($result)) {
            return null;
        }

        if (!isset($result[$queueName])) {
            return null;
        }

        $queueData = $result[$queueName];
        if (empty($queueData) || !is_array($queueData)) {
            return null;
        }

        $jobId = array_keys($queueData)[0];
        $score = (int) $queueData[$jobId];
        $currentTime = time();

        if ($score > $currentTime) {
            $this->redis->zadd($queueName, [$jobId => $score]);
            return null;
        }

        $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

        if (!$jobData) {
            return null;
        }

        $job = Job::fromArray(json_decode($jobData, true));

        $this->redis->zadd($queueName . ':processing', [$jobId => time()]);

        $job->setStatus('processing');
        $this->updateJobData($job, $queueName);

        return $job;
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

    private function ensureQueueConsistency(string $queueName): void
    {
        $queueExists = $this->redis->exists($queueName);
        $jobsHashKey = $queueName . ':jobs';
        $jobsHashExists = $this->redis->exists($jobsHashKey);
        
        // If jobs hash exists but queue doesn't, attempt recovery
        if (!$queueExists && $jobsHashExists) {
            $this->recoverQueueFromJobs($queueName);
        }
    }

    private function recoverQueueFromJobs(string $queueName): bool
    {
        $jobsHashKey = $queueName . ':jobs';
        $jobIds = $this->redis->hkeys($jobsHashKey);
        
        if (empty($jobIds)) {
            return false;
        }

        $currentTime = time();
        $recoveredCount = 0;

        foreach ($jobIds as $jobId) {
            $jobData = $this->redis->hget($jobsHashKey, $jobId);
            
            if (!$jobData) {
                continue;
            }

            $job = json_decode($jobData, true);
            
            if (!$job) {
                continue;
            }

            // Use processAt time if available, otherwise use current time
            $processAt = isset($job['processAt']) ? $job['processAt'] : $currentTime;
            
            // Add job to the sorted set with its process time as score
            $this->redis->zadd($queueName, [$jobId => $processAt]);
            $recoveredCount++;
        }

        if ($recoveredCount > 0) {
            error_log("QUEUE RECOVERY: Restored $recoveredCount jobs to queue $queueName");
        }

        return $recoveredCount > 0;
    }
}