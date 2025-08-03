<?php

namespace StieTotalWin\RedisQueue;

class Consumer
{
    private $redisConnection;
    private $redis;
    private $detailedLogging = false;

    public function __construct(\StieTotalWin\RedisQueue\Config\RedisQueue $config)
    {
        $redisConfig = $config->getRedisConfig();
        $this->redisConnection = RedisConnection::getInstance($redisConfig);
        $this->redis = $this->redisConnection->getClient();
    }

    public function consume(string $queueName, int $timeout = 10): ?Job
    {
        $currentTime = time();
        
        // For blocking operation, we need to use bzpopmin outside Lua
        // but we can make the validation and processing atomic
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
        
        // Lua script for atomic validation and processing
        $luaScript = '
            local queueName = ARGV[1]
            local jobId = ARGV[2]
            local score = tonumber(ARGV[3])
            local currentTime = tonumber(ARGV[4])
            local processingQueue = queueName .. ":processing"
            local jobsHash = queueName .. ":jobs"
            
            -- Check if job is ready
            if score > currentTime then
                -- Put it back, not ready yet
                redis.call("ZADD", queueName, score, jobId)
                return {"requeued", jobId, score}
            end
            
            -- Check if job data exists
            local jobData = redis.call("HGET", jobsHash, jobId)
            if not jobData then
                -- Job data missing, put back for potential recovery
                redis.call("ZADD", queueName, currentTime, jobId)
                return {"missing", jobId}
            end
            
            -- Move to processing queue atomically
            redis.call("ZADD", processingQueue, currentTime, jobId)
            
            return {"consumed", jobId, jobData}
        ';
        
        try {
            $result = $this->redis->eval($luaScript, 0, $queueName, $jobId, $score, $currentTime);
            
            if ($result === null) {
                return null;
            }
            
            $status = $result[0];
            $jobId = $result[1];
            
            switch ($status) {
                case 'requeued':
                    $score = $result[2];
                    $this->log("JOB_REQUEUED", "Job not ready, requeued", ['job_id' => $jobId, 'score' => $score]);
                    return null;
                    
                case 'missing':
                    $this->log("JOB_LOST", "Job data missing from hash, requeued for recovery", ['job_id' => $jobId]);
                    return null;
                    
                case 'consumed':
                    $jobData = $result[2];
                    $decodedData = json_decode($jobData, true);
                    
                    if (!$decodedData) {
                        $this->log("ERROR", "Job has invalid JSON data, moving to failed queue", ['job_id' => $jobId, 'raw_data' => $jobData]);
                        $this->redis->lpush($queueName . ':failed', [json_encode([
                            'id' => $jobId,
                            'error' => 'Corrupted job data - invalid JSON',
                            'raw_data' => $jobData,
                            'failed_at' => time()
                        ])]);
                        // Remove from processing since it's corrupted
                        $this->redis->zrem($queueName . ':processing', $jobId);
                        return null;
                    }
                    
                    $job = Job::fromArray($decodedData);
                    $job->setStatus('processing');
                    $this->updateJobData($job, $queueName);
                    
                    $this->log("JOB_CONSUMED", "Job atomically moved to processing", ['job_id' => $jobId, 'type' => $job->getType()]);
                    return $job;
                    
                default:
                    $this->log("ERROR", "Unknown status from atomic consume", ['status' => $status, 'job_id' => $jobId]);
                    return null;
            }
            
        } catch (\Exception $e) {
            $this->log("ERROR", "Atomic consume failed", ['job_id' => $jobId ?? 'unknown', 'error' => $e->getMessage()]);
            
            // Try to recover the job if we have the jobId
            if (isset($jobId)) {
                try {
                    $this->redis->zadd($queueName, [$jobId => $currentTime]);
                    $this->log("RECOVERY", "Job restored to queue after exception", ['job_id' => $jobId]);
                } catch (\Exception $recoveryException) {
                    $this->log("JOB_LOST", "Failed to recover job", [
                        'job_id' => $jobId,
                        'original_error' => $e->getMessage(),
                        'recovery_error' => $recoveryException->getMessage()
                    ]);
                    
                    // Save to lost queue for manual recovery
                    try {
                        $this->redis->lpush($queueName . ':lost', [json_encode([
                            'id' => $jobId,
                            'score' => $score ?? $currentTime,
                            'error' => $e->getMessage(),
                            'recovery_error' => $recoveryException->getMessage(),
                            'lost_at' => time()
                        ])]);
                    } catch (\Exception $logException) {
                        $this->log("ERROR", "Could not even log lost job", ['job_id' => $jobId]);
                    }
                }
            }
            
            return null;
        }
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

    public function cleanupExpiredJobs(string $queueName, int $maxAge = 86400, int $batchSize = 100): int
    {
        $expiredTime = time() - $maxAge;
        $this->redis->zremrangebyscore($queueName . ':processing', 0, $expiredTime);

        $allJobIds = $this->redis->hkeys($queueName . ':jobs');
        if (empty($allJobIds)) {
            return 0;
        }

        $expiredCount = 0;
        $batches = array_chunk($allJobIds, $batchSize);

        foreach ($batches as $batch) {
            $jobDataBatch = $this->redis->hmget($queueName . ':jobs', $batch);
            $expiredJobIds = [];

            for ($i = 0; $i < count($batch); $i++) {
                $jobData = $jobDataBatch[$i];
                if ($jobData) {
                    $job = Job::fromArray(json_decode($jobData, true));
                    if ($job->isExpired($maxAge)) {
                        $expiredJobIds[] = $batch[$i];
                    }
                }
            }

            if (!empty($expiredJobIds)) {
                $this->redis->hdel($queueName . ':jobs', $expiredJobIds);
                foreach ($expiredJobIds as $jobId) {
                    $this->redis->zrem($queueName, $jobId);
                }
                $expiredCount += count($expiredJobIds);
            }
        }

        return $expiredCount;
    }

    public function cleanupExpiredJobsFromMainQueue(string $queueName, int $maxAge = 86400): int
    {
        $expiredTime = time() - $maxAge;
        $expiredJobIds = $this->redis->zrangebyscore($queueName, 0, $expiredTime);
        
        if (empty($expiredJobIds)) {
            return 0;
        }

        $actuallyExpiredIds = [];
        foreach ($expiredJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
            if ($jobData) {
                $job = Job::fromArray(json_decode($jobData, true));
                if ($job->isExpired($maxAge)) {
                    $actuallyExpiredIds[] = $jobId;
                }
            } else {
                $actuallyExpiredIds[] = $jobId;
            }
        }

        if (!empty($actuallyExpiredIds)) {
            foreach ($actuallyExpiredIds as $jobId) {
                $this->redis->zrem($queueName, $jobId);
                $this->redis->hdel($queueName . ':jobs', [$jobId]);
            }
        }

        return count($actuallyExpiredIds);
    }

    public function recoverLostJobs(string $queueName, int $stuckTimeout = 3600): array
    {
        $currentTime = time();
        $stuckTime = $currentTime - $stuckTimeout;
        $recoveryStats = [
            'stuck_jobs_recovered' => 0,
            'lost_jobs_restored' => 0,
            'corrupted_jobs_removed' => 0
        ];

        // Recover stuck jobs from processing queue
        $stuckJobIds = $this->redis->zrangebyscore($queueName . ':processing', 0, $stuckTime);
        
        foreach ($stuckJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
            
            if ($jobData) {
                // Job data exists, move back to main queue
                $this->redis->zadd($queueName, [$jobId => $currentTime]);
                $this->redis->zrem($queueName . ':processing', $jobId);
                $recoveryStats['stuck_jobs_recovered']++;
                error_log("RECOVERY: Stuck job {$jobId} moved back to main queue");
            } else {
                // Job data missing, remove from processing
                $this->redis->zrem($queueName . ':processing', $jobId);
                $recoveryStats['corrupted_jobs_removed']++;
                error_log("RECOVERY: Corrupted job {$jobId} removed from processing queue");
            }
        }

        // Check for lost jobs in the :lost queue and attempt recovery
        $lostJobs = $this->redis->lrange($queueName . ':lost', 0, -1);
        
        foreach ($lostJobs as $lostJobData) {
            $lostJob = json_decode($lostJobData, true);
            if ($lostJob && isset($lostJob['id'])) {
                $jobId = $lostJob['id'];
                $jobData = $this->redis->hget($queueName . ':jobs', $jobId);
                
                if ($jobData) {
                    // Job data still exists, restore to main queue
                    $this->redis->zadd($queueName, [$jobId => $currentTime]);
                    $this->redis->lrem($queueName . ':lost', 1, $lostJobData);
                    $recoveryStats['lost_jobs_restored']++;
                    error_log("RECOVERY: Lost job {$jobId} restored to main queue");
                }
            }
        }

        if (array_sum($recoveryStats) > 0) {
            error_log("RECOVERY COMPLETE for queue {$queueName}: " . json_encode($recoveryStats));
        }

        return $recoveryStats;
    }

    public function getRecoveryStats(string $queueName): array
    {
        return [
            'processing_jobs' => $this->redis->zcard($queueName . ':processing'),
            'lost_jobs' => $this->redis->llen($queueName . ':lost'),
            'failed_jobs' => $this->redis->llen($queueName . ':failed'),
            'oldest_processing_job' => $this->getOldestProcessingJob($queueName),
            'recovery_recommended' => $this->shouldRunRecovery($queueName)
        ];
    }

    private function getOldestProcessingJob(string $queueName): ?array
    {
        $oldest = $this->redis->zrange($queueName . ':processing', 0, 0, ['WITHSCORES' => true]);
        
        if (empty($oldest)) {
            return null;
        }

        $jobId = array_keys($oldest)[0];
        $timestamp = $oldest[$jobId];
        
        return [
            'job_id' => $jobId,
            'processing_since' => $timestamp,
            'stuck_duration' => time() - $timestamp
        ];
    }

    private function shouldRunRecovery(string $queueName): bool
    {
        $oldestJob = $this->getOldestProcessingJob($queueName);
        return $oldestJob && $oldestJob['stuck_duration'] > 1800; // 30 minutes
    }

    public function enableDetailedLogging(bool $enabled = true): void
    {
        $this->detailedLogging = $enabled;
        $this->log("LOGGING", "Detailed logging " . ($enabled ? "enabled" : "disabled"));
    }

    private function log(string $type, string $message, array $context = []): void
    {
        if (!$this->detailedLogging && !in_array($type, ['ERROR', 'RECOVERY', 'JOB_LOST'])) {
            return;
        }

        $logMessage = "[{$type}] {$message}";
        if (!empty($context)) {
            $logMessage .= " | Context: " . json_encode($context);
        }

        error_log($logMessage);
    }


    public function comprehensiveCleanup(string $queueName, int $maxAge = 86400, int $batchSize = 100): array
    {
        $startTime = microtime(true);
        $stats = [
            'main_queue_cleaned' => 0,
            'job_data_cleaned' => 0,
            'execution_time' => 0
        ];

        $expiredTime = time() - $maxAge;
        $this->redis->zremrangebyscore($queueName . ':processing', 0, $expiredTime);

        $allJobIds = $this->redis->hkeys($queueName . ':jobs');
        if (!empty($allJobIds)) {
            $batches = array_chunk($allJobIds, $batchSize);
            $expiredJobIds = [];

            foreach ($batches as $batch) {
                $jobDataBatch = $this->redis->hmget($queueName . ':jobs', $batch);
                
                for ($i = 0; $i < count($batch); $i++) {
                    $jobData = $jobDataBatch[$i];
                    if ($jobData) {
                        $job = Job::fromArray(json_decode($jobData, true));
                        if ($job->isExpired($maxAge)) {
                            $expiredJobIds[] = $batch[$i];
                        }
                    } else {
                        $expiredJobIds[] = $batch[$i];
                    }
                }
            }

            if (!empty($expiredJobIds)) {
                $expiredBatches = array_chunk($expiredJobIds, $batchSize);
                foreach ($expiredBatches as $expiredBatch) {
                    $this->redis->hdel($queueName . ':jobs', $expiredBatch);
                    foreach ($expiredBatch as $jobId) {
                        $this->redis->zrem($queueName, $jobId);
                    }
                }
                $stats['job_data_cleaned'] = count($expiredJobIds);
                $stats['main_queue_cleaned'] = count($expiredJobIds);
            }
        }

        $stats['execution_time'] = round((microtime(true) - $startTime) * 1000, 2);
        
        if ($stats['job_data_cleaned'] > 0) {
            error_log("CLEANUP STATS for queue {$queueName}: " . json_encode($stats));
        }

        return $stats;
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