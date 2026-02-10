<?php

namespace StieTotalWin\RedisQueue;

use Ramsey\Uuid\Uuid;

class Publisher
{
    private $redis;
    private $redisConnection;
    private $defaultQueue;
    private $currentQueue;

    public function __construct(\StieTotalWin\RedisQueue\Config\RedisQueue $config, string $defaultQueue = 'default')
    {
        $redisConfig           = $config->getRedisConfig();
        $this->redisConnection = RedisConnection::getInstance($redisConfig);
        $this->redis           = $this->redisConnection->getClient();
        $this->defaultQueue    = $defaultQueue;
        $this->currentQueue    = $defaultQueue;
    }

    public function setQueue(string $queueName): self
    {
        $this->currentQueue = $queueName;
        return $this;
    }

    public function publish(string $type, string $data, int $delay = 0, string $queue = null): string
    {
        $queueName = $queue ?? $this->currentQueue;
        $jobId     = Uuid::uuid4()->toString();
        $timestamp = time();

        try {
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
        } catch (\Exception $e) {
            error_log("Redis publish error for job type {$type}: " . $e->getMessage());
            $this->currentQueue = $this->defaultQueue;
            throw $e;
        }
    }

    public function publishBulk(array $jobs, string $queue = null): array
    {
        $queueName = $queue ?? $this->currentQueue;
        $jobIds    = [];

        foreach ($jobs as $jobData) {
            $type  = $jobData['type'] ?? '';
            $data  = $jobData['data'] ?? [];
            $delay = $jobData['delay'] ?? 0;

            if (empty($type)) {
                continue;
            }

            // Ensure data is JSON encoded if it's an array
            $dataString = is_array($data) ? json_encode($data) : $data;

            $jobIds[] = $this->publish($type, $dataString, $delay, $queueName);
        }

        $this->currentQueue = $this->defaultQueue;

        return $jobIds;
    }

    public function getQueueStats(string $queue = null): array
    {
        $queueName = $queue ?? $this->currentQueue;

        try {
            $pendingJobs    = $this->redis->zcard($queueName);
            $processingJobs = $this->redis->zcard($queueName . ':processing');
            $failedJobs     = $this->redis->llen($queueName . ':failed');
            $totalJobData   = $this->redis->hlen($queueName . ':jobs');

            return [
                'queue_name'      => $queueName,
                'pending_jobs'    => $pendingJobs,
                'processing_jobs' => $processingJobs,
                'failed_jobs'     => $failedJobs,
                'total_job_data'  => $totalJobData
            ];
        } catch (\Exception $e) {
            error_log("Redis getQueueStats error for queue {$queueName}: " . $e->getMessage());
            return [
                'queue_name'      => $queueName,
                'pending_jobs'    => 0,
                'processing_jobs' => 0,
                'failed_jobs'     => 0,
                'total_job_data'  => 0,
                'error'           => $e->getMessage()
            ];
        }
    }

    public function clearQueue(string $queue = null): bool
    {
        $queueName = $queue ?? $this->currentQueue;

        try {
            $this->redis->del($queueName);
            $this->redis->del($queueName . ':jobs');
            $this->redis->del($queueName . ':failed');
            $this->redis->del($queueName . ':processing');

            return true;
        } catch (\Exception $e) {
            error_log("Redis clearQueue error for queue {$queueName}: " . $e->getMessage());
            return false;
        }
    }

    public function getJobCount(string $queue = null): int
    {
        $queueName = $queue ?? $this->currentQueue;

        try {
            return $this->redis->zcard($queueName);
        } catch (\Exception $e) {
            error_log("Redis getJobCount error for queue {$queueName}: " . $e->getMessage());
            return 0;
        }
    }

    public function getQueueNames(): array
    {
        try {
            $queueNames = [];
            $cursor = '0';

            do {
                $result = $this->redis->scan($cursor, ['COUNT' => 100]);

                if ($result === false || empty($result)) {
                    break;
                }

                $cursor = $result[0];
                $keys = $result[1];

                foreach ($keys as $key) {
                    if (strpos($key, ':') === false) {
                        $queueNames[] = $key;
                    }
                }
            } while ($cursor !== '0');

            return array_unique($queueNames);
        } catch (\Exception $e) {
            error_log("Redis getQueueNames error: " . $e->getMessage());
            return [];
        }
    }

    public function deleteJob(string $jobId, string $queue = null): bool
    {
        $queueName = $queue ?? $this->currentQueue;

        try {
            $removed = $this->redis->zrem($queueName, $jobId);
            $this->redis->hdel($queueName . ':jobs', [$jobId]); // Fixed: restored array brackets

            return $removed > 0;
        } catch (\Exception $e) {
            error_log("Redis deleteJob error for job {$jobId}: " . $e->getMessage());
            return false;
        }
    }

    public function getJob(string $jobId, string $queue = null): ?Job
    {
        $queueName = $queue ?? $this->currentQueue;

        try {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

            if (!$jobData) {
                return null;
            }

            $decodedData = json_decode($jobData, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                error_log("JSON decode error for job {$jobId}: " . json_last_error_msg());
                return null;
            }

            return Job::fromArray($decodedData);
        } catch (\Exception $e) {
            error_log("Redis getJob error for job {$jobId}: " . $e->getMessage());
            return null;
        }
    }

    public function republishStuckJobs(string $queue = null, int $stuckMinutes = 10): int
    {
        $queueName        = $queue ?? $this->currentQueue;
        $stuckTime        = time() - ($stuckMinutes * 60);
        $republishedCount = 0;

        // Get all processing jobs that have been stuck for more than the specified time
        $stuckJobIds = $this->redis->zrangebyscore($queueName . ':processing', 0, $stuckTime);

        foreach ($stuckJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

            if (!$jobData) {
                // Clean up orphaned processing job
                $this->redis->zrem($queueName . ':processing', $jobId);
                continue;
            }

            $job = Job::fromArray(json_decode($jobData, true));

            // Reset job for republishing
            $job->setStatus('pending');
            $job->setProcessAt(time()); // Process immediately
            $job->incrementAttempts();

            // Remove from processing queue
            $this->redis->zrem($queueName . ':processing', $jobId);

            // Add back to main queue for immediate processing
            $this->redis->zadd($queueName, [$job->getId() => $job->getProcessAt()]);

            // Update job data
            $this->redis->hset($queueName . ':jobs', $job->getId(), json_encode($job->toArray()));

            $republishedCount++;
        }

        return $republishedCount;
    }

    public function getStuckJobs(string $queue = null, int $stuckMinutes = 10): array
    {
        $queueName = $queue ?? $this->currentQueue;
        $stuckTime = time() - ($stuckMinutes * 60);
        $stuckJobs = [];

        // Get all processing jobs that have been stuck for more than the specified time
        $stuckJobIds = $this->redis->zrangebyscore($queueName . ':processing', 0, $stuckTime);

        foreach ($stuckJobIds as $jobId) {
            $jobData = $this->redis->hget($queueName . ':jobs', $jobId);

            if ($jobData) {
                $job         = Job::fromArray(json_decode($jobData, true));
                $stuckJobs[] = $job;
            }
        }

        return $stuckJobs;
    }

    private function addJobToQueue(Job $job, string $queueName): void
    {
        try {
            $this->redis->zadd($queueName, [$job->getId() => $job->getProcessAt()]);
            $jobData = json_encode($job->toArray());

            if ($jobData === false) {
                throw new \Exception("Failed to JSON encode job data");
            }

            $this->redis->hset($queueName . ':jobs', $job->getId(), $jobData);
        } catch (\Exception $e) {
            error_log("Redis addJobToQueue error for job {$job->getId()}: " . $e->getMessage());
            throw $e;
        }
    }
}