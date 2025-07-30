# StieTotalWin Redis Queue

A generic Redis-based queue library for CodeIgniter 4 framework that provides reliable job processing with support for delayed jobs, retries, and failure handling.

## Features

- **Job Publishing**: Publish jobs to Redis queues with optional delays
- **Job Consumption**: Consume and process jobs from queues
- **Retry Mechanism**: Automatic retry with exponential backoff for failed jobs
- **Multiple Queues**: Support for multiple named queues
- **Queue Management**: Monitor queue statistics, clear queues, and manage jobs
- **Bulk Operations**: Publish multiple jobs at once
- **Failed Job Handling**: Track and retry failed jobs
- **Job Expiration**: Automatic cleanup of expired jobs
- **CodeIgniter 4 Integration**: Built specifically for CodeIgniter 4 framework

## Requirements

- PHP >= 7.4
- CodeIgniter 4 >= 4.1
- Redis server
- Predis client library

## Installation

Install via Composer:

```bash
composer require stietotalwin/stw-sdk-php-redis-queue
```

## Configuration

Create a configuration file `app/Config/RedisQueue.php`:

```php
<?php

namespace Config;

use StieTotalWin\RedisQueue\Config\RedisQueue as BaseRedisQueue;

class RedisQueue extends BaseRedisQueue
{
    public $queueHost = '127.0.0.1';
    public $queuePort = 6379;
    public $queuePassword = null;
    public $queueDatabase = 2;
    public $queueTimeout = 5;
    
    public $defaultQueue = 'default';
    public $maxAttempts = 3;
    public $retryDelay = 60;
    public $jobTtl = 86400;
    public $cleanupInterval = 3600;
}
```

## Usage

### Publishing Jobs

```php
use StieTotalWin\RedisQueue\Publisher;

// Initialize publisher
$config = config('RedisQueue')->getQueueConfig();
$publisher = new Publisher($config, 'default');

// Publish a simple job
$jobId = $publisher->publish('send_email', [
    'to' => 'user@example.com',
    'subject' => 'Welcome!',
    'message' => 'Thank you for joining us.'
]);

// Publish a delayed job (process after 5 minutes)
$jobId = $publisher->publish('send_newsletter', [
    'template' => 'monthly',
    'users' => [1, 2, 3]
], 300);

// Publish to a specific queue
$jobId = $publisher->setQueue('high_priority')
    ->publish('urgent_task', ['data' => 'important']);

// Bulk publish
$jobs = [
    ['type' => 'send_email', 'data' => ['to' => 'user1@example.com']],
    ['type' => 'send_email', 'data' => ['to' => 'user2@example.com'], 'delay' => 60],
];
$jobIds = $publisher->publishBulk($jobs, 'email_queue');
```

### Consuming Jobs

```php
use StieTotalWin\RedisQueue\Consumer;

// Initialize consumer
$config = config('RedisQueue')->getQueueConfig();
$consumer = new Consumer($config);

// Consume jobs from a queue
while (true) {
    $job = $consumer->consume('default');
    
    if ($job) {
        try {
            // Process the job
            processJob($job->getType(), $job->getData());
            
            // Mark as completed
            $consumer->markCompleted($job->getId(), 'default');
            
        } catch (Exception $e) {
            // Mark as failed with error message
            $consumer->markFailed($job->getId(), 'default', $e->getMessage());
        }
    } else {
        // No jobs available, wait before checking again
        sleep(1);
    }
}

function processJob($type, $data) {
    switch ($type) {
        case 'send_email':
            // Send email logic
            sendEmail($data['to'], $data['subject'], $data['message']);
            break;
            
        case 'send_newsletter':
            // Newsletter logic
            sendNewsletter($data['template'], $data['users']);
            break;
            
        default:
            throw new Exception("Unknown job type: {$type}");
    }
}
```

### Queue Management

```php
// Get queue statistics
$stats = $publisher->getQueueStats('default');
/*
Array:
[
    'queue_name' => 'default',
    'pending_jobs' => 5,
    'processing_jobs' => 2,
    'failed_jobs' => 1,
    'total_job_data' => 8
]
*/

// Get job count
$count = $publisher->getJobCount('default');

// Get all queue names
$queues = $publisher->getQueueNames();

// Clear a queue (removes all jobs)
$publisher->clearQueue('default');

// Get a specific job
$job = $publisher->getJob($jobId, 'default');
if ($job) {
    echo "Job Type: " . $job->getType();
    echo "Job Data: " . json_encode($job->getData());
    echo "Status: " . $job->getStatus();
}

// Delete a specific job
$publisher->deleteJob($jobId, 'default');
```

### Failed Job Management

```php
// Get failed jobs
$failedJobs = $consumer->getFailedJobs('default', 10);

foreach ($failedJobs as $job) {
    echo "Failed Job: " . $job->getType();
    echo "Error: " . $job->getError();
    echo "Attempts: " . $job->getAttempts();
}

// Retry failed jobs
$retriedCount = $consumer->retryFailedJobs('default', 5);
echo "Retried {$retriedCount} jobs";

// Get processing jobs
$processingJobs = $consumer->getProcessingJobs('default');

// Clean up expired jobs
$cleanedCount = $consumer->cleanupExpiredJobs('default', 86400);
echo "Cleaned {$cleanedCount} expired jobs";
```

### Job Object Properties

The `Job` class provides access to job information:

```php
$job = $consumer->consume('default');

if ($job) {
    echo $job->getId();          // Unique job ID (UUID)
    echo $job->getType();        // Job type
    print_r($job->getData());    // Job data array
    echo $job->getStatus();      // Current status
    echo $job->getAttempts();    // Number of attempts
    echo $job->getMaxAttempts(); // Maximum allowed attempts
    echo $job->getCreatedAt();   // Creation timestamp
    echo $job->getProcessAt();   // When to process (for delayed jobs)
    echo $job->getError();       // Error message (if failed)
    
    // Check if job can be retried
    if ($job->canRetry()) {
        echo "Job can be retried";
    }
    
    // Check if job is expired
    if ($job->isExpired()) {
        echo "Job has expired";
    }
}
```

## Queue Structure

The library uses the following Redis data structures:

- `{queue_name}`: Sorted set for pending jobs (score = process_at timestamp)
- `{queue_name}:jobs`: Hash map storing job data (key = job_id, value = job_json)
- `{queue_name}:processing`: Sorted set for jobs currently being processed
- `{queue_name}:failed`: List of failed jobs that exceeded max attempts

## Configuration Options

| **Option**         | **Default**   | **Description**                                |
|--------------------|---------------|------------------------------------------------|
| `queueHost`        | `127.0.0.1`   | Redis server host                              |
| `queuePort`        | `6379`        | Redis server port                              |
| `queuePassword`    | `null`        | Redis password (if required)                   |
| `queueDatabase`    | `2`           | Redis database number                          |
| `queueTimeout`     | `5`           | Connection timeout in seconds                  |
| `defaultQueue`     | `default`     | Default queue name                             |
| `maxAttempts`      | `3`           | Maximum retry attempts for failed jobs         |
| `retryDelay`       | `60`          | Base retry delay in seconds                    |
| `jobTtl`           | `86400`       | Job time-to-live in seconds                    |
| `cleanupInterval`  | `3600`        | Cleanup interval in seconds                    |

## Error Handling

The library provides robust error handling:

- **Automatic Retries**: Failed jobs are automatically retried with exponential backoff
- **Failed Job Storage**: Jobs that exceed max attempts are stored in a failed jobs list
- **Error Messages**: Exception messages are captured and stored with failed jobs
- **Job Expiration**: Old jobs are automatically cleaned up to prevent memory leaks

## Best Practices

1. **Use Specific Queue Names**: Create separate queues for different job types
2. **Handle Exceptions**: Always wrap job processing in try-catch blocks
3. **Monitor Failed Jobs**: Regularly check and handle failed jobs
4. **Set Appropriate TTL**: Configure job TTL based on your use case
5. **Clean Up Regularly**: Use the cleanup functionality to remove expired jobs
6. **Use Delays Wisely**: For time-sensitive jobs, use delays instead of immediate processing

## License

This library is licensed under the MIT License. See the LICENSE file for details.

## Support

For issues and questions, please contact: <dev@stietotalwin.com>
