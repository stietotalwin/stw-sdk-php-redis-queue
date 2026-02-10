# Go Redis Queue Publisher

Go publisher compatible with `stw-sdk-php-redis-queue` PHP consumer.

## Install

```bash
go get github.com/stietotalwin/stw-sdk-php-redis-queue/go/redisqueue
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/stietotalwin/stw-sdk-php-redis-queue/go/redisqueue"
)

func main() {
    ctx := context.Background()

    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    pub := redisqueue.NewPublisher(rdb,
        redisqueue.WithDefaultQueue("default"),
        redisqueue.WithMaxAttempts(3),
    )

    // Publish immediately
    jobID, _ := pub.Publish(ctx, "email_queue", "send_email", map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Hello!",
    }, 0)
    fmt.Println("Job ID:", jobID)

    // Publish with 5 minute delay
    jobID, _ = pub.Publish(ctx, "email_queue", "send_reminder", map[string]interface{}{
        "user_id": 123,
    }, 5*time.Minute)

    // Bulk publish
    jobIDs, _ := pub.PublishBulk(ctx, "notification_queue", []redisqueue.BulkJob{
        {Type: "sms", Data: map[string]string{"phone": "+628123456789"}},
        {Type: "push", Data: map[string]string{"device": "abc123"}},
    })
    fmt.Printf("Published %d jobs\n", len(jobIDs))

    // Queue stats
    stats, _ := pub.GetQueueStats(ctx, "email_queue")
    fmt.Printf("Pending: %d\n", stats.PendingJobs)
}
```

## Features

- **Compatible** with PHP consumer (`stw-sdk-php-redis-queue`)
- **Atomic** operations using Redis transactions (MULTI/EXEC)
- **Delayed jobs** with configurable delay
- **Bulk publish** multiple jobs in a single transaction
- **Data validation** — ensures job data is valid JSON
- **Queue management** — stats, get job, delete job, clear queue

## Data Format

Data can be:
- `map[string]interface{}` or any struct (auto-marshaled to JSON)
- `string` (must be valid JSON)
- `[]byte` (must be valid JSON)

The publisher stores data as a JSON **string** inside the job payload, matching the PHP SDK format.
