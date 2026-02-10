package redisqueue_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stietotalwin/stw-sdk-php-redis-queue/go/redisqueue"
)

func Example_basic() {
	ctx := context.Background()

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	// Create publisher
	pub := redisqueue.NewPublisher(rdb,
		redisqueue.WithDefaultQueue("default"),
		redisqueue.WithMaxAttempts(3),
	)

	// 1. Publish a simple job (map)
	jobID, err := pub.Publish(ctx, "email_queue", "send_email", map[string]interface{}{
		"to":      "user@example.com",
		"subject": "Welcome!",
		"body":    "Thank you for joining us.",
	}, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published email job: %s\n", jobID)

	// 2. Publish a delayed job (process after 5 minutes)
	jobID, err = pub.Publish(ctx, "email_queue", "send_newsletter", map[string]interface{}{
		"template": "monthly",
		"users":    []int{1, 2, 3},
	}, 5*time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published delayed job: %s\n", jobID)

	// 3. Publish with struct
	type BNICallback struct {
		TransactionID string  `json:"transaction_id"`
		Amount        float64 `json:"amount"`
		Status        string  `json:"status"`
	}

	jobID, err = pub.Publish(ctx, "bni_callback_queue", "bni_callback", BNICallback{
		TransactionID: "TXN-2026-001",
		Amount:        500000,
		Status:        "success",
	}, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published BNI callback job: %s\n", jobID)

	// 4. Bulk publish
	jobIDs, err := pub.PublishBulk(ctx, "notification_queue", []redisqueue.BulkJob{
		{Type: "sms", Data: map[string]string{"phone": "+628123456789", "message": "Hello!"}},
		{Type: "push", Data: map[string]string{"device": "abc123", "title": "Alert"}},
		{Type: "whatsapp", Data: map[string]string{"phone": "+628987654321", "message": "Hi!"}, Delay: 2 * time.Minute},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Published %d bulk jobs\n", len(jobIDs))

	// 5. Get queue stats
	stats, err := pub.GetQueueStats(ctx, "email_queue")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Queue stats: pending=%d, processing=%d, failed=%d\n",
		stats.PendingJobs, stats.ProcessingJobs, stats.FailedJobs)

	// 6. Get a specific job
	job, err := pub.GetJob(ctx, "email_queue", jobID)
	if err != nil {
		panic(err)
	}
	if job != nil {
		fmt.Printf("Job %s: type=%s, status=%s\n", job.ID, job.Type, job.Status)
	}
}
