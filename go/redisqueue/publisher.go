package redisqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Job represents a queue job compatible with stw-sdk-php-redis-queue
type Job struct {
	ID          string  `json:"id"`
	Type        string  `json:"type"`
	Data        string  `json:"data"`
	CreatedAt   int64   `json:"created_at"`
	ProcessAt   int64   `json:"process_at"`
	Attempts    int     `json:"attempts"`
	MaxAttempts int     `json:"max_attempts"`
	Status      string  `json:"status"`
	Error       *string `json:"error"`
	UpdatedAt   int64   `json:"updated_at"`
}

// BulkJob is input for PublishBulk
type BulkJob struct {
	Type  string
	Data  interface{}
	Delay time.Duration
}

// QueueStats contains queue statistics
type QueueStats struct {
	QueueName      string `json:"queue_name"`
	PendingJobs    int64  `json:"pending_jobs"`
	ProcessingJobs int64  `json:"processing_jobs"`
	FailedJobs     int64  `json:"failed_jobs"`
	TotalJobData   int64  `json:"total_job_data"`
}

// Publisher publishes jobs to Redis queues
// Compatible with stw-sdk-php-redis-queue Consumer
type Publisher struct {
	client       *redis.Client
	defaultQueue string
	maxAttempts  int
}

// PublisherOption configures the Publisher
type PublisherOption func(*Publisher)

// WithDefaultQueue sets the default queue name
func WithDefaultQueue(queue string) PublisherOption {
	return func(p *Publisher) {
		p.defaultQueue = queue
	}
}

// WithMaxAttempts sets the default max retry attempts
func WithMaxAttempts(attempts int) PublisherOption {
	return func(p *Publisher) {
		p.maxAttempts = attempts
	}
}

// NewPublisher creates a new Publisher
func NewPublisher(client *redis.Client, opts ...PublisherOption) *Publisher {
	p := &Publisher{
		client:       client,
		defaultQueue: "default",
		maxAttempts:  3,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish publishes a single job to the queue.
// data can be any JSON-serializable value (struct, map, slice, string, etc.)
// delay of 0 means process immediately.
func (p *Publisher) Publish(ctx context.Context, queue, jobType string, data interface{}, delay time.Duration) (string, error) {
	if jobType == "" {
		return "", fmt.Errorf("job type cannot be empty")
	}

	if queue == "" {
		queue = p.defaultQueue
	}

	// Encode data to JSON string
	dataStr, err := marshalData(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job data: %w", err)
	}

	now := time.Now().Unix()
	processAt := now + int64(delay.Seconds())
	jobID := uuid.New().String()

	job := Job{
		ID:          jobID,
		Type:        jobType,
		Data:        dataStr,
		CreatedAt:   now,
		ProcessAt:   processAt,
		Attempts:    0,
		MaxAttempts: p.maxAttempts,
		Status:      "pending",
		Error:       nil,
		UpdatedAt:   now,
	}

	jobJSON, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("failed to marshal job: %w", err)
	}

	// Use pipeline for atomicity
	pipe := p.client.TxPipeline()

	// ZADD queue <process_at> <job_id>
	pipe.ZAdd(ctx, queue, redis.Z{
		Score:  float64(processAt),
		Member: jobID,
	})

	// HSET queue:jobs <job_id> <job_json>
	pipe.HSet(ctx, queue+":jobs", jobID, string(jobJSON))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to publish job: %w", err)
	}

	return jobID, nil
}

// PublishBulk publishes multiple jobs to the queue.
// Returns a slice of job IDs for successfully published jobs.
func (p *Publisher) PublishBulk(ctx context.Context, queue string, jobs []BulkJob) ([]string, error) {
	if queue == "" {
		queue = p.defaultQueue
	}

	pipe := p.client.TxPipeline()
	jobIDs := make([]string, 0, len(jobs))

	for _, bj := range jobs {
		if bj.Type == "" {
			continue
		}

		dataStr, err := marshalData(bj.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data for job type %s: %w", bj.Type, err)
		}

		now := time.Now().Unix()
		processAt := now + int64(bj.Delay.Seconds())
		jobID := uuid.New().String()

		job := Job{
			ID:          jobID,
			Type:        bj.Type,
			Data:        dataStr,
			CreatedAt:   now,
			ProcessAt:   processAt,
			Attempts:    0,
			MaxAttempts: p.maxAttempts,
			Status:      "pending",
			Error:       nil,
			UpdatedAt:   now,
		}

		jobJSON, err := json.Marshal(job)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal job %s: %w", jobID, err)
		}

		pipe.ZAdd(ctx, queue, redis.Z{
			Score:  float64(processAt),
			Member: jobID,
		})
		pipe.HSet(ctx, queue+":jobs", jobID, string(jobJSON))

		jobIDs = append(jobIDs, jobID)
	}

	if len(jobIDs) == 0 {
		return jobIDs, nil
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to publish bulk jobs: %w", err)
	}

	return jobIDs, nil
}

// GetJob retrieves a job by ID from the queue
func (p *Publisher) GetJob(ctx context.Context, queue, jobID string) (*Job, error) {
	if queue == "" {
		queue = p.defaultQueue
	}

	data, err := p.client.HGet(ctx, queue+":jobs", jobID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var job Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

// DeleteJob removes a job from the queue
func (p *Publisher) DeleteJob(ctx context.Context, queue, jobID string) (bool, error) {
	if queue == "" {
		queue = p.defaultQueue
	}

	pipe := p.client.TxPipeline()
	zremCmd := pipe.ZRem(ctx, queue, jobID)
	pipe.HDel(ctx, queue+":jobs", jobID)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to delete job: %w", err)
	}

	return zremCmd.Val() > 0, nil
}

// GetQueueStats returns statistics for a queue
func (p *Publisher) GetQueueStats(ctx context.Context, queue string) (*QueueStats, error) {
	if queue == "" {
		queue = p.defaultQueue
	}

	pipe := p.client.Pipeline()
	pendingCmd := pipe.ZCard(ctx, queue)
	processingCmd := pipe.ZCard(ctx, queue+":processing")
	failedCmd := pipe.LLen(ctx, queue+":failed")
	totalCmd := pipe.HLen(ctx, queue+":jobs")

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue stats: %w", err)
	}

	return &QueueStats{
		QueueName:      queue,
		PendingJobs:    pendingCmd.Val(),
		ProcessingJobs: processingCmd.Val(),
		FailedJobs:     failedCmd.Val(),
		TotalJobData:   totalCmd.Val(),
	}, nil
}

// ClearQueue removes all data for a queue
func (p *Publisher) ClearQueue(ctx context.Context, queue string) error {
	if queue == "" {
		queue = p.defaultQueue
	}

	pipe := p.client.Pipeline()
	pipe.Del(ctx, queue)
	pipe.Del(ctx, queue+":jobs")
	pipe.Del(ctx, queue+":failed")
	pipe.Del(ctx, queue+":processing")

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to clear queue: %w", err)
	}

	return nil
}

// GetJobCount returns the number of pending jobs in a queue
func (p *Publisher) GetJobCount(ctx context.Context, queue string) (int64, error) {
	if queue == "" {
		queue = p.defaultQueue
	}

	count, err := p.client.ZCard(ctx, queue).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get job count: %w", err)
	}

	return count, nil
}

// marshalData converts data to a JSON string.
// If data is already a string, it validates it's valid JSON.
// Otherwise marshals it to JSON.
func marshalData(data interface{}) (string, error) {
	switch v := data.(type) {
	case string:
		// Validate it's valid JSON
		if !json.Valid([]byte(v)) {
			return "", fmt.Errorf("string data must be valid JSON")
		}
		return v, nil
	case []byte:
		if !json.Valid(v) {
			return "", fmt.Errorf("byte data must be valid JSON")
		}
		return string(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
