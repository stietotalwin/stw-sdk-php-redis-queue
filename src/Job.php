<?php

namespace StieTotalWin\RedisQueue;

class Job
{
    private $id;
    private $type;
    private $data;
    private $createdAt;
    private $processAt;
    private $attempts;
    private $maxAttempts;
    private $status;
    private $error;
    private $updatedAt;

    public function __construct(
        string $id,
        string $type,
        array $data,
        int $createdAt,
        int $processAt,
        int $attempts = 0,
        int $maxAttempts = 3,
        string $status = 'pending',
        string $error = null,
        int $updatedAt = null
    ) {
        $this->id = $id;
        $this->type = $type;
        $this->data = $data;
        $this->createdAt = $createdAt;
        $this->processAt = $processAt;
        $this->attempts = $attempts;
        $this->maxAttempts = $maxAttempts;
        $this->status = $status;
        $this->error = $error;
        $this->updatedAt = $updatedAt ?? $createdAt;
    }

    public static function fromArray(array $jobData): self
    {
        return new self(
            $jobData['id'],
            $jobData['type'],
            $jobData['data'] ?? [],
            $jobData['created_at'],
            $jobData['process_at'],
            $jobData['attempts'] ?? 0,
            $jobData['max_attempts'] ?? 3,
            $jobData['status'] ?? 'pending',
            $jobData['error'] ?? null,
            $jobData['updated_at'] ?? null
        );
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'type' => $this->type,
            'data' => $this->data,
            'created_at' => $this->createdAt,
            'process_at' => $this->processAt,
            'attempts' => $this->attempts,
            'max_attempts' => $this->maxAttempts,
            'status' => $this->status,
            'error' => $this->error,
            'updated_at' => $this->updatedAt,
        ];
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function getData(): array
    {
        return $this->data;
    }

    public function getCreatedAt(): int
    {
        return $this->createdAt;
    }

    public function getProcessAt(): int
    {
        return $this->processAt;
    }

    public function getAttempts(): int
    {
        return $this->attempts;
    }

    public function getMaxAttempts(): int
    {
        return $this->maxAttempts;
    }

    public function getStatus(): string
    {
        return $this->status;
    }

    public function getError(): ?string
    {
        return $this->error;
    }

    public function getUpdatedAt(): int
    {
        return $this->updatedAt;
    }

    public function incrementAttempts(): void
    {
        $this->attempts++;
        $this->updatedAt = time();
    }

    public function setStatus(string $status): void
    {
        $this->status = $status;
        $this->updatedAt = time();
    }

    public function setError(string $error): void
    {
        $this->error = $error;
        $this->updatedAt = time();
    }

    public function setProcessAt(int $processAt): void
    {
        $this->processAt = $processAt;
        $this->updatedAt = time();
    }

    public function canRetry(): bool
    {
        return $this->attempts < $this->maxAttempts;
    }

    public function isExpired(int $maxAge = 86400): bool
    {
        return (time() - $this->createdAt) > $maxAge;
    }
}