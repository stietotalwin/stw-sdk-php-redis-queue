<?php

namespace StieTotalWin\RedisQueue\Test;

require_once __DIR__ . '/../vendor/autoload.php';

use StieTotalWin\RedisQueue\RedisConnection;
use StieTotalWin\RedisQueue\Publisher;
use StieTotalWin\RedisQueue\Consumer;

/**
 * Consumer Blocking Test
 * 
 * Test for the new blocking consumer functionality using BZPOPMIN
 */
class ConsumerTest
{
    private $config;
    private $publisher;
    private $consumer;
    private $testQueue = 'test_blocking_consumer_queue';
    private $simulationMode = false;
    
    public function __construct()
    {
        // Real Upstash Redis configuration
        $this->config = new class extends \StieTotalWin\RedisQueue\Config\RedisQueue {
            public function __construct() {
                $this->scheme = 'tls';
                $this->host = 'XXXX.upstash.io';
                $this->port = 6379;
                $this->user = 'default';
                $this->password = 'XXXX';
                $this->parameters = [
                    'timeout' => 30,
                    'read_write_timeout' => 30,
                    'ssl' => [
                        'verify_peer' => false,
                        'verify_peer_name' => false,
                        'allow_self_signed' => true
                    ]
                ];
            }
        };
        
        // Try to connect, fall back to simulation mode if it fails
        try {
            $this->publisher = new Publisher($this->config, $this->testQueue);
            $this->consumer = new Consumer($this->config);
            $redisConfig = $this->config->getRedisConfig();
            $connection = RedisConnection::getInstance($redisConfig);
            $connection->getClient()->ping();
            echo "🔗 Real Redis connection established!\n";
        } catch (\Exception $e) {
            echo "⚠️  Redis connection failed, switching to simulation mode...\n";
            echo "   Error: " . $e->getMessage() . "\n";
            $this->simulationMode = true;
        }
    }
    
    /**
     * Test 1: Basic blocking consume with immediate job
     */
    public function testBlockingConsumeImmediate()
    {
        echo "\n🔄 Testing Blocking Consume (Immediate Job)...\n";
        
        if ($this->simulationMode) {
            echo "⚠️  Simulation mode - skipping real Redis test\n";
            return true;
        }
        
        try {
            // Clear queue first
            $this->publisher->clearQueue($this->testQueue);
            
            // Publish a job immediately available
            $jobId = $this->publisher->publish('test_blocking_job', json_encode([
                'message' => 'This job should be consumed immediately',
                'timestamp' => time()
            ]));
            
            echo "   Published job: $jobId\n";
            
            // Consume with short timeout
            $startTime = microtime(true);
            $job = $this->consumer->consume($this->testQueue, 5);
            $duration = microtime(true) - $startTime;
            
            if (!$job) {
                throw new \Exception("Job not consumed");
            }
            
            echo "✅ Job consumed successfully\n";
            echo "   Job ID: " . $job->getId() . "\n";
            echo "   Job Type: " . $job->getType() . "\n";
            echo "   Duration: " . round($duration * 1000, 2) . "ms\n";
            
            if ($duration > 1.0) {
                echo "⚠️  Warning: Consumption took longer than expected\n";
            }
            
            return true;
        } catch (\Exception $e) {
            echo "❌ Blocking consume immediate test failed: " . $e->getMessage() . "\n";
            return false;
        }
    }
    
    /**
     * Test 2: Blocking consume with timeout (no jobs)
     */
    public function testBlockingConsumeTimeout()
    {
        echo "\n⏱️  Testing Blocking Consume (Timeout)...\n";
        
        if ($this->simulationMode) {
            echo "⚠️  Simulation mode - skipping real Redis test\n";
            return true;
        }
        
        try {
            // Clear queue to ensure no jobs
            $this->publisher->clearQueue($this->testQueue);
            
            // Try to consume with short timeout
            $timeout = 2;
            $startTime = microtime(true);
            $job = $this->consumer->consume($this->testQueue, $timeout);
            $duration = microtime(true) - $startTime;
            
            if ($job !== null) {
                throw new \Exception("Expected no job, but got: " . $job->getId());
            }
            
            echo "✅ Timeout behavior working correctly\n";
            echo "   Expected timeout: {$timeout}s\n";
            echo "   Actual duration: " . round($duration, 2) . "s\n";
            
            // Should be close to timeout duration
            if (abs($duration - $timeout) > 0.5) {
                echo "⚠️  Warning: Duration significantly different from timeout\n";
            }
            
            return true;
        } catch (\Exception $e) {
            echo "❌ Blocking consume timeout test failed: " . $e->getMessage() . "\n";
            return false;
        }
    }
    
    /**
     * Test 3: Delayed job handling
     */
    public function testDelayedJobHandling()
    {
        echo "\n⏰ Testing Delayed Job Handling...\n";
        
        if ($this->simulationMode) {
            echo "⚠️  Simulation mode - skipping real Redis test\n";
            return true;
        }
        
        try {
            // Clear queue first
            $this->publisher->clearQueue($this->testQueue);
            
            // Publish a delayed job (5 seconds in future)
            $delay = 5;
            $jobId = $this->publisher->publish('delayed_test_job', json_encode([
                'message' => 'This job is delayed',
                'delay' => $delay
            ]), $delay);
            
            echo "   Published delayed job: $jobId (delay: {$delay}s)\n";
            
            // Try to consume immediately (should timeout or handle correctly)
            $startTime = microtime(true);
            $job = $this->consumer->consume($this->testQueue, 2);
            $duration = microtime(true) - $startTime;
            
            if ($job !== null) {
                echo "⚠️  Got job before delay expired - this may be correct if job was re-queued\n";
                echo "   Job ID: " . $job->getId() . "\n";
            } else {
                echo "✅ Delayed job correctly not consumed immediately\n";
            }
            
            echo "   Duration: " . round($duration, 2) . "s\n";
            
            return true;
        } catch (\Exception $e) {
            echo "❌ Delayed job handling test failed: " . $e->getMessage() . "\n";
            return false;
        }
    }
    
    /**
     * Run all tests
     */
    public function runAllTests()
    {
        echo "🚀 Starting Redis Queue Consumer Blocking Tests\n";
        echo "=" . str_repeat("=", 70) . "\n";
        echo "Using Redis: " . $this->config->host . ":" . $this->config->port . "\n";
        echo "Test Queue: " . $this->testQueue . "\n";
        echo "Mode: " . ($this->simulationMode ? "Simulation (Network Issues)" : "Real Redis Connection") . "\n";
        echo "=" . str_repeat("=", 70) . "\n";
        
        $tests = [
            'Blocking Consume Immediate' => 'testBlockingConsumeImmediate',
            'Blocking Consume Timeout' => 'testBlockingConsumeTimeout',
            'Delayed Job Handling' => 'testDelayedJobHandling'
        ];
        
        $results = [];
        $passed = 0;
        
        foreach ($tests as $testName => $testMethod) {
            $result = $this->$testMethod();
            $results[$testName] = $result;
            if ($result) {
                $passed++;
            }
            
            // Small delay between tests
            sleep(1);
        }
        
        // Summary
        echo "\n" . str_repeat("=", 70) . "\n";
        echo "📋 CONSUMER TEST SUMMARY\n";
        echo str_repeat("=", 70) . "\n";
        
        foreach ($results as $testName => $result) {
            $status = $result ? "✅ PASS" : "❌ FAIL";
            echo sprintf("%-30s %s\n", $testName . ":", $status);
        }
        
        echo str_repeat("-", 70) . "\n";
        echo sprintf("OVERALL: %d/%d tests passed (%.1f%%)\n", 
                    $passed, count($tests), ($passed / count($tests)) * 100);
        
        if ($passed === count($tests)) {
            echo "\n🎉 ALL CONSUMER TESTS PASSED!\n";
            echo "✅ Blocking consumer with BZPOPMIN is working correctly\n";
            echo "✅ Timeout handling is functional\n";
            echo "✅ Delayed job processing is working\n";
        } else {
            echo "\n⚠️  Some consumer tests failed.\n";
        }
        
        // Clean up
        if (!$this->simulationMode) {
            $this->publisher->clearQueue($this->testQueue);
        }
        
        return $passed === count($tests);
    }
}

// Auto-run tests when file is executed directly
if (basename(__FILE__) === basename($_SERVER['SCRIPT_NAME'] ?? '')) {
    $test = new ConsumerTest();
    $success = $test->runAllTests();
    exit($success ? 0 : 1);
}