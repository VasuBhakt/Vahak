package forwarder

import (
	"testing"
	"time"
)

func TestCircuitBreaker_HappyPath(t *testing.T) {
	cm := NewCircuitManager()
	target := "http://example.com"

	// Should allow requests initially
	for i := 0; i < 10; i++ {
		if !cm.Allow(target) {
			t.Fatalf("expected request %d to be allowed", i)
		}
		cm.RecordSuccess(target)
	}
}

func TestCircuitBreaker_TripAndReset(t *testing.T) {
	cm := NewCircuitManager()
	target := "http://example.com"

	// 4 failures should not trip
	for i := 0; i < 4; i++ {
		cm.RecordFailure(target)
		if !cm.Allow(target) {
			t.Fatalf("expected request %d to be allowed before threshold", i)
		}
	}

	// 5th failure trips the breaker
	cm.RecordFailure(target)
	if cm.Allow(target) {
		t.Fatal("expected request to be blocked after 5 failures")
	}

	// Mock time forward by 31 seconds (past 30s initial cooldown)
	now := time.Now()
	clockNow = func() time.Time { return now.Add(31 * time.Second) }
	defer func() { clockNow = time.Now }() // cleanup

	// First probe should be allowed (HalfOpen)
	if !cm.Allow(target) {
		t.Fatal("expected probe request to be allowed after cooldown")
	}

	// Second concurrent probe should be blocked (only 1 probe allowed)
	if cm.Allow(target) {
		t.Fatal("expected second probe to be blocked")
	}

	// Probe succeeds! Should reset completely.
	cm.RecordSuccess(target)
	if !cm.Allow(target) {
		t.Fatal("expected normal requests to be allowed after probe success")
	}
}

func TestCircuitBreaker_ExponentialBackoff(t *testing.T) {
	cm := NewCircuitManager()
	target := "http://example.com"

	now := time.Now()
	mockTime := now
	clockNow = func() time.Time { return mockTime }
	defer func() { clockNow = time.Now }()

	// Trip it initially (5 failures)
	for i := 0; i < 5; i++ {
		cm.RecordFailure(target)
	}
	if cm.Allow(target) {
		t.Fatal("expected to be blocked (Open state)")
	}

	// Wait 30 seconds -> Probe allowed
	mockTime = mockTime.Add(31 * time.Second)
	if !cm.Allow(target) {
		t.Fatal("expected probe 1 to be allowed")
	}

	// Probe fails! Cooldown should now be 60 seconds
	cm.RecordFailure(target)
	
	// Wait 30 seconds (total 61s from trip 1) -> Should still be blocked
	mockTime = mockTime.Add(30 * time.Second)
	if cm.Allow(target) {
		t.Fatal("expected to be blocked (waiting for 60s cooldown)")
	}

	// Wait another 31 seconds (total 61s from trip 2) -> Probe allowed
	mockTime = mockTime.Add(31 * time.Second)
	if !cm.Allow(target) {
		t.Fatal("expected probe 2 to be allowed after 60s")
	}

	// Probe fails again! Cooldown should now be 120 seconds
	cm.RecordFailure(target)

	// Wait 61 seconds -> Still blocked
	mockTime = mockTime.Add(61 * time.Second)
	if cm.Allow(target) {
		t.Fatal("expected to be blocked (waiting for 120s cooldown)")
	}

	// Wait another 60 seconds -> Probe allowed
	mockTime = mockTime.Add(60 * time.Second)
	if !cm.Allow(target) {
		t.Fatal("expected probe 3 to be allowed after 120s")
	}
}
