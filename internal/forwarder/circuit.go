package forwarder

import (
	"sync"
	"time"
)

const (
	cbThreshold  = 5                  // consecutive failures before tripping
	cbCooldown   = 30 * time.Second   // initial cooldown after first trip
	cbMaxCooldown = 5 * time.Minute   // maximum cooldown cap
)

// Exported variable so tests can mock time
var clockNow = time.Now

type circuitState int

const (
	circuitClosed   circuitState = iota // healthy, allow requests
	circuitOpen                         // tripped, block requests
	circuitHalfOpen                     // testing with one probe request
)

type circuitBreaker struct {
	mu          sync.Mutex
	failures    int
	tripCount   int          // how many times the circuit has tripped (drives exponential backoff)
	state       circuitState
	lastFailure time.Time
}

// CircuitManager holds per-target-URL circuit breakers.
type CircuitManager struct {
	breakers sync.Map // map[string]*circuitBreaker
}

func NewCircuitManager() *CircuitManager {
	return &CircuitManager{}
}

func (cm *CircuitManager) get(targetURL string) *circuitBreaker {
	val, _ := cm.breakers.LoadOrStore(targetURL, &circuitBreaker{})
	return val.(*circuitBreaker)
}

// Allow checks if a request to this target should proceed.
func (cm *CircuitManager) Allow(targetURL string) bool {
	cb := cm.get(targetURL)
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case circuitClosed:
		return true
	case circuitOpen:
		// exponential cooldown: 30s, 60s, 120s, 240s... capped at 5 min
		cooldown := cbCooldown
		for i := 1; i < cb.tripCount && cooldown < cbMaxCooldown; i++ {
			cooldown *= 2
		}
		if cooldown > cbMaxCooldown {
			cooldown = cbMaxCooldown
		}
		if clockNow().Sub(cb.lastFailure) > cooldown {
			cb.state = circuitHalfOpen
			return true // allow one probe request
		}
		return false
	case circuitHalfOpen:
		return false // already have a probe in flight
	}
	return true
}

// RecordSuccess resets the breaker back to closed.
func (cm *CircuitManager) RecordSuccess(targetURL string) {
	cb := cm.get(targetURL)
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.tripCount = 0
	cb.state = circuitClosed
}

// RecordFailure increments failures and trips the breaker if threshold is hit.
func (cm *CircuitManager) RecordFailure(targetURL string) {
	cb := cm.get(targetURL)
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailure = clockNow()

	if cb.state == circuitHalfOpen {
		// A probe failed. Increment tripCount and open the circuit again.
		cb.state = circuitOpen
		cb.tripCount++
		return
	}

	if cb.state == circuitClosed && cb.failures >= cbThreshold {
		// First time tripping after being closed.
		cb.state = circuitOpen
		cb.tripCount = 1
	}
}
