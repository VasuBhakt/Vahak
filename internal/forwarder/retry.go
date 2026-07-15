package forwarder

import (
	"math/rand"
	"time"
)

// uses exponential backoff with "full jitter"
// this prevents the "thundering herd" problem when many webhooks fail simultaneously

func CalculateNextAttempt(attempts int) time.Time {
	base := float64(InitialBackoff * time.Second)
	maxBackoff := float64(30 * time.Minute)

	for i := 0; i < attempts-1; i++ {
		base *= float64(InitialBackoff)
		if base > maxBackoff {
			base = maxBackoff
		}
	}

	// apply full jitter: pick a random duration between 0 and base
	jitter := rand.Float64() * base

	// add the jitter to tcurrent time to get exact next attempt time
	return time.Now().Add(time.Duration(jitter))
}
