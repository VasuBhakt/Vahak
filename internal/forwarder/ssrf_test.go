package forwarder

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VasuBhakt/vahak/internal/models"
	"github.com/VasuBhakt/vahak/internal/queue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSSRFProtection_BlocksLocalAndMetadata(t *testing.T) {
	logger := zap.NewNop()
	jq := queue.NewJobQueue(10)
	fBlocked := New(nil, logger, jq, false) // allowLocal = false

	tests := []struct {
		name      string
		targetURL string
	}{
		{"IPv4 Loopback", "http://127.0.0.1:8080/webhook"},
		{"Cloud Metadata", "http://169.254.169.254/latest/meta-data"},
		{"Private RFC1918 10.x", "http://10.0.0.1:80/webhook"},
		{"Private RFC1918 192.168.x", "http://192.168.1.1:80/webhook"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &models.Request{
				ID:     uuid.New(),
				Method: "POST",
				Body:   `{"test":true}`,
			}
			err := fBlocked.deliver(tc.targetURL, req)
			assert.Error(t, err, "expected SSRF block for %s", tc.targetURL)
		})
	}
}

func TestSSRFProtection_RedirectDisabled(t *testing.T) {
	// Target that returns a 302 redirect
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:9999/redirected", http.StatusFound)
	}))
	defer ts.Close()

	logger := zap.NewNop()
	jq := queue.NewJobQueue(10)
	// allowLocal = true so the initial call to ts.URL succeeds and triggers the redirect
	f := New(nil, logger, jq, true)

	req := &models.Request{
		ID:     uuid.New(),
		Method: "POST",
		Body:   `{"test":true}`,
	}
	err := f.deliver(ts.URL, req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "redirects disabled")
}
