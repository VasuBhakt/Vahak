package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestCrashRecovery_ZeroLoss asserts that in ACK_MODE=durable,
// any webhook that received a 200 OK from Vahak is NEVER lost,
// even if the Vahak process is abruptly killed with SIGKILL (kill -9) mid-load.
func TestCrashRecovery_ZeroLoss(t *testing.T) {
	// 1. Start a local HTTP sink to record delivered webhooks
	var receivedIDs sync.Map
	var receivedCount int64

	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(body, &payload); err == nil && payload.EventID != "" {
			receivedIDs.Store(payload.EventID, true)
			atomic.AddInt64(&receivedCount, 1)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer sink.Close()

	// 2. Build Vahak binary for deterministic process lifecycle control
	tmpDir, err := os.MkdirTemp("", "vahak_chaos_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	binName := "vahak_chaos"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(tmpDir, binName)
	dbPath := filepath.Join(tmpDir, "crash_test.db")

	t.Log("Building Vahak binary...")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/server")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build binary: %v\nOutput: %s", err, string(out))
	}

	testPort := "8099"
	testAPIKey := "chaos_test_secret_key"

	startVahak := func() *exec.Cmd {
		cmd := exec.Command(binPath)
		cmd.Env = append(os.Environ(),
			"PORT="+testPort,
			"DB_ENGINE=sqlite",
			"DB_PATH="+dbPath,
			"ACK_MODE=durable",
			"API_KEY="+testAPIKey,
			"ALLOW_LOCAL_TARGETS=true",
			"ALLOW_INSECURE_DEFAULT_KEY=true",
			"RETENTION_DAYS=7",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start Vahak process: %v", err)
		}
		return cmd
	}

	waitForHealthy := func(timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/health", testPort))
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return true
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(50 * time.Millisecond)
		}
		return false
	}

	// 3. Start Vahak (Process 1)
	t.Log("Starting initial Vahak process...")
	proc1 := startVahak()

	if !waitForHealthy(10 * time.Second) {
		proc1.Process.Kill()
		t.Fatal("Vahak process 1 failed to become healthy")
	}

	// 4. Register an endpoint pointing to our test sink
	createEpBody, _ := json.Marshal(map[string]string{
		"name":       "chaos-endpoint",
		"target_url": sink.URL,
	})
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%s/api/endpoints", testPort), bytes.NewBuffer(createEpBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", testAPIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusCreated {
		proc1.Process.Kill()
		t.Fatalf("failed to create endpoint: %v, status: %v", err, resp)
	}

	var createdEp struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&createdEp)
	resp.Body.Close()
	epID := createdEp.ID
	t.Logf("Created endpoint ID: %s pointing to sink: %s", epID, sink.URL)

	// 5. Send webhooks concurrently and track ACKed events
	const totalEvents = 200
	var ackedIDs sync.Map
	var ackedCount int64

	var wg sync.WaitGroup
	killOnce := sync.Once{}

	t.Logf("Firing %d webhooks and executing abrupt kill -9 mid-flight...", totalEvents)

	for i := 0; i < totalEvents; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			eventID := uuid.New().String()
			body, _ := json.Marshal(map[string]string{
				"event_id": eventID,
				"idx":      fmt.Sprintf("%d", idx),
			})

			hookURL := fmt.Sprintf("http://127.0.0.1:%s/hooks/%s", testPort, epID)
			postReq, _ := http.NewRequest("POST", hookURL, bytes.NewBuffer(body))
			postReq.Header.Set("Content-Type", "application/json")

			hookClient := &http.Client{Timeout: 3 * time.Second}
			hookResp, hookErr := hookClient.Do(postReq)

			if hookErr == nil && hookResp.StatusCode == http.StatusOK {
				hookResp.Body.Close()
				ackedIDs.Store(eventID, true)
				currentAcked := atomic.AddInt64(&ackedCount, 1)

				// Kill the process abruptly mid-load after ~40-60 successful acks
				if currentAcked >= 50 {
					killOnce.Do(func() {
						t.Logf("KILL -9 TRIGGERED: Abruptly terminating Vahak after %d acknowledged requests...", currentAcked)
						if err := proc1.Process.Kill(); err != nil {
							t.Logf("Process kill warning: %v", err)
						}
					})
				}
			} else if hookResp != nil {
				hookResp.Body.Close()
			}
		}(i)

		// Stagger requests slightly to ensure in-flight pipeline occupancy
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for in-flight sender goroutines to finish (or error out after kill)
	wg.Wait()
	proc1.Wait() // ensure proc1 is completely terminated

	totalAcked := atomic.LoadInt64(&ackedCount)
	t.Logf("Vahak process 1 killed. Total requests that received 200 OK: %d", totalAcked)
	if totalAcked == 0 {
		t.Fatal("no requests were acknowledged before kill")
	}

	// 6. Restart Vahak with the SAME database (Process 2)
	t.Log("Restarting Vahak (Process 2) to trigger recovery sweeper...")
	proc2 := startVahak()
	defer func() {
		if proc2.Process != nil {
			proc2.Process.Kill()
		}
	}()

	if !waitForHealthy(10 * time.Second) {
		t.Fatal("Vahak process 2 failed to become healthy on restart")
	}

	// 7. Wait for the recovery sweeper to deliver all acknowledged requests to the sink
	t.Log("Waiting for recovery sweeper to deliver all acked requests...")
	deadline := time.Now().Add(25 * time.Second)
	var missing []string

	for time.Now().Before(deadline) {
		missing = nil
		ackedIDs.Range(func(key, value any) bool {
			id := key.(string)
			if _, received := receivedIDs.Load(id); !received {
				missing = append(missing, id)
			}
			return true
		})

		if len(missing) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 8. Assert: Provably zero acknowledged events lost
	if len(missing) > 0 {
		t.Fatalf("PROVABLE DATA LOSS DETECTED: %d / %d acknowledged events were lost after kill -9!\nMissing IDs: %v",
			len(missing), totalAcked, missing)
	}

	t.Logf("SUCCESS: All %d acknowledged events were durably recovered and delivered to the sink after kill -9!", totalAcked)
}
