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

	t.Log("Building Vahak binary...")
	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/server")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build binary: %v\nOutput: %s", err, string(out))
	}

	testPort := "8099"
	testAPIKey := "chaos_test_secret_key"

	startVahak := func(currentPort, currentDBPath string) *exec.Cmd {
		cmd := exec.Command(binPath)
		cmd.Env = append(os.Environ(),
			"PORT="+currentPort,
			"DB_ENGINE=sqlite",
			"DB_PATH="+currentDBPath,
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

	waitForHealthy := func(port string, timeout time.Duration) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/health", port))
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

	const totalIterations = 3
	var totalAckedAllIterations int64
	var totalDuplicatesAllIterations int64

	for iter := 1; iter <= totalIterations; iter++ {
		t.Logf("=== STARTING CHAOS RUN %d of %d ===", iter, totalIterations)
		iterDBPath := filepath.Join(tmpDir, fmt.Sprintf("crash_test_iter_%d.db", iter))

		// Start Vahak (Process 1)
		proc1 := startVahak(testPort, iterDBPath)

		if !waitForHealthy(testPort, 10*time.Second) {
			proc1.Process.Kill()
			t.Fatalf("Iteration %d: Vahak process 1 failed to become healthy", iter)
		}

		// Register endpoint pointing to test sink
		createEpBody, _ := json.Marshal(map[string]string{
			"name":       fmt.Sprintf("chaos-endpoint-%d", iter),
			"target_url": sink.URL,
		})
		req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%s/api/endpoints", testPort), bytes.NewBuffer(createEpBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-Key", testAPIKey)

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)
		if err != nil || resp.StatusCode != http.StatusCreated {
			proc1.Process.Kill()
			t.Fatalf("Iteration %d: failed to create endpoint: %v, status: %v", iter, err, resp)
		}

		var createdEp struct {
			ID string `json:"id"`
		}
		json.NewDecoder(resp.Body).Decode(&createdEp)
		resp.Body.Close()
		epID := createdEp.ID

		// Send webhooks concurrently and randomly trigger kill -9
		const totalEvents = 150
		var ackedIDs sync.Map
		var ackedCount int64
		var wg sync.WaitGroup
		killOnce := sync.Once{}

		// Randomized kill threshold between 25 and 55 successful ACKs
		killThreshold := int64(25 + (time.Now().UnixNano() % 30))
		t.Logf("Iteration %d: Firing %d webhooks. Randomized kill threshold: %d ACKs...", iter, totalEvents, killThreshold)

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

					if currentAcked >= killThreshold {
						killOnce.Do(func() {
							t.Logf("Iteration %d: KILL -9 TRIGGERED at %d acknowledged requests", iter, currentAcked)
							_ = proc1.Process.Kill()
						})
					}
				} else if hookResp != nil {
					hookResp.Body.Close()
				}
			}(i)

			time.Sleep(4 * time.Millisecond)
		}

		wg.Wait()
		proc1.Wait()

		iterAcked := atomic.LoadInt64(&ackedCount)
		t.Logf("Iteration %d: Process 1 terminated. Acknowledged: %d", iter, iterAcked)
		if iterAcked == 0 {
			t.Fatalf("Iteration %d: no requests acknowledged before kill", iter)
		}
		totalAckedAllIterations += iterAcked

		// Restart Vahak (Process 2) with the SAME database
		t.Logf("Iteration %d: Restarting Vahak (Process 2) to trigger recovery sweeper...", iter)
		proc2 := startVahak(testPort, iterDBPath)

		if !waitForHealthy(testPort, 10*time.Second) {
			proc2.Process.Kill()
			t.Fatalf("Iteration %d: Process 2 failed to become healthy on restart", iter)
		}

		// Wait for lease expiry and sweeper delivery of all acked requests
		t.Logf("Iteration %d: Waiting for recovery sweeper...", iter)
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

		proc2.Process.Kill()
		proc2.Wait()

		if len(missing) > 0 {
			t.Fatalf("Iteration %d: DATA LOSS DETECTED: %d / %d acknowledged events lost after kill -9!\nMissing IDs: %v",
				iter, len(missing), iterAcked, missing)
		}

		// Calculate duplicates for this iteration
		currentTotalReceived := atomic.LoadInt64(&receivedCount)
		var uniqueReceivedCount int64
		receivedIDs.Range(func(_, _ any) bool {
			uniqueReceivedCount++
			return true
		})
		iterDuplicates := currentTotalReceived - uniqueReceivedCount
		totalDuplicatesAllIterations = iterDuplicates

		t.Logf("Iteration %d SUCCESS: All %d acknowledged events durably delivered. (Cumulative duplicates: %d)",
			iter, iterAcked, iterDuplicates)
	}

	t.Logf("================================================================================")
	t.Logf("CHAOS TEST COMPLETE: Verified ZERO acknowledged-event loss across %d randomized kill -9 runs.", totalIterations)
	t.Logf("Total acknowledged requests recovered: %d | Total benign duplicates: %d", totalAckedAllIterations, totalDuplicatesAllIterations)
	t.Logf("================================================================================")
}
