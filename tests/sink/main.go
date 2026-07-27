package main

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

var count uint64

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Just read the body and discard to avoid broken pipes
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
		
		atomic.AddUint64(&count, 1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Print stats every second
	go func() {
		for {
			time.Sleep(time.Second)
			current := atomic.SwapUint64(&count, 0)
			if current > 0 {
				fmt.Printf("Received %d requests in the last second\n", current)
			}
		}
	}()

	fmt.Println("Dummy Sink Server listening on :9090")
	if err := http.ListenAndServe(":9090", nil); err != nil {
		fmt.Println("Server failed:", err)
	}
}

