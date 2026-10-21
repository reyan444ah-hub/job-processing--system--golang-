package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type RegisterResponse struct {
	AccessToken string `json:"access_token"`
}

type JobResponse struct {
	Job struct {
		ID string `json:"id"`
	} `json:"job"`
	Deduplicated bool `json:"deduplicated"`
}

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "Base API URL")
	concurrency := flag.Int("c", 20, "Concurrent goroutines")
	totalRequests := flag.Int("n", 100, "Total job requests")
	flag.Parse()

	fmt.Printf("==> Starting Load Test: %d jobs with concurrency %d on %s\n", *totalRequests, *concurrency, *baseURL)

	client := &http.Client{Timeout: 10 * time.Second}

	// 1. Register test tenant
	regPayload := fmt.Sprintf(`{"organization_name":"LoadTestOrg_%d","email":"load_%d@test.com","password":"Password123!"}`, time.Now().Unix(), time.Now().Unix())
	resp, err := client.Post(*baseURL+"/api/v1/auth/register", "application/json", bytes.NewBufferString(regPayload))
	if err != nil {
		fmt.Printf("Failed to register tenant: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var regData RegisterResponse
	_ = json.NewDecoder(resp.Body).Decode(&regData)
	token := regData.AccessToken

	if token == "" {
		fmt.Println("Error: Failed to obtain access token. Is the server running?")
		return
	}

	fmt.Println("==> Authenticated successfully. Dispatching jobs...")

	start := time.Now()
	var successCount int64
	var errorCount int64

	jobsChan := make(chan int, *totalRequests)
	for i := 0; i < *totalRequests; i++ {
		jobsChan <- i
	}
	close(jobsChan)

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobsChan {
				jobReqBody := fmt.Sprintf(`{"type":"generic.benchmark","payload":{"index":%d},"priority":1,"max_attempts":3}`, idx)
				req, err := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/jobs", bytes.NewBufferString(jobReqBody))
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}

				req.Header.Set("Authorization", "Bearer "+token)
				req.Header.Set("Content-Type", "application/json")

				res, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				res.Body.Close()

				if res.StatusCode == http.StatusAccepted {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&errorCount, 1)
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)

	rps := float64(successCount) / duration.Seconds()

	fmt.Println("\n================ LOAD TEST RESULTS ================")
	fmt.Printf("Total Requests:       %d\n", *totalRequests)
	fmt.Printf("Accepted (202):       %d\n", successCount)
	fmt.Printf("Errors:               %d\n", errorCount)
	fmt.Printf("Total Elapsed Time:   %v\n", duration)
	fmt.Printf("Throughput (Req/Sec): %.2f req/s\n", rps)
	fmt.Println("===================================================")
}
