package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ServerStatus struct {
	Status string `json:"status"`
}

type JobID struct {
	JobID string `json:"job_id"`
}

type JobStatus struct {
	Status string `json:"status"`
}

const (
	SERVER_RUNNING = "running"
	JOB_RUNNING    = "running"
	JOB_FINISHED   = "finished"
)

var serverURL string

func serverIsRunning() error {

	fmt.Println("ServerURL:", serverURL)

	resp, err := http.Get(serverURL + "status")
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}

	var status ServerStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return fmt.Errorf("unmarshal: %v", err)
	}

	if err := resp.Body.Close(); err != nil {
		fmt.Println("Error closing response body:", err)
	}

	if status.Status != SERVER_RUNNING {
		return fmt.Errorf("status: %s", status.Status)
	}

	return nil
}

func extractSchema(dbConfigs []byte) (string, error) {
	resp, err := http.Post(serverURL+"extract", "application/json", bytes.NewBuffer(dbConfigs))
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %v", err)
	}

	var jobID JobID
	if err := json.Unmarshal(body, &jobID); err != nil {
		return "", fmt.Errorf(" unmarshal: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		fmt.Println("Error closing response body:", err)
	}

	if jobID.JobID == "" {
		return "", fmt.Errorf("job ID is empty")
	}

	return jobID.JobID, nil
}

func getJobStatus(jobID string) (string, error) {
	resp, err := http.Get(serverURL + "jobs/" + jobID)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("tatus code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %v", err)
	}

	var status JobStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return "", fmt.Errorf("unmarshal: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		fmt.Println("Error closing response body:", err)
	}

	return status.Status, nil
}

func waitForJobToFinish(jobID string) error {
	spinSigns := []string{"|", "/", "-", "\\"}
	spinIndex := 0
	for {
		status, err := getJobStatus(jobID)
		if err != nil {
			return fmt.Errorf("get job status: %v", err)
		}

		if status == JOB_FINISHED {
			fmt.Printf("\rJob finished successfully.\n")
			return nil
		} else if status != JOB_RUNNING {
			return fmt.Errorf("job failed with status: %s", status)
		}

		fmt.Printf("\r  %s %s", spinSigns[spinIndex], status)
		spinIndex = (spinIndex + 1) % len(spinSigns)
		time.Sleep(100 * time.Millisecond)
	}
}

func getJobResult(jobID string) ([]byte, error) {
	resp, err := http.Get(serverURL + "jobs/" + jobID + "/results")
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %v", err)
	}

	if err := resp.Body.Close(); err != nil {
		fmt.Println("Error closing response body:", err)
	}

	return body, nil
}

func getSchema(dbConfigs []byte) ([]byte, error) {
	// Check if the server is running
	if err := serverIsRunning(); err != nil {
		return nil, fmt.Errorf("server is not running: %v", err)
	}

	jobId, err := extractSchema(dbConfigs)
	if err != nil {
		return nil, fmt.Errorf("extracting schema: %v", err)
	}

	fmt.Printf("Schema extraction job ID: %s\n", jobId)

	// Wait for the job to finish
	if err := waitForJobToFinish(jobId); err != nil {
		return nil, fmt.Errorf("waiting for job to finish: %v", err)
	}

	// Get the job result
	result, err := getJobResult(jobId)
	if err != nil {
		return nil, fmt.Errorf("getting job result: %v", err)
	}

	return result, nil
}
