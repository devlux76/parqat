package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestToParquet(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool // true if should succeed
	}{
		{
			name:     "simple JSON object",
			input:    `{"name": "John", "age": 30, "active": true}`,
			expected: true,
		},
		{
			name:     "multiple JSON objects",
			input:    `{"name": "John", "age": 30}` + "\n" + `{"name": "Jane", "age": 25}`,
			expected: true,
		},
		{
			name:     "empty input",
			input:    "",
			expected: true,
		},
		{
			name:     "invalid JSON",
			input:    `{"name": "John", "age":`,
			expected: false,
		},
		{
			name:     "JSON with array",
			input:    `{"name": "John", "hobbies": ["reading", "coding"]}`,
			expected: true,
		},
		{
			name:     "JSON with null values",
			input:    `{"name": "John", "middle_name": null, "age": 30}`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := strings.NewReader(tt.input)
			output := &bytes.Buffer{}

			err := ToParquet(output, input)
			if tt.expected && err != nil {
				t.Errorf("ToParquet() error = %v, want nil", err)
			}
			if !tt.expected && err == nil {
				t.Errorf("ToParquet() error = nil, want error")
			}

			if tt.expected && err == nil {
				// Verify we can read the parquet data back
				if output.Len() > 0 {
					reader := parquet.NewGenericReader[any](bytes.NewReader(output.Bytes()))
					defer reader.Close()

					// Try to read at least one row to verify format
					rows := make([]any, 1)
					_, readErr := reader.Read(rows)
					if readErr != nil && readErr != io.EOF {
						t.Errorf("Failed to read back parquet data: %v", readErr)
					}
				}
			}
		})
	}
}

func TestFromParquet(t *testing.T) {
	// Create a test parquet file
	testData := []map[string]any{
		{"name": "John", "age": 30.0, "active": true},
		{"name": "Jane", "age": 25.0, "active": false},
		{"name": "Bob", "age": 35.0, "active": true},
	}

	// Convert to parquet
	parquetBuf := &bytes.Buffer{}
	schema := parquet.NewSchema("test", parquet.Group{
		"name":   parquet.Optional(parquet.String()),
		"age":    parquet.Optional(parquet.Leaf(parquet.DoubleType)),
		"active": parquet.Optional(parquet.Leaf(parquet.BooleanType)),
	})

	err := parquet.Write(parquetBuf, testData, schema)
	if err != nil {
		t.Fatalf("Failed to create test parquet data: %v", err)
	}

	tests := []struct {
		name         string
		head         int
		tail         int
		expectedRows int
	}{
		{
			name:         "read all rows",
			head:         0,
			tail:         0,
			expectedRows: 3,
		},
		{
			name:         "read first 2 rows",
			head:         2,
			tail:         0,
			expectedRows: 2,
		},
		{
			name:         "read last 1 row",
			head:         0,
			tail:         1,
			expectedRows: 1,
		},
		{
			name:         "read first 10 rows (more than available)",
			head:         10,
			tail:         0,
			expectedRows: 3,
		},
		{
			name:         "read last 10 rows (more than available)",
			head:         0,
			tail:         10,
			expectedRows: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := bytes.NewReader(parquetBuf.Bytes())
			output := &bytes.Buffer{}

			err := FromParquet(output, input, tt.head, tt.tail)
			if err != nil {
				t.Errorf("FromParquet() error = %v, want nil", err)
			}

			// Count JSON lines in output
			outputStr := output.String()
			lines := strings.Split(strings.TrimSpace(outputStr), "\n")
			if outputStr == "" {
				lines = []string{}
			}

			if len(lines) != tt.expectedRows {
				t.Errorf("FromParquet() returned %d rows, want %d", len(lines), tt.expectedRows)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that JSON -> Parquet -> JSON preserves data
	testCases := []string{
		`{"name": "John", "age": 30, "active": true}`,
		`{"name": "Jane", "age": 25, "active": false}` + "\n" + `{"name": "Bob", "age": 35, "active": true}`,
		`{"id": 1, "value": null, "score": 99.5}`,
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("roundtrip_%d", i), func(t *testing.T) {
			// JSON -> Parquet
			jsonInput := strings.NewReader(testCase)
			parquetBuf := &bytes.Buffer{}
			err := ToParquet(parquetBuf, jsonInput)
			if err != nil {
				t.Fatalf("ToParquet() error = %v", err)
			}

			// Parquet -> JSON
			parquetInput := bytes.NewReader(parquetBuf.Bytes())
			jsonOutput := &bytes.Buffer{}
			err = FromParquet(jsonOutput, parquetInput, 0, 0)
			if err != nil {
				t.Fatalf("FromParquet() error = %v", err)
			}

			// Verify we got some output
			if jsonOutput.Len() == 0 {
				t.Error("Round trip produced no output")
			}

			// For more complex validation, we could parse both JSON inputs and outputs
			// and compare the data structures, but for now just verify we get valid JSON
			outputStr := strings.TrimSpace(jsonOutput.String())
			if outputStr != "" {
				lines := strings.Split(outputStr, "\n")
				for _, line := range lines {
					var obj map[string]any
					if err := json.Unmarshal([]byte(line), &obj); err != nil {
						t.Errorf("Round trip produced invalid JSON: %v", err)
					}
				}
			}
		})
	}
}

func TestEmptyFile(t *testing.T) {
	// Test empty parquet file
	emptyParquetBuf := &bytes.Buffer{}
	schema := parquet.NewSchema("empty", parquet.Group{
		"dummy": parquet.Optional(parquet.String()),
	})

	err := parquet.Write(emptyParquetBuf, []map[string]any{}, schema)
	if err != nil {
		t.Fatalf("Failed to create empty parquet file: %v", err)
	}

	input := bytes.NewReader(emptyParquetBuf.Bytes())
	output := &bytes.Buffer{}

	err = FromParquet(output, input, 0, 0)
	if err != nil {
		t.Errorf("FromParquet() with empty file error = %v, want nil", err)
	}

	if output.Len() != 0 {
		t.Errorf("FromParquet() with empty file produced output, want empty")
	}
}

func TestVersionInfo(t *testing.T) {
	// Test that version variables are set
	if version == "" {
		t.Error("version variable is empty")
	}
	if company == "" {
		t.Error("company variable is empty")
	}
}

// Helper function to create a temporary file for testing
func createTempFile(t *testing.T, content string) *os.File {
	file, err := os.CreateTemp("", "parqat_test_*.tmp")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	if _, err := file.WriteString(content); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to seek temp file: %v", err)
	}

	return file
}

func TestFromParquetFile(t *testing.T) {
	// Create test data
	testData := []map[string]any{
		{"name": "Alice", "score": 95.5},
		{"name": "Bob", "score": 87.2},
	}

	// Create a temporary parquet file
	tempFile := createTempFile(t, "")
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	schema := parquet.NewSchema("test", parquet.Group{
		"name":  parquet.Optional(parquet.String()),
		"score": parquet.Optional(parquet.Leaf(parquet.DoubleType)),
	})

	err := parquet.Write(tempFile, testData, schema)
	if err != nil {
		t.Fatalf("Failed to write test parquet file: %v", err)
	}

	// Test reading from file
	output := &bytes.Buffer{}
	err = FromParquetFile(output, tempFile.Name(), 0, 0)
	if err != nil {
		t.Errorf("FromParquetFile() error = %v, want nil", err)
	}

	// Verify output
	outputStr := strings.TrimSpace(output.String())
	lines := strings.Split(outputStr, "\n")
	if len(lines) != 2 {
		t.Errorf("FromParquetFile() returned %d lines, want 2", len(lines))
	}
}
