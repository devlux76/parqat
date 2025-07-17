package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// BenchmarkOriginalToParquet benchmarks the original ToParquet function
func BenchmarkOriginalToParquet(b *testing.B) {
	data := generateBenchmarkData(1024) // 2^10 - SIMD-optimized size

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := ToParquet(&buf, strings.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOptimizedToParquet benchmarks the optimized ToParquet function
func BenchmarkOptimizedToParquet(b *testing.B) {
	data := generateBenchmarkData(1024) // 2^10 - SIMD-optimized size
	config := DefaultWriterConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := toParquetOptimized(&buf, strings.NewReader(data), config)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStreamingToParquet benchmarks the streaming approach
func BenchmarkStreamingToParquet(b *testing.B) {
	data := generateBenchmarkData(1024) // 2^10 - SIMD-optimized size
	config := DefaultWriterConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := StreamingToParquet(&buf, strings.NewReader(data), config)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCompressionComparison compares different compression algorithms
func BenchmarkCompressionComparison(b *testing.B) {
	data := generateBenchmarkData(1024) // 2^10 - SIMD-optimized size

	codecs := map[string]func() WriterConfig{
		"Uncompressed": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Uncompressed
			return config
		},
		"Snappy": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Snappy
			return config
		},
		"Gzip": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Gzip
			return config
		},
		"Zstd": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Zstd
			return config
		},
	}

	for name, configFunc := range codecs {
		b.Run(name, func(b *testing.B) {
			config := configFunc()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer
				err := toParquetOptimized(&buf, strings.NewReader(data), config)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestCompressionRatio tests compression ratios of different algorithms
func TestCompressionRatio(t *testing.T) {
	data := generateBenchmarkData(4096) // 2^12 - SIMD-optimized size

	codecs := map[string]func() WriterConfig{
		"Uncompressed": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Uncompressed
			return config
		},
		"Snappy": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Snappy
			return config
		},
		"Gzip": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Gzip
			return config
		},
		"Zstd": func() WriterConfig {
			config := DefaultWriterConfig()
			config.Codec = &parquet.Zstd
			return config
		},
	}

	var uncompressedSize int
	results := make(map[string]int)

	for name, configFunc := range codecs {
		config := configFunc()
		var buf bytes.Buffer
		start := time.Now()
		err := toParquetOptimized(&buf, strings.NewReader(data), config)
		duration := time.Since(start)

		if err != nil {
			t.Errorf("Error with %s: %v", name, err)
			continue
		}

		size := buf.Len()
		results[name] = size

		if name == "Uncompressed" {
			uncompressedSize = size
		}

		t.Logf("%s: %d bytes, %v duration", name, size, duration)
	}

	// Calculate compression ratios
	for name, size := range results {
		if name != "Uncompressed" && uncompressedSize > 0 {
			ratio := float64(uncompressedSize-size) / float64(uncompressedSize) * 100
			t.Logf("%s compression ratio: %.2f%%", name, ratio)
		}
	}
}

// TestSchemaInference tests the schema inference improvements
func TestSchemaInference(t *testing.T) {
	// Test data with mixed types and null values
	testData := []string{
		`{"id": 1, "name": "Alice", "age": 30, "active": true, "score": 95.5, "tags": ["user", "admin"], "metadata": null}`,
		`{"id": 2, "name": "Bob", "age": null, "active": false, "score": 87.2, "tags": ["user"], "metadata": {"role": "viewer"}}`,
		`{"id": 3, "name": "Charlie", "age": 25, "active": true, "score": null, "tags": [], "metadata": {"role": "editor", "team": "engineering"}}`,
	}

	data := strings.Join(testData, "\n")

	// Test with optimized schema inference
	var buf bytes.Buffer
	config := DefaultWriterConfig()
	err := toParquetOptimized(&buf, strings.NewReader(data), config)
	if err != nil {
		t.Fatalf("Schema inference failed: %v", err)
	}

	t.Logf("Successfully generated parquet file with mixed types: %d bytes", buf.Len())

	// Verify we can read it back
	file, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("Failed to open generated parquet file: %v", err)
	}

	t.Logf("Parquet file has %d row groups with %d total rows", len(file.RowGroups()), file.NumRows())

	// Verify schema structure
	schema := file.Schema()
	t.Logf("Schema: %s", schema.String())
}

// generateBenchmarkData creates realistic test data for benchmarking
func generateBenchmarkData(numRows int) string {
	var buf bytes.Buffer

	for i := 0; i < numRows; i++ {
		row := map[string]interface{}{
			"id":         i,
			"name":       fmt.Sprintf("User_%d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"age":        20 + (i % 60),
			"active":     i%3 != 0,
			"score":      float64(60+(i%40)) + float64(i%100)/100.0,
			"department": []string{"Engineering", "Marketing", "Sales", "HR"}[i%4],
			"tags":       []string{"tag1", "tag2", "tag3"}[:1+(i%3)],
			"created_at": fmt.Sprintf("2023-%02d-%02d", 1+(i%12), 1+(i%28)),
		}

		// Add some null values
		if i%10 == 0 {
			row["age"] = nil
		}
		if i%15 == 0 {
			row["score"] = nil
		}
		if i%20 == 0 {
			row["tags"] = nil
		}

		jsonBytes, _ := json.Marshal(row)
		buf.Write(jsonBytes)
		buf.WriteByte('\n')
	}

	return buf.String()
}
