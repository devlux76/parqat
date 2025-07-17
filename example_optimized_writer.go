// This is an example file showing how to use the optimized parquet writer
// To run this example, create a separate main function or integrate into your application

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// ExampleOptimizedWriter demonstrates the new optimized parquet writer
func ExampleOptimizedWriter() {
	// Sample JSON data
	jsonData := `{"name": "John", "age": 30, "active": true, "scores": [85, 92, 78]}
{"name": "Jane", "age": 25, "active": false, "scores": [90, 88, 95]}
{"name": "Bob", "age": 35, "active": true, "scores": [82, 87, 91]}
`

	// Example 1: Using default configuration
	fmt.Println("Example 1: Default configuration")
	var buf1 bytes.Buffer
	if err := ToParquet(&buf1, strings.NewReader(jsonData)); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Generated parquet file size: %d bytes\n\n", buf1.Len())

	// Example 2: Using custom configuration for better compression
	fmt.Println("Example 2: Custom configuration with better compression")
	config := WriterConfig{
		Codec:              &parquet.Zstd, // Better compression
		PageBufferSize:     128 * 1024,    // 131072 (2^17) - SIMD-optimized size
		MaxRowsPerRowGroup: 131072,        // 2^17 - SIMD-optimized for medium datasets
		DataPageVersion:    2,             // Use latest version
		UseDictionary:      true,          // Enable dictionary encoding
	}

	var buf2 bytes.Buffer
	if err := toParquetOptimized(&buf2, strings.NewReader(jsonData), config); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Generated parquet file size with custom config: %d bytes\n\n", buf2.Len())

	// Example 3: Using streaming approach for large datasets
	fmt.Println("Example 3: Streaming approach")
	var buf3 bytes.Buffer
	if err := StreamingToParquet(&buf3, strings.NewReader(jsonData), config); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Generated parquet file size with streaming: %d bytes\n\n", buf3.Len())

	// Example 4: Performance comparison
	fmt.Println("Example 4: Performance comparison")
	largeJsonData := generateLargeJsonData(1024) // 2^10 - SIMD-optimized size

	// Time original approach
	var bufOrig bytes.Buffer
	if err := ToParquet(&bufOrig, strings.NewReader(largeJsonData)); err != nil {
		fmt.Printf("Error with original: %v\n", err)
		return
	}

	// Time optimized approach
	var bufOpt bytes.Buffer
	if err := toParquetOptimized(&bufOpt, strings.NewReader(largeJsonData), DefaultWriterConfig()); err != nil {
		fmt.Printf("Error with optimized: %v\n", err)
		return
	}

	fmt.Printf("Original approach: %d bytes\n", bufOrig.Len())
	fmt.Printf("Optimized approach: %d bytes\n", bufOpt.Len())
	fmt.Printf("Compression improvement: %.2f%%\n",
		float64(bufOrig.Len()-bufOpt.Len())/float64(bufOrig.Len())*100)
}

// generateLargeJsonData creates a large JSON dataset for testing
func generateLargeJsonData(numRows int) string {
	var buf bytes.Buffer
	for i := 0; i < numRows; i++ {
		row := map[string]interface{}{
			"id":       i,
			"name":     fmt.Sprintf("User%d", i),
			"email":    fmt.Sprintf("user%d@example.com", i),
			"age":      20 + (i % 50),
			"active":   i%2 == 0,
			"score":    float64(60 + (i % 40)),
			"tags":     []string{"tag1", "tag2", "tag3"},
			"metadata": map[string]string{"key1": "value1", "key2": "value2"},
		}

		jsonBytes, _ := json.Marshal(row)
		buf.Write(jsonBytes)
		buf.WriteByte('\n')
	}
	return buf.String()
}
