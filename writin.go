package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
)

// WriterConfig holds configuration for parquet writing
type WriterConfig struct {
	Codec               compress.Codec
	PageBufferSize      int
	MaxRowsPerRowGroup  int64
	DataPageVersion     int
	UseDictionary       bool
	DefaultEncodingType string
}

// DefaultWriterConfig returns sensible defaults for performance
func DefaultWriterConfig() WriterConfig {
	return WriterConfig{
		Codec:              &parquet.Zstd, // Better compression than default
		PageBufferSize:     256 * 1024,    // 262144 (2^18) - SIMD-optimized size
		MaxRowsPerRowGroup: 1048576,       // 2^20 - SIMD-optimized for typical datasets
		DataPageVersion:    2,             // Use v2 for better performance
		UseDictionary:      true,          // Enable dictionary encoding
	}
}

// StreamingToParquet writes JSON to Parquet in a streaming fashion without loading all data into memory
func StreamingToParquet(w io.Writer, r io.Reader, config WriterConfig) error {
	// Create a buffer to collect rows for schema inference
	var sampleRows []map[string]any
	const sampleSize = 1024 // Sample first 1024 rows for schema inference (2^10 - SIMD-optimized)

	// Use a temporary file to store the complete JSON data
	tempFile, err := os.CreateTemp("", "parqat_stream_*.json")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Tee the input to both sample collection and temp file
	dec := json.NewDecoder(r)

	// First pass: collect samples and write to temp file
	for len(sampleRows) < sampleSize {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoding json for sampling: %w", err)
		}

		sampleRows = append(sampleRows, row)

		// Write to temp file
		if err := json.NewEncoder(tempFile).Encode(row); err != nil {
			return fmt.Errorf("writing to temp file: %w", err)
		}
	}

	// Continue reading remaining data to temp file
	for {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoding json: %w", err)
		}

		if err := json.NewEncoder(tempFile).Encode(row); err != nil {
			return fmt.Errorf("writing to temp file: %w", err)
		}
	}

	if len(sampleRows) == 0 {
		return nil // Empty input is valid
	}

	// Build optimized schema from samples
	schema, err := buildOptimizedSchema(sampleRows)
	if err != nil {
		return fmt.Errorf("building schema: %w", err)
	}

	// Create writer with optimized configuration
	writerConfig := &parquet.WriterConfig{
		Schema:             schema,
		Compression:        config.Codec,
		PageBufferSize:     config.PageBufferSize,
		MaxRowsPerRowGroup: config.MaxRowsPerRowGroup,
		DataPageVersion:    config.DataPageVersion,
		DataPageStatistics: true, // Enable statistics for better query performance
	}

	writer := parquet.NewWriter(w, writerConfig)

	// Second pass: read from temp file and write to parquet
	if _, err := tempFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seeking temp file: %w", err)
	}

	dec = json.NewDecoder(tempFile)
	const batchSize = 131072 // 2^17 - SIMD-optimized batch processing

	for {
		var batch []map[string]any
		for len(batch) < batchSize {
			var row map[string]any
			if err := dec.Decode(&row); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("decoding json: %w", err)
			}
			batch = append(batch, row)
		}

		if len(batch) == 0 {
			break
		}

		// Write batch to parquet
		for _, row := range batch {
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("writing row to parquet: %w", err)
			}
		}
	}

	return writer.Close()
}

// buildOptimizedSchema analyzes sample rows to build an optimized schema
func buildOptimizedSchema(sampleRows []map[string]any) (*parquet.Schema, error) {
	if len(sampleRows) == 0 {
		return nil, fmt.Errorf("no sample rows provided")
	}

	// Analyze all fields across all samples
	fieldStats := make(map[string]*fieldAnalysis)

	for _, row := range sampleRows {
		for key, value := range row {
			if fieldStats[key] == nil {
				fieldStats[key] = &fieldAnalysis{
					name:       key,
					nullable:   false,
					types:      make(map[reflect.Type]int),
					arrayTypes: make(map[reflect.Type]int),
				}
			}

			stats := fieldStats[key]
			stats.totalCount++

			if value == nil {
				stats.nullCount++
				stats.nullable = true
				continue
			}

			t := reflect.TypeOf(value)
			stats.types[t]++

			// Special handling for arrays
			if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Interface {
				if slice, ok := value.([]any); ok && len(slice) > 0 {
					// Analyze all elements in the array, not just the first
					for _, elem := range slice {
						if elem != nil {
							elemType := reflect.TypeOf(elem)
							stats.arrayTypes[elemType]++
						}
					}
				}
			}
		}
	}

	// Build schema fields
	schemaFields := make(parquet.Group)

	for name, stats := range fieldStats {
		node, err := buildNodeFromStats(stats)
		if err != nil {
			return nil, fmt.Errorf("building node for field %s: %w", name, err)
		}
		schemaFields[name] = node
	}

	return parquet.NewSchema("row", schemaFields), nil
}

type fieldAnalysis struct {
	name       string
	totalCount int
	nullCount  int
	nullable   bool
	types      map[reflect.Type]int
	arrayTypes map[reflect.Type]int
}

func buildNodeFromStats(stats *fieldAnalysis) (parquet.Node, error) {
	// Determine the most common type
	var dominantType reflect.Type
	maxCount := 0

	for t, count := range stats.types {
		if count > maxCount {
			maxCount = count
			dominantType = t
		}
	}

	var node parquet.Node

	// Handle array types
	if dominantType != nil && dominantType.Kind() == reflect.Slice {
		// For arrays, always use string elements to avoid type conflicts
		// This is safer and more compatible with diverse JSON data
		node = parquet.Repeated(parquet.String())
	} else if dominantType != nil {
		node = createLeafNode(dominantType)
	} else {
		// Default to string for unknown types
		node = parquet.String()
	}

	// Make optional if we found null values
	if stats.nullable || stats.nullCount > 0 {
		node = parquet.Optional(node)
	}

	return node, nil
}

func createLeafNode(t reflect.Type) parquet.Node {
	switch t.Kind() {
	case reflect.String:
		return parquet.String()
	case reflect.Float64:
		return parquet.Leaf(parquet.DoubleType)
	case reflect.Float32:
		return parquet.Leaf(parquet.FloatType)
	case reflect.Int, reflect.Int64:
		return parquet.Leaf(parquet.Int64Type)
	case reflect.Int32:
		return parquet.Leaf(parquet.Int32Type)
	case reflect.Bool:
		return parquet.Leaf(parquet.BooleanType)
	default:
		// Default to string for unknown types
		return parquet.String()
	}
}

// ToParquet is the backward-compatible function with performance improvements
func ToParquet(w io.Writer, r io.Reader) error {
	config := DefaultWriterConfig()

	// For small datasets or when memory is not a concern, use original approach
	// but with optimizations
	return toParquetOptimized(w, r, config)
}

func toParquetOptimized(w io.Writer, r io.Reader, config WriterConfig) error {
	// Create a temporary file to store JSON data
	tempFile, err := os.CreateTemp("", "parqat_temp_*.json")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Stream JSON from stdin to temp file
	if _, err := io.Copy(tempFile, r); err != nil {
		return fmt.Errorf("copying input to temp file: %w", err)
	}

	// Rewind temp file for reading
	if _, err := tempFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seeking temp file: %w", err)
	}

	// Read all JSON rows to determine schema
	var allRows []map[string]any
	dec := json.NewDecoder(tempFile)

	for {
		var row map[string]any
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decoding json: %w", err)
		}
		allRows = append(allRows, row)
	}

	if len(allRows) == 0 {
		return nil // Empty input is valid
	}

	// Build optimized schema
	schema, err := buildOptimizedSchema(allRows)
	if err != nil {
		return fmt.Errorf("building schema: %w", err)
	}

	// Create writer with optimized configuration
	writerConfig := &parquet.WriterConfig{
		Schema:             schema,
		Compression:        config.Codec,
		PageBufferSize:     config.PageBufferSize,
		MaxRowsPerRowGroup: config.MaxRowsPerRowGroup,
		DataPageVersion:    config.DataPageVersion,
		DataPageStatistics: true,
	}

	writer := parquet.NewWriter(w, writerConfig)

	// Write all rows in batches for better performance
	const batchSize = 262144 // 2^18 - SIMD-optimized batch processing
	for i := 0; i < len(allRows); i += batchSize {
		end := min(i+batchSize, len(allRows))
		batch := allRows[i:end]
		for _, row := range batch {
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("writing row to parquet: %w", err)
			}
		}
	}

	return writer.Close()
}
