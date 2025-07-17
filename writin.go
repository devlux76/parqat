package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

func ToParquet(w io.Writer, r io.Reader) error {
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

	// Build schema from first row
	firstRow := allRows[0]
	schemaFields := make(parquet.Group)

	for key, value := range firstRow {
		var node parquet.Node
		switch value.(type) {
		case nil:
			node = parquet.Optional(parquet.String())
		case string:
			node = parquet.Optional(parquet.String())
		case float64:
			node = parquet.Optional(parquet.Leaf(parquet.DoubleType))
		case bool:
			node = parquet.Optional(parquet.Leaf(parquet.BooleanType))
		case []any:
			// For arrays, we'll treat them as repeated strings for now
			node = parquet.Repeated(parquet.String())
		default:
			// Default to string for unknown types
			node = parquet.Optional(parquet.String())
		}
		schemaFields[key] = node
	}

	schema := parquet.NewSchema("row", schemaFields)

	// Use the Write function with explicit schema
	return parquet.Write(w, allRows, schema)
}
