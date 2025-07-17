package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

func FromParquet(w io.Writer, r io.Reader, head, tail int) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("empty input")
	}

	pr, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("opening parquet data: %w", err)
	}

	return fromParquet(w, pr, head, tail)
}

func FromParquetFile(w io.Writer, filePath string, head, tail int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file info for %s: %w", filePath, err)
	}

	pr, err := parquet.OpenFile(file, fileInfo.Size())
	if err != nil {
		return fmt.Errorf("opening parquet file %s: %w", filePath, err)
	}

	return fromParquet(w, pr, head, tail)
}

func fromParquet(w io.Writer, pr *parquet.File, head, tail int) error {
	// Use buffered writer for better performance
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	enc := json.NewEncoder(bw)
	numRows := pr.NumRows()

	if numRows == 0 {
		return nil // No rows to process
	}

	// Use GenericReader with any type, like in the test examples
	reader := parquet.NewGenericReader[any](pr)
	defer reader.Close()

	// Read all rows
	allRows := make([]any, numRows)
	n, err := reader.Read(allRows)
	if err != nil && err != io.EOF {
		return fmt.Errorf("reading parquet data: %w", err)
	}

	// Trim to actual rows read
	allRows = allRows[:n]

	// Apply head/tail logic
	var rowsToOutput []any
	if head > 0 {
		end := head
		if end > len(allRows) {
			end = len(allRows)
		}
		rowsToOutput = allRows[:end]
	} else if tail > 0 {
		start := len(allRows) - tail
		if start < 0 {
			start = 0
		}
		rowsToOutput = allRows[start:]
	} else {
		rowsToOutput = allRows
	}

	// Write each row as JSON
	for _, row := range rowsToOutput {
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("encoding json: %w", err)
		}
	}

	return nil
}
