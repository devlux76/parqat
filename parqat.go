package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

// Version info (set by build flags)
var (
	version = "dev"
	company = "syntropiq"
)

var rootCmd = &cobra.Command{
	Use:   "parqat [file]",
	Short: "A simple cat-like tool for converting between JSON and Parquet formats",
	Long: `parqat v` + version + ` - A lightweight, streaming tool for converting between JSON and Parquet formats.
Designed to sit in Unix pipelines like cat, tail, grep, sed, awk, etc.

If given a parquet file, converts it to JSON.
If given JSON from stdin, converts it to Parquet.

Examples:
  cat data.json | parqat -o data.parquet        # JSON to Parquet
  parqat data.parquet                           # Parquet to JSON
  parqat data.parquet --head 10                 # First 10 rows
  parqat data.parquet --tail 5                  # Last 5 rows
  echo '{"name":"John"}' | parqat > data.parquet # JSON to Parquet

Created by ` + company + ` - https://github.com/syntropiq/parqat`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) > 0 {
			// File provided - convert Parquet to JSON
			return FromParquetFile(os.Stdout, args[0], head, tail)
		}
		// No file - convert JSON from stdin to Parquet

		// Validate that head/tail aren't used when converting JSON to Parquet
		if head > 0 || tail > 0 {
			return fmt.Errorf("--head and --tail flags can only be used when reading parquet files")
		}

		var w io.Writer
		if outputPath == "" {
			w = os.Stdout
		} else {
			file, err := os.Create(outputPath)
			if err != nil {
				return fmt.Errorf("creating output file: %w", err)
			}
			defer file.Close()
			w = file
		}
		return ToParquet(w, os.Stdin)
	},
}

func init() {
	rootCmd.Flags().IntVar(&head, "head", 0, "Number of rows to read from the beginning")
	rootCmd.Flags().IntVar(&tail, "tail", 0, "Number of rows to read from the end")
	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output Parquet file path. If not provided, output is written to stdout.")
	rootCmd.Flags().BoolP("version", "v", false, "Show version information")

	// Handle version flag
	rootCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if versionFlag, _ := cmd.Flags().GetBool("version"); versionFlag {
			fmt.Printf("parqat v%s\nCreated by %s\nhttps://github.com/syntropiq/parqat\n", version, company)
			os.Exit(0)
		}
		return nil
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	Execute()
}

var outputPath string

var (
	head int
	tail int
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
		case []interface{}:
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
