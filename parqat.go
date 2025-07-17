package main

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

/*
Package main implements the parqat CLI tool for converting between JSON and Parquet formats.
It provides high-performance, streaming, and SIMD-optimized options for large-scale data processing.
*/

// Version info (set by build flags)
var (
	version = "1.0.0"
	company = "syntropiq"
)

var rootCmd = &cobra.Command{
	Use:   "parqat [file]",
	Short: "A high-performance tool for converting between JSON and Parquet formats",
	// Long provides detailed usage, features, and examples for the CLI.
	Long: `parqat v` + version + ` - A SIMD-optimized, streaming tool for converting between JSON and Parquet formats.
Designed to sit in Unix pipelines like cat, tail, grep, sed, awk, etc.

Features:
- SIMD-optimized defaults (power-of-2 buffer sizes for Go 1.25+)
- Safe handling of complex types (arrays, maps, nested objects)
- Streaming mode for large datasets
- Multiple compression algorithms (zstd default for best performance)
- Configurable row group sizes and page buffers

If given a parquet file, converts it to JSON.
If given JSON from stdin, converts it to Parquet.

Examples:
  cat data.json | parqat -o data.parquet              # JSON to Parquet (default settings)
  cat data.json | parqat --streaming -o data.parquet  # Streaming mode for large files
  cat data.json | parqat --compression snappy -o data.parquet  # Use Snappy compression
  parqat data.parquet                                  # Parquet to JSON
  parqat data.parquet --head 10                        # First 10 rows
  parqat data.parquet --tail 5                         # Last 5 rows
  echo '{"name":"John","tags":["user","admin"]}' | parqat > data.parquet  # Complex JSON

Performance Options:
  --compression: none, snappy, gzip, zstd (default: zstd)
  --page-buffer-size: Buffer size in bytes (default: 262144 = 2^18)
  --max-rows-per-group: Rows per group (default: 1048576 = 2^20)
  --streaming: Enable for large datasets (uses temp files)

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

		// Create writer configuration from command line flags
		config := createWriterConfig()

		if enableStreaming {
			return StreamingToParquet(w, os.Stdin, config)
		}
		return ToParquetWithConfig(w, os.Stdin, config)
	},
}

func init() {
	// Basic flags
	rootCmd.Flags().IntVar(&head, "head", 0, "Number of rows to read from the beginning")
	rootCmd.Flags().IntVar(&tail, "tail", 0, "Number of rows to read from the end")
	rootCmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output Parquet file path. If not provided, output is written to stdout.")
	rootCmd.Flags().BoolP("version", "v", false, "Show version information")

	// Writer configuration flags (SIMD-optimized defaults)
	rootCmd.Flags().StringVar(&compressionType, "compression", "zstd", "Compression type: none, snappy, gzip, zstd (default: zstd for best performance)")
	rootCmd.Flags().IntVar(&pageBufferSize, "page-buffer-size", 256*1024, "Page buffer size in bytes (default: 262144 = 2^18, SIMD-optimized)")
	rootCmd.Flags().Int64Var(&maxRowsPerGroup, "max-rows-per-group", 1048576, "Maximum rows per row group (default: 1048576 = 2^20, SIMD-optimized)")
	rootCmd.Flags().IntVar(&dataPageVersion, "data-page-version", 2, "Data page version (1 or 2, default: 2 for better performance)")
	rootCmd.Flags().BoolVar(&enableDictionary, "enable-dictionary", true, "Enable dictionary encoding for better compression")
	rootCmd.Flags().BoolVar(&enableStreaming, "streaming", false, "Enable streaming mode for large datasets (uses temp files)")

	// Handle version flag
	rootCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if versionFlag, _ := cmd.Flags().GetBool("version"); versionFlag {
			fmt.Printf("parqat v%s\nCreated by %s\nhttps://github.com/syntropiq/parqat\n", version, company)
			os.Exit(0)
		}
		return nil
	}
}

/*
Execute runs the root Cobra command for the CLI.
It handles command-line parsing and error reporting.
*/
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

// Writer configuration flags with SIMD-optimized defaults
var (
	compressionType  string
	pageBufferSize   int
	maxRowsPerGroup  int64
	dataPageVersion  int
	enableDictionary bool
	enableStreaming  bool
)

/*
createWriterConfig creates a WriterConfig from command line flags.
It applies user-specified compression, buffer sizes, and other performance options.
*/
func createWriterConfig() WriterConfig {
	config := DefaultWriterConfig()

	// Override with command line flags
	switch compressionType {
	case "none":
		config.Codec = &parquet.Uncompressed
	case "snappy":
		config.Codec = &parquet.Snappy
	case "gzip":
		config.Codec = &parquet.Gzip
	case "zstd":
		config.Codec = &parquet.Zstd
		// default already set to zstd in DefaultWriterConfig
	}

	config.PageBufferSize = pageBufferSize
	config.MaxRowsPerRowGroup = maxRowsPerGroup
	config.DataPageVersion = dataPageVersion
	config.UseDictionary = enableDictionary

	return config
}

/*
ToParquetWithConfig wraps ToParquet with a custom WriterConfig.
It allows fine-tuned control over Parquet writing parameters.
*/
func ToParquetWithConfig(w io.Writer, r io.Reader, config WriterConfig) error {
	return toParquetOptimized(w, r, config)
}
