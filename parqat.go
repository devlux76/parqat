package main

import (
	"fmt"
	"io"
	"os"

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
