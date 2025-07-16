# parqat

[![Go](https://img.shields.io/badge/Go-1.18%2B-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/syntropiq/parqat)](https://github.com/syntropiq/parqat/releases)

A lightweight, fast, and simple cat-like tool for converting between JSON and Parquet formats. Designed to sit perfectly in Unix pipelines alongside tools like `cat`, `tail`, `grep`, `sed`, `awk`, and more.

## Features

- **Bidirectional conversion**: JSON ↔ Parquet
- **Streaming support**: Process large files efficiently
- **Pipeline friendly**: Designed for Unix pipelines
- **Head/tail support**: Extract specific rows from Parquet files
- **Schema inference**: Automatically detects JSON structure
- **Static binary**: No dependencies, runs anywhere
- **Fast**: Built with Go and optimized for performance

## Installation

### Pre-built binaries

Download the latest release from the [releases page](https://github.com/syntropiq/parqat/releases).

### Build from source

```bash
git clone https://github.com/syntropiq/parqat.git
cd parqat
make install
```

This will build a statically linked, UPX-compressed binary and install it to `/usr/local/bin`.

### Using Go

```bash
go install github.com/syntropiq/parqat@latest
```

## Usage

### Basic Examples

```bash
# Convert JSON to Parquet
cat data.json | parqat -o data.parquet

# Convert Parquet to JSON
parqat data.parquet

# Convert JSON from stdin to Parquet on stdout
echo '{"name":"John","age":30}' | parqat > output.parquet

# Read first 10 rows from Parquet file
parqat data.parquet --head 10

# Read last 5 rows from Parquet file  
parqat data.parquet --tail 5
```

### Pipeline Examples

```bash
# Process JSON, convert to Parquet, then back to JSON
cat input.json | parqat | parqat output.parquet

# Extract specific rows and convert format
parqat large_data.parquet --head 1000 | jq '.name' | sort | uniq

# Filter and convert
cat logs.json | jq 'select(.level == "error")' | parqat -o errors.parquet

# Combine multiple JSON files into one Parquet file
cat file1.json file2.json file3.json | parqat -o combined.parquet
```

### Command Line Options

```
Usage:
  parqat [file] [flags]

Flags:
  -h, --help            Show help message
  -v, --version         Show version information
  -o, --output string   Output Parquet file path. If not provided, output is written to stdout.
      --head int        Number of rows to read from the beginning (only for Parquet input)
      --tail int        Number of rows to read from the end (only for Parquet input)
```

## Data Type Mapping

parqat automatically infers Parquet schema from JSON data:

| JSON Type | Parquet Type |
|-----------|--------------|
| `string`  | `STRING`     |
| `number`  | `DOUBLE`     |
| `boolean` | `BOOLEAN`    |
| `null`    | `OPTIONAL`   |
| `array`   | `REPEATED`   |

All fields are treated as optional to handle varying JSON structures.

## Performance

parqat is designed for performance:

- **Static binary**: No runtime dependencies
- **Streaming processing**: Efficient memory usage for large files
- **Optimized builds**: Uses Go's optimization flags and UPX compression
- **Minimal overhead**: Direct conversion with minimal data copying

## Examples

### Convert log files

```bash
# Convert application logs to Parquet for analysis
cat app.log | jq -c '.' | parqat -o logs.parquet

# Analyze the converted logs
parqat logs.parquet | jq 'select(.level == "ERROR")' | wc -l
```

### ETL Pipeline

```bash
# Extract data from API, transform, and load into Parquet
curl -s "https://api.example.com/data" | \
  jq -c '.results[]' | \
  parqat -o processed_data.parquet
```

### Data Sampling

```bash
# Sample first 1000 rows from large Parquet file
parqat huge_dataset.parquet --head 1000 > sample.json
```

## Building

### Development build

```bash
go build -o parqat .
```

### Production build

```bash
make build          # Build static binary
make build-upx      # Build and compress with UPX
make install        # Build, compress, and install to /usr/local/bin
make build-all      # Build for all platforms
make release        # Create release archives
```

### Make targets

- `make build` - Build static binary
- `make build-upx` - Build and compress with UPX
- `make install` - Build, compress, and install to system PATH
- `make uninstall` - Remove from system PATH
- `make test` - Run tests
- `make clean` - Clean build artifacts
- `make build-all` - Build for Linux, macOS, and Windows
- `make release` - Create release archives for all platforms

## Testing

```bash
make test
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## About

Created by [Syntropiq](https://syntropiq.com) - Building tools for data professionals.

---

**parqat** - Because sometimes you need to cat your parquet files! 🐱📊
