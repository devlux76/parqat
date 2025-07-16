# Go parameters
BINARY_NAME=parqat
VERSION?=$(shell git describe --tags --always --dirty)
COMPANY?=syntropiq
BUILD_DIR=build

# Build flags
LDFLAGS_RELEASE=-ldflags="-s -w -X main.version=$(VERSION) -X main.company=$(COMPANY)"
LDFLAGS_DEBUG=-ldflags="-X main.version=$(VERSION) -X main.company=$(COMPANY)"
BUILD_TAGS=netgo

.PHONY: all release debug test clean install

all: release

# Builds the release binary, statically compiled and packed with upx.
release:
	@echo "Building release version $(VERSION)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build -a -tags $(BUILD_TAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(LDFLAGS_RELEASE) .
	@echo "Packing with upx..."
	@upx --best --lzma $(BUILD_DIR)/$(BINARY_NAME)

# Builds the debug binary with symbols, statically compiled.
debug:
	@echo "Building debug version $(VERSION)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build -a -tags $(BUILD_TAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-debug $(LDFLAGS_DEBUG) .

# Runs all tests.
test:
	@echo "Running tests..."
	@go test -v ./...

# Removes build artifacts.
clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)

# Installs the release binary.
install: release
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	@sudo cp $(BUILD_DIR)/$(BINARY_NAME) /usr/local/bin/$(BINARY_NAME)
