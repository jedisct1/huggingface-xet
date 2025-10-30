# zig-xet

<p align="center">
  <img src=".media/logo.jpg" />
</p>

A pure Zig implementation of the XET protocol for efficient file storage and retrieval through content-defined chunking and deduplication.

## What is this?

XET is a protocol for handling large files by breaking them into chunks based on their content (not fixed sizes), compressing them, and storing them in a way that eliminates duplicates.

It's particularly useful for managing large models and datasets, like those hosted on HuggingFace.

This library implements the full XET protocol spec in Zig, including:

- Content-defined chunking using the Gearhash algorithm (chunks are between 8KB-128KB)
- LZ4 compression with byte grouping optimization
- Merkle tree construction for efficient file verification
- Xorb format for serializing chunked data
- MDB shard format for metadata storage
- CAS client for downloading files from HuggingFace

The implementation has been cross-verified against the Rust reference implementation to ensure correctness.

## Quick start

### Requirements

- Zig 0.16 or newer
- A HuggingFace token (for downloading models)

### Build and test

```bash
# Build the project
zig build

# Run tests (62 tests covering all components)
zig build test

# Run the demo CLI
zig build run

# Run benchmarks
zig build bench
```

### Downloading a model from HuggingFace

The most common use case is downloading models efficiently:

```bash
# Set your HuggingFace token
export HF_TOKEN="your_token_here"

# Run the download example
zig build run-example-download
```

This downloads a model using the XET protocol, which handles all the chunking, deduplication, and reconstruction automatically.

### Using as a library

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .xet = .{
        .url = "https://github.com/yourusername/zig-xet/archive/main.tar.gz",
    },
},
```

Then in your code:

```zig
const xet = @import("xet");

// Chunk a file using content-defined chunking
var chunks = try xet.chunking.chunkBuffer(allocator, data);
defer chunks.deinit(allocator);

// Hash chunks with BLAKE3
const hash = xet.hashing.computeDataHash(chunk_data);

// Build a Merkle tree for verification
const merkle_root = try xet.hashing.buildMerkleTree(allocator, &nodes);

// Download a model from HuggingFace
const config = xet.model_download.DownloadConfig{
    .repo_id = "org/model",
    .repo_type = "model",
    .revision = "main",
    .file_hash_hex = "...",
};
try xet.model_download.downloadModelToFile(allocator, config, "output.gguf");
```

## How it works

The XET protocol processes files in several stages:

1. Chunking: Files are split using a rolling hash algorithm. Instead of fixed-size chunks, boundaries are determined by content patterns, which means similar files share many identical chunks.

2. Hashing: Each chunk gets a BLAKE3 hash. A Merkle tree combines these hashes to create a single file identifier.

3. Compression: Chunks are compressed with LZ4, optionally with byte grouping preprocessing for better ratios.

4. Deduplication: Identical chunks (same hash) are stored only once, saving space when you have multiple similar files.

5. Storage: Chunks are bundled into "xorbs" and metadata is stored in "MDB shards" for efficient retrieval.

When downloading from HuggingFace, the library queries the CAS (content-addressable storage) API to find which chunks are needed, fetches them, decompresses, and reconstructs the original file.

## Protocol compliance

This implementation follows the official XET protocol specification exactly.

All constants, algorithms, and formats match the reference Rust implementation byte-for-byte. The test suite includes cross-verification tests to ensure continued compatibility.

## Getting a HuggingFace token

1. Go to https://huggingface.co/settings/tokens
2. Create a new token with "Read access to contents of all public gated repos you can access"
3. Copy the token and set it as `HF_TOKEN` environment variable

## Links

- [XET Protocol Documentation](https://huggingface.co/docs/xet/index)
