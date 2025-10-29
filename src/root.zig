//! XET Protocol Implementation in Zig
//!
//! This library implements the XET protocol for content-addressed data handling
//! through chunking, hashing, and deduplication.
//!
//! Main modules:
//! - constants: Protocol constants (Gearhash TABLE, BLAKE3 keys, etc.)
//! - chunking: Gearhash content-defined chunking (CDC)
//! - hashing: BLAKE3 hashing with 4 types + Merkle trees
//! - compression: LZ4 and ByteGrouping4LZ4 compression
//! - xorb: Xorb format serialization/deserialization
//! - shard: MDB shard format I/O
//! - cas_client: HTTP CAS API client
//! - reconstruction: File reconstruction from terms
//! - model_download: High-level API for downloading models from Hugging Face

const std = @import("std");

// Export all public modules
pub const constants = @import("constants.zig");
pub const chunking = @import("chunking.zig");
pub const hashing = @import("hashing.zig");
pub const compression = @import("compression.zig");
pub const xorb = @import("xorb.zig");
pub const shard = @import("shard.zig");
pub const cas_client = @import("cas_client.zig");
pub const reconstruction = @import("reconstruction.zig");
pub const model_download = @import("model_download.zig");
pub const benchmark = @import("benchmark.zig");

test {
    std.testing.refAllDecls(@This());
    _ = @import("verification_test.zig");
}
