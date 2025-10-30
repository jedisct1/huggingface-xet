// Cross-verification tests against the Rust reference implementation
// Test vectors extracted from xet-core repository

const std = @import("std");
const testing = std.testing;
const chunking = @import("chunking.zig");
const hashing = @import("hashing.zig");
const constants = @import("constants.zig");

// SplitMix64 RNG for deterministic test data generation
// This matches the Rust implementation's test data generation
fn splitmix64Next(state: *u64) u64 {
    state.* = state.* +% 0x9E3779B97F4A7C15;
    var z = state.*;
    z = (z ^ (z >> 30)) *% 0xBF58476D1CE4E5B9;
    z = (z ^ (z >> 27)) *% 0x94D049BB133111EB;
    return z ^ (z >> 31);
}

fn createRandomData(allocator: std.mem.Allocator, n: usize, seed: u64) ![]u8 {
    var ret = try allocator.alloc(u8, n);
    var state = seed;

    var pos: usize = 0;
    while (pos < n) {
        const next_u64 = splitmix64Next(&state);
        const bytes = std.mem.asBytes(&next_u64);
        const to_copy = @min(8, n - pos);
        @memcpy(ret[pos..][0..to_copy], bytes[0..to_copy]);
        pos += to_copy;
    }

    return ret;
}

// Test: Verify that our random data generation matches Rust's
test "random data generation matches Rust" {
    const allocator = testing.allocator;

    const data = try createRandomData(allocator, 1000000, 0);
    defer allocator.free(data);

    // Verify specific bytes that the Rust implementation checks
    try testing.expectEqual(@as(u8, 175), data[0]);
    try testing.expectEqual(@as(u8, 132), data[127]);
    try testing.expectEqual(@as(u8, 118), data[111111]);
}

// Test: Chunking 1MB of random data with seed 0
// Expected chunk boundaries from Rust implementation
test "chunking 1MB random data (seed 0)" {
    const allocator = testing.allocator;

    const data = try createRandomData(allocator, 1000000, 0);
    defer allocator.free(data);

    var chunk_boundaries = try chunking.chunkBuffer(allocator, data);
    defer chunk_boundaries.deinit(allocator);

    // Convert chunk boundaries to cumulative offsets
    var boundaries: std.ArrayList(usize) = .empty;
    defer boundaries.deinit(allocator);

    for (chunk_boundaries.items) |boundary| {
        try boundaries.append(allocator, boundary.start + boundary.size());
    }

    // Expected boundaries from Rust test_correctness_1mb_random_data
    const expected = [_]usize{ 84493, 134421, 144853, 243318, 271793, 336457, 467529, 494581, 582000, 596735, 616815, 653164, 678202, 724510, 815591, 827760, 958832, 991092, 1000000 };

    try testing.expectEqual(expected.len, boundaries.items.len);
    for (expected, 0..) |exp, i| {
        try testing.expectEqual(exp, boundaries.items[i]);
    }
}

// Test: Chunking 1MB of constant data (all bytes = 59)
test "chunking 1MB const data (value 59)" {
    const allocator = testing.allocator;

    const data = try allocator.alloc(u8, 1000000);
    defer allocator.free(data);
    @memset(data, 59);

    var chunk_boundaries = try chunking.chunkBuffer(allocator, data);
    defer chunk_boundaries.deinit(allocator);

    // Convert chunk boundaries to cumulative offsets
    var boundaries: std.ArrayList(usize) = .empty;
    defer boundaries.deinit(allocator);

    for (chunk_boundaries.items) |boundary| {
        try boundaries.append(allocator, boundary.start + boundary.size());
    }

    // Expected boundaries from Rust test_correctness_1mb_const_data
    // For constant data, chunker hits maximum chunk size boundaries
    const expected = [_]usize{ 131072, 262144, 393216, 524288, 655360, 786432, 917504, 1000000 };

    try testing.expectEqual(expected.len, boundaries.items.len);
    for (expected, 0..) |exp, i| {
        try testing.expectEqual(exp, boundaries.items[i]);
    }
}

// Test: BLAKE3 data hash
test "BLAKE3 data hash" {
    // Test simple data
    const data1 = "hello world";
    const hash1 = hashing.computeDataHash(data1);

    // Verify it's deterministic
    const hash1_again = hashing.computeDataHash(data1);
    try testing.expectEqualSlices(u8, &hash1, &hash1_again);

    // Different data should produce different hash
    const data2 = "goodbye world";
    const hash2 = hashing.computeDataHash(data2);
    try testing.expect(!std.mem.eql(u8, &hash1, &hash2));
}

// Test: Merkle tree format
test "merkle tree internal node format" {
    const allocator = testing.allocator;

    // Create some fake chunk hashes
    const chunk1_hash = hashing.computeDataHash("chunk1");
    const chunk2_hash = hashing.computeDataHash("chunk2");

    var chunks = std.ArrayList(hashing.MerkleNode).empty;
    defer chunks.deinit(allocator);

    try chunks.append(allocator, .{ .hash = chunk1_hash, .size = 100 });
    try chunks.append(allocator, .{ .hash = chunk2_hash, .size = 200 });

    // Build Merkle tree (compute root hash from chunks)
    const merkle_root = try hashing.buildMerkleTree(allocator, chunks.items);

    // Verify it's deterministic
    const merkle_root_again = try hashing.buildMerkleTree(allocator, chunks.items);
    try testing.expectEqualSlices(u8, &merkle_root, &merkle_root_again);
}

// Test: File hash with zero salt
test "file hash computation" {
    const allocator = testing.allocator;

    // Create some fake chunk hashes
    const chunk1_hash = hashing.computeDataHash("chunk1");
    const chunk2_hash = hashing.computeDataHash("chunk2");

    var chunks = std.ArrayList(hashing.MerkleNode).empty;
    defer chunks.deinit(allocator);

    try chunks.append(allocator, .{ .hash = chunk1_hash, .size = 100 });
    try chunks.append(allocator, .{ .hash = chunk2_hash, .size = 200 });

    // Build Merkle tree and compute file hash
    const merkle_root = try hashing.buildMerkleTree(allocator, chunks.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    // Verify it's deterministic
    const merkle_root_again = try hashing.buildMerkleTree(allocator, chunks.items);
    const file_hash_again = hashing.computeFileHash(merkle_root_again);
    try testing.expectEqualSlices(u8, &file_hash, &file_hash_again);
}

// Test: Merkle tree (xorb hash) against Rust reference
test "merkle tree with single zero chunk" {
    const allocator = testing.allocator;

    // Test case: Single chunk with all zeros
    var chunks = std.ArrayList(hashing.MerkleNode).empty;
    defer chunks.deinit(allocator);

    const zero_hash: [32]u8 = @splat(0);
    try chunks.append(allocator, .{ .hash = zero_hash, .size = 0 });

    const merkle_root = try hashing.buildMerkleTree(allocator, chunks.items);

    // For a single chunk, the merkle root should be the chunk hash itself
    try testing.expectEqualSlices(u8, &zero_hash, &merkle_root);
}

// Test: Constants match Rust implementation
test "constants match Rust implementation" {
    // Verify BLAKE3 keys
    const expected_data_key = [_]u8{
        102, 151, 245, 119, 91,  149, 80, 222, 49,  53,  203, 172, 165, 151, 24,  28,
        157, 228, 33,  16,  155, 235, 43, 88,  180, 208, 176, 75,  147, 173, 242, 41,
    };
    try testing.expectEqualSlices(u8, &expected_data_key, &constants.DataKey);

    const expected_internal_key = [_]u8{
        1,  126, 197, 199, 165, 71,  41,  150, 253, 148, 102, 102, 180, 138, 2,   230,
        93, 221, 83,  111, 55,  199, 109, 210, 248, 99,  82,  230, 74,  83,  113, 63,
    };
    try testing.expectEqualSlices(u8, &expected_internal_key, &constants.InternalNodeKey);

    // Verify chunking constants
    try testing.expectEqual(@as(usize, 64 * 1024), constants.TargetChunkSize);
    try testing.expectEqual(@as(usize, 8 * 1024), constants.MinChunkSize);
    try testing.expectEqual(@as(usize, 128 * 1024), constants.MaxChunkSize);
    try testing.expectEqual(@as(u64, 0xFFFF000000000000), constants.GearHashMask);
}

// Test: Gearhash TABLE constant (verify it's non-zero and consistent)
test "gearhash TABLE constant" {
    // Verify the TABLE has 256 entries and they're non-trivial
    try testing.expectEqual(@as(usize, 256), constants.GearHashTable.len);

    // Verify first few entries are the actual values from our TABLE
    // (from rust-gearhash DEFAULT_TABLE)
    const expected_first_10 = [_]u64{
        0xb088d3a9e840f559,
        0x5652c7f739ed20d6,
        0x45b28969898972ab,
        0x6b0a89d5b68ec777,
        0x368f573e8b7a31b7,
        0x1dc636dce936d94b,
        0x207a4c4e5554d5b6,
        0xa474b34628239acb,
        0x3b06a83e1ca3b912,
        0x90e78d6c2f02baf7,
    };

    for (expected_first_10, 0..) |expected, i| {
        try testing.expectEqual(expected, constants.GearHashTable[i]);
    }
}

// End-to-end test: Chunk, hash, and verify a simple file
test "end-to-end: chunk and hash small file" {
    const allocator = testing.allocator;

    // Create test data - 200KB of predictable data
    const data_size = 200 * 1024;
    const data = try allocator.alloc(u8, data_size);
    defer allocator.free(data);

    for (data, 0..) |*byte, i| {
        byte.* = @as(u8, @truncate(i));
    }

    // Chunk the data
    var chunk_boundaries = try chunking.chunkBuffer(allocator, data);
    defer chunk_boundaries.deinit(allocator);

    // Hash each chunk
    var chunk_hashes = std.ArrayList(hashing.MerkleNode).empty;
    defer chunk_hashes.deinit(allocator);

    for (chunk_boundaries.items) |boundary| {
        const start = boundary.start;
        const len = boundary.size();
        const chunk_data = data[start..][0..len];
        const hash = hashing.computeDataHash(chunk_data);
        try chunk_hashes.append(allocator, .{ .hash = hash, .size = @as(u64, @intCast(len)) });
    }

    // Compute file hash (Merkle tree root + file hash transform)
    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_hashes.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    // Verify determinism - re-chunk and re-hash should produce same result
    var chunk_boundaries2 = try chunking.chunkBuffer(allocator, data);
    defer chunk_boundaries2.deinit(allocator);

    var chunk_hashes2 = std.ArrayList(hashing.MerkleNode).empty;
    defer chunk_hashes2.deinit(allocator);

    for (chunk_boundaries2.items) |boundary| {
        const start = boundary.start;
        const len = boundary.size();
        const chunk_data = data[start..][0..len];
        const hash = hashing.computeDataHash(chunk_data);
        try chunk_hashes2.append(allocator, .{ .hash = hash, .size = @as(u64, @intCast(len)) });
    }

    const merkle_root2 = try hashing.buildMerkleTree(allocator, chunk_hashes2.items);
    const file_hash2 = hashing.computeFileHash(merkle_root2);

    // File hashes should be identical
    try testing.expectEqualSlices(u8, &file_hash, &file_hash2);

    // Number of chunks should be identical
    try testing.expectEqual(chunk_boundaries.items.len, chunk_boundaries2.items.len);

    // Each chunk hash should be identical
    try testing.expectEqual(chunk_hashes.items.len, chunk_hashes2.items.len);
    for (chunk_hashes.items, chunk_hashes2.items) |h1, h2| {
        try testing.expectEqualSlices(u8, &h1.hash, &h2.hash);
        try testing.expectEqual(h1.size, h2.size);
    }
}
