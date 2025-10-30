//! XET Protocol Hashing - BLAKE3 with domain separation keys

const std = @import("std");
const constants = @import("constants.zig");

pub const Hash = [constants.HashSize]u8;

pub fn computeDataHash(data: []const u8) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(data, &hash, .{ .key = constants.DataKey });
    return hash;
}

pub fn computeInternalNodeHash(data: []const u8) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(data, &hash, .{ .key = constants.InternalNodeKey });
    return hash;
}

pub fn computeFileHash(merkle_root: Hash) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(&merkle_root, &hash, .{ .key = constants.FileHashKey });
    return hash;
}

pub fn computeVerificationHash(data: []const u8) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(data, &hash, .{ .key = constants.VerificationKey });
    return hash;
}

pub fn hmac(key: [32]u8, message: []const u8) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(message, &hash, .{ .key = key });
    return hash;
}

pub fn hashToHex(hash: Hash) [64]u8 {
    // Rust formats hashes as [u64; 4] with each u64 in little-endian format
    // Read each 8-byte chunk as a little-endian u64, then format as hex
    var result: [64]u8 = undefined;
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const offset = i * 8;
        const val = std.mem.readInt(u64, hash[offset..][0..8], .little);
        // Format the u64 value as 16 hex digits (lowercase)
        const formatted = std.fmt.bufPrint(result[i * 16 ..][0..16], "{x:0>16}", .{val}) catch unreachable;
        _ = formatted;
    }
    return result;
}

pub fn hexToHash(hex: []const u8) !Hash {
    if (hex.len != 64) return error.InvalidHexLength;
    // Rust formats hashes as [u64; 4] with each u64 in little-endian format
    // Parse each 16-char hex string as a u64, then write as little-endian bytes
    var hash: Hash = undefined;
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const hex_chunk = hex[i * 16 ..][0..16];
        const val = try std.fmt.parseInt(u64, hex_chunk, 16);
        std.mem.writeInt(u64, hash[i * 8 ..][0..8], val, .little);
    }
    return hash;
}

pub const MerkleNode = struct {
    hash: Hash,
    size: u64,
};

// Mean branching factor for the merkle tree
const MeanTreeBranchingFactor: u64 = 4;

/// Find the next cut point in a sequence of hashes at which to break.
///
/// This implements the variable branching logic from the Rust reference:
/// - Each parent must have at least 2 children and at most 2*MEAN_TREE_BRANCHING_FACTOR children
/// - Split when hash % MEAN_TREE_BRANCHING_FACTOR == 0 (on average, every 4 nodes)
/// - This ensures the tree has O(log n) height with controlled branching
fn nextMergeCut(nodes: []const MerkleNode) usize {
    if (nodes.len <= 2) {
        return nodes.len;
    }

    const end = @min(2 * MeanTreeBranchingFactor + 1, nodes.len);

    var i: usize = 2;
    while (i < end) : (i += 1) {
        // Convert hash bytes to u64 for modulo check
        // Rust uses self[3] which is the last u64 (bytes 24-31) of the [u64; 4] array
        const hash_as_u64 = std.mem.readInt(u64, nodes[i].hash[24..32], .little);

        if (hash_as_u64 % MeanTreeBranchingFactor == 0) {
            return i + 1;
        }
    }

    return end;
}

/// Merge a sequence of nodes into a single node by concatenating their representations
/// and hashing with the InternalNodeKey.
///
/// Returns a new MerkleNode with the merged hash and sum of all sizes.
fn mergedHashOfSequence(allocator: std.mem.Allocator, nodes: []const MerkleNode) !MerkleNode {
    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    var total_size: u64 = 0;

    for (nodes) |node| {
        const hex = hashToHex(node.hash);
        const line = try std.fmt.allocPrint(allocator, "{s} : {d}\n", .{ hex, node.size });
        defer allocator.free(line);
        try buffer.appendSlice(allocator, line);
        total_size += node.size;
    }

    const merged_hash = computeInternalNodeHash(buffer.items);
    return MerkleNode{ .hash = merged_hash, .size = total_size };
}

/// Build a Merkle tree using the XET protocol's aggregated node hash algorithm.
///
/// This implements the tree-based approach with variable branching factor (mean=4)
/// from the Rust reference implementation. The algorithm:
/// 1. Iteratively collapses groups of 2-9 nodes based on hash % 4 == 0
/// 2. Continues until only one node remains (the root)
/// 3. Returns the final hash
///
/// This is CRITICAL for protocol compatibility - must match Rust exactly.
pub fn buildMerkleTree(allocator: std.mem.Allocator, chunks: []const MerkleNode) !Hash {
    if (chunks.len == 0) {
        // Return zero hash for empty input
        return [_]u8{0} ** constants.HashSize;
    }

    if (chunks.len == 1) {
        return chunks[0].hash;
    }

    // Copy chunks to working vector that we'll iteratively collapse
    var hv = try std.ArrayList(MerkleNode).initCapacity(allocator, chunks.len);
    defer hv.deinit(allocator);

    try hv.appendSlice(allocator, chunks);

    // Iteratively collapse until only one node remains
    while (hv.items.len > 1) {
        var write_idx: usize = 0;
        var read_idx: usize = 0;

        while (read_idx < hv.items.len) {
            // Find the next cut point
            const next_cut = read_idx + nextMergeCut(hv.items[read_idx..]);

            // Merge this group of nodes
            const merged = try mergedHashOfSequence(allocator, hv.items[read_idx..next_cut]);

            // Store the merged node at write_idx
            hv.items[write_idx] = merged;
            write_idx += 1;

            // Move to next group
            read_idx = next_cut;
        }

        // Resize to only include the merged nodes
        try hv.resize(allocator, write_idx);
    }

    return hv.items[0].hash;
}

test "data hash is deterministic" {
    const data = "Hello, World!";
    const hash1 = computeDataHash(data);
    const hash2 = computeDataHash(data);
    try std.testing.expectEqualSlices(u8, &hash1, &hash2);
}

test "different data produces different hashes" {
    const data1 = "Hello, World!";
    const data2 = "Hello, Zig!";
    const hash1 = computeDataHash(data1);
    const hash2 = computeDataHash(data2);

    // Hashes should be different
    try std.testing.expect(!std.mem.eql(u8, &hash1, &hash2));
}

test "data hash and internal node hash are different for same input" {
    const data = "Hello, World!";
    const data_hash = computeDataHash(data);
    const internal_hash = computeInternalNodeHash(data);

    // Different keys should produce different hashes
    try std.testing.expect(!std.mem.eql(u8, &data_hash, &internal_hash));
}

test "hash to hex conversion" {
    const data = "test";
    const hash = computeDataHash(data);
    const hex = hashToHex(hash);

    // Should be 64 hex characters (32 bytes)
    try std.testing.expectEqual(@as(usize, 64), hex.len);

    // Should only contain valid hex characters
    for (hex) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "hex to hash round trip" {
    const data = "test";
    const original = computeDataHash(data);
    const hex = hashToHex(original);
    const parsed = try hexToHash(&hex);

    try std.testing.expectEqualSlices(u8, &original, &parsed);
}

test "invalid hex length" {
    const invalid_hex = "123abc"; // Too short
    try std.testing.expectError(error.InvalidHexLength, hexToHash(invalid_hex));
}

test "merkle tree with single chunk" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = computeDataHash("test"), .size = 4 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    try std.testing.expectEqualSlices(u8, &chunks[0].hash, &root);
}

test "merkle tree with multiple chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = computeDataHash("chunk1"), .size = 6 },
        .{ .hash = computeDataHash("chunk2"), .size = 6 },
        .{ .hash = computeDataHash("chunk3"), .size = 6 },
    };

    const root1 = try buildMerkleTree(allocator, &chunks);
    const root2 = try buildMerkleTree(allocator, &chunks);

    // Should be deterministic
    try std.testing.expectEqualSlices(u8, &root1, &root2);

    // Should be different from individual chunk hashes
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[0].hash));
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[1].hash));
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[2].hash));
}

test "file hash is different from merkle root" {
    const merkle_root = computeDataHash("test");
    const file_hash = computeFileHash(merkle_root);

    // File hash applies additional transformation
    try std.testing.expect(!std.mem.eql(u8, &merkle_root, &file_hash));
}

test "hmac produces different output for different keys" {
    const message = "test message";
    const key1: [32]u8 = @splat(0);
    const key2: [32]u8 = @splat(1);

    const hmac1 = hmac(key1, message);
    const hmac2 = hmac(key2, message);

    // Different keys should produce different HMACs
    try std.testing.expect(!std.mem.eql(u8, &hmac1, &hmac2));
}

// Cross-verification tests with Rust reference implementation
// These test vectors are taken from the Rust merklehash crate's test suite
// and verify that our implementation produces identical hashes.

test "merkle tree - empty input" {
    const allocator = std.testing.allocator;
    const chunks: []const MerkleNode = &.{};

    const root = try buildMerkleTree(allocator, chunks);
    const expected = "0000000000000000000000000000000000000000000000000000000000000000";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - single chunk (all zeros)" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("0000000000000000000000000000000000000000000000000000000000000000");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 0 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    // Single chunk returns the chunk hash unchanged
    try std.testing.expectEqualSlices(u8, &hash, &root);
}

test "merkle tree - single chunk (non-zero)" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 100 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - three chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "71ec1275fca074724e2dd666921b3277c7cee603e4d025bcab2d4050015be2bc";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - four identical chunks" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "89f2ada89ff8c96763c6b25010e6dd76a4c05b1466207633ea559acf2093211b";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - six chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
        .{ .hash = try hexToHash("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784"), .size = 400 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "52c826f99507aa05d0b45e9837fa1709e0485425cfbcb1e80db3905cf98b3ee9";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - eight chunks (powers of 2)" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("0000000000000000000000000000000000000000000000000000000000000000"), .size = 0 },
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
        .{ .hash = try hexToHash("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784"), .size = 400 },
        .{ .hash = try hexToHash("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e"), .size = 500 },
        .{ .hash = try hexToHash("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389"), .size = 600 },
        .{ .hash = try hexToHash("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c"), .size = 700 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "f62abe77e3fb9c954fe52b0028027ddc90c064c45951a4fd2211d87e5c0011db";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - 32 identical chunks" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");

    var chunks: [32]MerkleNode = undefined;
    for (&chunks) |*chunk| {
        chunk.* = .{ .hash = hash, .size = 100 };
    }

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "0a0123c1617921883b7e13902095fcb86676e77c49120c33b233003b0af0e0a6";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}
