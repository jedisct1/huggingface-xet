//! XET Protocol Hashing - BLAKE3 with domain separation keys

const std = @import("std");
const constants = @import("constants.zig");

pub const Hash = [constants.HashSize]u8;

inline fn computeHashWithKey(key: [32]u8, data: []const u8) Hash {
    var hasher = std.crypto.hash.Blake3.init(.{ .key = key });
    hasher.update(data);
    var hash: Hash = undefined;
    hasher.final(&hash);
    return hash;
}

pub fn computeDataHash(data: []const u8) Hash {
    return computeHashWithKey(constants.DataKey, data);
}

pub fn computeInternalNodeHash(data: []const u8) Hash {
    return computeHashWithKey(constants.InternalNodeKey, data);
}

pub fn computeFileHash(merkle_root: Hash) Hash {
    return computeHashWithKey(constants.FileHashKey, &merkle_root);
}

pub fn computeVerificationHash(data: []const u8) Hash {
    return computeHashWithKey(constants.VerificationKey, data);
}

pub fn hmac(key: [32]u8, message: []const u8) Hash {
    return computeHashWithKey(key, message);
}

pub fn hashToHex(hash: Hash) [64]u8 {
    return std.fmt.bytesToHex(&hash, .lower);
}

pub fn hexToHash(hex: []const u8) !Hash {
    if (hex.len != 64) return error.InvalidHexLength;
    var hash: Hash = undefined;
    _ = try std.fmt.hexToBytes(&hash, hex);
    return hash;
}

pub const MerkleNode = struct {
    hash: Hash,
    size: usize,
};

pub fn buildMerkleTree(allocator: std.mem.Allocator, chunks: []const MerkleNode) !Hash {
    if (chunks.len == 0) {
        return error.EmptyChunkList;
    }

    if (chunks.len == 1) return chunks[0].hash;

    var buffer: std.ArrayList(u8) = .empty;
    defer buffer.deinit(allocator);

    for (chunks) |chunk| {
        const hex = hashToHex(chunk.hash);
        const line = try std.fmt.allocPrint(allocator, "{s} : {d}\n", .{ hex, chunk.size });
        defer allocator.free(line);
        try buffer.appendSlice(allocator, line);
    }

    return computeInternalNodeHash(buffer.items);
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
