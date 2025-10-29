//! XET Protocol Compression: None, LZ4, ByteGrouping4LZ4

const std = @import("std");
const constants = @import("constants.zig");
const lz4 = @import("lz4");

pub const CompressionError = error{
    CompressionFailed,
    DecompressionFailed,
    InvalidCompressionType,
    BufferTooSmall,
    InvalidUncompressedSize,
};

pub const CompressionResult = struct {
    data: []u8,
    type: constants.CompressionType,
};

fn lz4FrameCompress(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    // Calculate maximum output size
    const max_size = lz4.lz4f.compressFrameBound(data.len, null);
    const compressed_buffer = try allocator.alloc(u8, max_size);
    errdefer allocator.free(compressed_buffer);

    // Compress frame
    const compressed_size = lz4.lz4f.compressFrame(
        allocator,
        data,
        compressed_buffer,
        null,
    ) catch |err| {
        return switch (err) {
            error.DstMaxSizeTooSmall => error.CompressionFailed,
            else => error.CompressionFailed,
        };
    };

    // Resize to actual size
    const result = try allocator.realloc(compressed_buffer, compressed_size);
    return result;
}

fn compressLZ4(
    allocator: std.mem.Allocator,
    data: []const u8,
    original_size: usize,
    compression_type: constants.CompressionType,
) !CompressionResult {
    const compressed_data = try lz4FrameCompress(allocator, data);
    const final_size = compressed_data.len;

    if (final_size >= original_size) {
        allocator.free(compressed_data);
        const result = try allocator.dupe(u8, data);
        return .{ .data = result, .type = .None };
    }

    return .{ .data = compressed_data, .type = compression_type };
}

pub fn compress(
    allocator: std.mem.Allocator,
    data: []const u8,
    compression_type: constants.CompressionType,
) !CompressionResult {
    switch (compression_type) {
        .None => {
            const result = try allocator.dupe(u8, data);
            return .{ .data = result, .type = .None };
        },
        .LZ4 => {
            return compressLZ4(allocator, data, data.len, .LZ4);
        },
        .ByteGrouping4LZ4 => {
            const grouped = try applyByteGrouping(allocator, data);
            defer allocator.free(grouped);
            return compressLZ4(allocator, grouped, data.len, .ByteGrouping4LZ4);
        },
    }
}

fn lz4FrameDecompress(allocator: std.mem.Allocator, data: []const u8, expected_size: usize) ![]u8 {
    // Allocate output buffer based on expected size
    const decompressed_buffer = try allocator.alloc(u8, expected_size);
    errdefer allocator.free(decompressed_buffer);

    // Decompress frame
    const decompressed_size = lz4.lz4f.decompressFrame(
        allocator,
        data,
        decompressed_buffer,
    ) catch |err| {
        return switch (err) {
            else => error.DecompressionFailed,
        };
    };

    // Verify size matches expected
    if (decompressed_size != expected_size) {
        allocator.free(decompressed_buffer);
        return error.InvalidUncompressedSize;
    }

    return decompressed_buffer;
}

pub fn decompress(
    allocator: std.mem.Allocator,
    data: []const u8,
    compression_type: constants.CompressionType,
    uncompressed_size: usize,
) ![]u8 {
    switch (compression_type) {
        .None => {
            if (data.len != uncompressed_size) {
                return error.InvalidUncompressedSize;
            }
            return try allocator.dupe(u8, data);
        },
        .LZ4 => {
            return try lz4FrameDecompress(allocator, data, uncompressed_size);
        },
        .ByteGrouping4LZ4 => {
            const lz4_decompressed = try lz4FrameDecompress(allocator, data, uncompressed_size);
            defer allocator.free(lz4_decompressed);
            return try reverseByteGrouping(allocator, lz4_decompressed);
        },
    }
}

pub fn applyByteGrouping(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const full_groups = data.len / 4;
    const remaining = data.len % 4;

    var group_idx: usize = 0;
    while (group_idx < full_groups) : (group_idx += 1) {
        const base_in = group_idx * 4;
        for (0..4) |byte_pos| {
            const out_idx = byte_pos * full_groups + group_idx;
            result[out_idx] = data[base_in + byte_pos];
        }
    }

    for (0..remaining) |i| {
        const in_idx = full_groups * 4 + i;
        const out_idx = full_groups * 4 + i;
        result[out_idx] = data[in_idx];
    }

    return result;
}

pub fn reverseByteGrouping(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const full_groups = data.len / 4;
    const remaining = data.len % 4;

    var group_idx: usize = 0;
    while (group_idx < full_groups) : (group_idx += 1) {
        for (0..4) |byte_pos| {
            const in_idx = byte_pos * full_groups + group_idx;
            const out_idx = group_idx * 4 + byte_pos;
            result[out_idx] = data[in_idx];
        }
    }

    for (0..remaining) |i| {
        const in_idx = full_groups * 4 + i;
        const out_idx = full_groups * 4 + i;
        result[out_idx] = data[in_idx];
    }

    return result;
}

test "compress with no compression" {
    const allocator = std.testing.allocator;
    const data = "Hello, World!";

    const result = try compress(allocator, data, .None);
    defer allocator.free(result.data);

    try std.testing.expectEqualSlices(u8, data, result.data);
    try std.testing.expectEqual(constants.CompressionType.None, result.type);
}

test "decompress with no compression" {
    const allocator = std.testing.allocator;
    const data = "Hello, World!";

    const result = try decompress(allocator, data, .None, data.len);
    defer allocator.free(result);

    try std.testing.expectEqualSlices(u8, data, result);
}

test "byte grouping with 4-byte aligned data" {
    const allocator = std.testing.allocator;
    // Input: A1 A2 A3 A4 B1 B2 B3 B4
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // Expected: A1 B1 A2 B2 A3 B3 A4 B4
    const expected = [_]u8{ 1, 5, 2, 6, 3, 7, 4, 8 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);
}

test "byte grouping with non-aligned data" {
    const allocator = std.testing.allocator;
    // 10 bytes = 2 full groups (8 bytes) + 2 remaining
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // First 8 bytes get grouped, last 2 stay at end
    const expected = [_]u8{ 1, 5, 2, 6, 3, 7, 4, 8, 9, 10 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);
}

test "byte grouping round trip" {
    const allocator = std.testing.allocator;
    const original = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const grouped = try applyByteGrouping(allocator, &original);
    defer allocator.free(grouped);

    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);

    try std.testing.expectEqualSlices(u8, &original, ungrouped);
}

test "byte grouping with single byte" {
    const allocator = std.testing.allocator;
    const data = [_]u8{42};

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &data, grouped);
}

test "LZ4 compression and decompression" {
    const allocator = std.testing.allocator;
    const base_str = "Hello, World! This is a test of LZ4 compression. ";
    const original = base_str ++ base_str ++ base_str ++ base_str ++ base_str ++
        base_str ++ base_str ++ base_str ++ base_str ++ base_str;

    // Compress
    const compressed_result = try compress(allocator, original, .LZ4);
    defer allocator.free(compressed_result.data);

    // Verify compression reduced size
    try std.testing.expect(compressed_result.data.len < original.len);
    try std.testing.expectEqual(constants.CompressionType.LZ4, compressed_result.type);

    // Decompress
    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        original.len,
    );
    defer allocator.free(decompressed);

    // Verify round trip
    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "LZ4 with incompressible data" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    // Generate random data (hard to compress)
    var data: [100]u8 = undefined;
    random.bytes(&data);

    // Try to compress
    const result = try compress(allocator, &data, .LZ4);
    defer allocator.free(result.data);

    // Should fall back to no compression if it doesn't reduce size
    // (may or may not compress, depends on random data)
}

test "ByteGrouping4LZ4 compression and decompression" {
    const allocator = std.testing.allocator;
    // Create data with patterns that benefit from byte grouping
    const base_pattern = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 };
    const pattern = base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern;

    // Compress
    const compressed_result = try compress(allocator, &pattern, .ByteGrouping4LZ4);
    defer allocator.free(compressed_result.data);

    // ByteGrouping should improve compression for this pattern
    try std.testing.expect(compressed_result.data.len < pattern.len);
    try std.testing.expectEqual(constants.CompressionType.ByteGrouping4LZ4, compressed_result.type);

    // Decompress
    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        pattern.len,
    );
    defer allocator.free(decompressed);

    // Verify round trip
    try std.testing.expectEqualSlices(u8, &pattern, decompressed);
}

test "LZ4 compression with small data" {
    const allocator = std.testing.allocator;
    const original = "Hi";

    // Compress
    const compressed_result = try compress(allocator, original, .LZ4);
    defer allocator.free(compressed_result.data);

    // Small data might not compress well, but should still work
    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        original.len,
    );
    defer allocator.free(decompressed);

    // Verify round trip works
    try std.testing.expectEqualSlices(u8, original, decompressed);
}
