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

    const n = data.len;
    const split = n / 4;
    const rem = n % 4;

    // Calculate group sizes based on remainder
    const g0_size = split + @min(1, rem);
    const g1_size = split + @min(1, if (rem >= 1) rem - 1 else 0);
    const g2_size = split + @min(1, if (rem >= 2) rem - 2 else 0);

    // Group offsets in output buffer
    const g1_offset = g0_size;
    const g2_offset = g1_offset + g1_size;
    const g3_offset = g2_offset + g2_size;

    // Fill the full groups
    for (0..split) |i| {
        result[i] = data[4 * i];
        result[g1_offset + i] = data[4 * i + 1];
        result[g2_offset + i] = data[4 * i + 2];
        result[g3_offset + i] = data[4 * i + 3];
    }

    // Handle remainder bytes
    switch (rem) {
        1 => {
            result[split] = data[4 * split];
        },
        2 => {
            result[split] = data[4 * split];
            result[g1_offset + split] = data[4 * split + 1];
        },
        3 => {
            result[split] = data[4 * split];
            result[g1_offset + split] = data[4 * split + 1];
            result[g2_offset + split] = data[4 * split + 2];
        },
        else => {},
    }

    return result;
}

pub fn reverseByteGrouping(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const n = data.len;
    const split = n / 4;
    const rem = n % 4;

    // Calculate group sizes based on remainder
    const g0_size = split + @min(1, rem);
    const g1_size = split + @min(1, if (rem >= 1) rem - 1 else 0);
    const g2_size = split + @min(1, if (rem >= 2) rem - 2 else 0);

    // Group offsets in input buffer
    const g1_offset = g0_size;
    const g2_offset = g1_offset + g1_size;
    const g3_offset = g2_offset + g2_size;

    // Regroup the full groups
    for (0..split) |i| {
        result[4 * i] = data[i];
        result[4 * i + 1] = data[g1_offset + i];
        result[4 * i + 2] = data[g2_offset + i];
        result[4 * i + 3] = data[g3_offset + i];
    }

    // Handle remainder bytes
    switch (rem) {
        1 => {
            result[4 * split] = data[split];
        },
        2 => {
            result[4 * split] = data[split];
            result[4 * split + 1] = data[g1_offset + split];
        },
        3 => {
            result[4 * split] = data[split];
            result[4 * split + 1] = data[g1_offset + split];
            result[4 * split + 2] = data[g2_offset + split];
        },
        else => {},
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
    // Input: [1,2,3,4,5,6,7,8,9,10]
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=2, rem=2
    // g0: [1,5,9] (size=3)
    // g1: [2,6,10] (size=3)
    // g2: [3,7] (size=2)
    // g3: [4,8] (size=2)
    // Concatenated: [1,5,9,2,6,10,3,7,4,8]
    const expected = [_]u8{ 1, 5, 9, 2, 6, 10, 3, 7, 4, 8 };
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

test "byte grouping with remainder 1" {
    const allocator = std.testing.allocator;
    // 13 bytes = 3 full groups (12 bytes) + 1 remaining
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=3, rem=1
    // g0: [0,4,8,12] (size=4)
    // g1: [1,5,9] (size=3)
    // g2: [2,6,10] (size=3)
    // g3: [3,7,11] (size=3)
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 2, 6, 10, 3, 7, 11 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "byte grouping with remainder 3" {
    const allocator = std.testing.allocator;
    // 15 bytes = 3 full groups (12 bytes) + 3 remaining
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=3, rem=3
    // g0: [0,4,8,12] (size=4)
    // g1: [1,5,9,13] (size=4)
    // g2: [2,6,10,14] (size=4)
    // g3: [3,7,11] (size=3)
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "byte grouping large data round trip" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    // Test with different sizes matching Rust test suite
    const sizes = [_]usize{
        64 * 1024, // 65536 - aligned
        64 * 1024 - 53, // 65483 - rem=3
        64 * 1024 + 135, // 65671 - rem=3
        1000, // Small size
        1, // Single byte
        7, // rem=3
    };

    for (sizes) |size| {
        const data = try allocator.alloc(u8, size);
        defer allocator.free(data);
        random.bytes(data);

        const grouped = try applyByteGrouping(allocator, data);
        defer allocator.free(grouped);

        const ungrouped = try reverseByteGrouping(allocator, grouped);
        defer allocator.free(ungrouped);

        try std.testing.expectEqualSlices(u8, data, ungrouped);
    }
}

test "ByteGrouping4LZ4 with model-like data" {
    const allocator = std.testing.allocator;
    // Simulate float32 data (4 bytes per float) with patterns
    // This is similar to what model weights look like
    var data = try allocator.alloc(u8, 1024);
    defer allocator.free(data);

    // Fill with pattern: little-endian float-like values
    for (0..256) |i| {
        const idx = i * 4;
        data[idx] = @truncate(i);
        data[idx + 1] = 0;
        data[idx + 2] = 0;
        data[idx + 3] = 0x3F; // Sign/exponent bits
    }

    // Compress with regular LZ4
    const lz4_result = try compress(allocator, data, .LZ4);
    defer allocator.free(lz4_result.data);

    // Compress with ByteGrouping4LZ4
    const bg4_result = try compress(allocator, data, .ByteGrouping4LZ4);
    defer allocator.free(bg4_result.data);

    // ByteGrouping4LZ4 should compress better for this pattern
    try std.testing.expect(bg4_result.data.len < lz4_result.data.len);

    // Verify decompression works correctly
    const decompressed = try decompress(
        allocator,
        bg4_result.data,
        bg4_result.type,
        data.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}
