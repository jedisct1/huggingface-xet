// Cross-verification test for ByteGrouping4LZ4 against Rust reference implementation
// This test verifies that our implementation matches the Rust xet-core byte grouping behavior

const std = @import("std");
const compression = @import("compression.zig");

test "ByteGrouping4LZ4: verify against Rust reference - 10 bytes" {
    const allocator = std.testing.allocator;

    // Test with 10 bytes (split=2, rem=2)
    // Rust bg4_split_together produces: [0,4,8,1,5,9,2,6,3,7]
    // Based on the algorithm:
    // - g0: [0,4,8] (size=3)
    // - g1: [1,5,9] (size=3)
    // - g2: [2,6] (size=2)
    // - g3: [3,7] (size=2)
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    const expected = [_]u8{ 0, 4, 8, 1, 5, 9, 2, 6, 3, 7 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: verify against Rust reference - 13 bytes" {
    const allocator = std.testing.allocator;

    // Test with 13 bytes (split=3, rem=1)
    // - g0: [0,4,8,12] (size=4)
    // - g1: [1,5,9] (size=3)
    // - g2: [2,6,10] (size=3)
    // - g3: [3,7,11] (size=3)
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 2, 6, 10, 3, 7, 11 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: verify against Rust reference - 15 bytes" {
    const allocator = std.testing.allocator;

    // Test with 15 bytes (split=3, rem=3)
    // - g0: [0,4,8,12] (size=4)
    // - g1: [1,5,9,13] (size=4)
    // - g2: [2,6,10,14] (size=4)
    // - g3: [3,7,11] (size=3)
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: verify against Rust reference - 8 bytes aligned" {
    const allocator = std.testing.allocator;

    // Test with 8 bytes (split=2, rem=0) - perfectly aligned
    // - g0: [0,4] (size=2)
    // - g1: [1,5] (size=2)
    // - g2: [2,6] (size=2)
    // - g3: [3,7] (size=2)
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const expected = [_]u8{ 0, 4, 1, 5, 2, 6, 3, 7 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: Rust test suite sizes" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    // These are the exact sizes tested in Rust test suite
    const sizes = [_]usize{
        64 * 1024, // 65536 bytes - perfectly aligned
        64 * 1024 - 53, // 65483 bytes - rem=3
        64 * 1024 + 135, // 65671 bytes - rem=3
    };

    for (sizes) |size| {
        const data = try allocator.alloc(u8, size);
        defer allocator.free(data);
        random.bytes(data);

        const grouped = try compression.applyByteGrouping(allocator, data);
        defer allocator.free(grouped);

        const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
        defer allocator.free(ungrouped);

        try std.testing.expectEqualSlices(u8, data, ungrouped);
    }
}
