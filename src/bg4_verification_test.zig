const std = @import("std");
const compression = @import("compression.zig");

test "ByteGrouping4LZ4: 10 bytes" {
    const allocator = std.testing.allocator;

    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    const expected = [_]u8{ 0, 4, 8, 1, 5, 9, 2, 6, 3, 7 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: 13 bytes" {
    const allocator = std.testing.allocator;

    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 2, 6, 10, 3, 7, 11 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: 15 bytes" {
    const allocator = std.testing.allocator;

    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: 8 bytes aligned" {
    const allocator = std.testing.allocator;

    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const expected = [_]u8{ 0, 4, 1, 5, 2, 6, 3, 7 };

    const grouped = try compression.applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &expected, grouped);

    const ungrouped = try compression.reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "ByteGrouping4LZ4: various sizes" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    const sizes = [_]usize{
        64 * 1024,
        64 * 1024 - 53,
        64 * 1024 + 135,
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
