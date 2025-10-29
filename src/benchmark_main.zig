const std = @import("std");
const zig_xet = @import("xet");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    try zig_xet.benchmark.runAllBenchmarks(allocator);
}
