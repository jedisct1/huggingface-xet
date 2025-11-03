const std = @import("std");
const chunking = @import("chunking.zig");
const hashing = @import("hashing.zig");
const compression = @import("compression.zig");
const xorb = @import("xorb.zig");

const Timer = std.time.Timer;

pub const BenchmarkResult = struct {
    name: []const u8,
    duration_ns: u64,
    throughput_mbs: f64,
    iterations: usize,
};

pub fn formatDuration(ns: u64) void {
    if (ns < 1000) {
        std.debug.print("{d} ns", .{ns});
    } else if (ns < 1_000_000) {
        std.debug.print("{d:.2} µs", .{@as(f64, @floatFromInt(ns)) / 1000.0});
    } else if (ns < 1_000_000_000) {
        std.debug.print("{d:.2} ms", .{@as(f64, @floatFromInt(ns)) / 1_000_000.0});
    } else {
        std.debug.print("{d:.3} s", .{@as(f64, @floatFromInt(ns)) / 1_000_000_000.0});
    }
}

pub fn printResult(result: BenchmarkResult) void {
    std.debug.print("{s}: ", .{result.name});
    formatDuration(result.duration_ns);
    if (result.throughput_mbs > 0) {
        std.debug.print(" ({d:.2} MB/s)", .{result.throughput_mbs});
    }
    if (result.iterations > 1) {
        std.debug.print(" [{d} iterations]", .{result.iterations});
    }
    std.debug.print("\n", .{});
}

fn generateRandomData(allocator: std.mem.Allocator, size: usize, seed: u64) ![]u8 {
    const data = try allocator.alloc(u8, size);
    var rng = std.Random.DefaultPrng.init(seed);
    const random = rng.random();
    random.bytes(data);
    return data;
}

pub fn benchmarkChunking(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 100 * 1024 * 1024; // 100 MB
    const data = try generateRandomData(allocator, data_size, 12345);
    defer allocator.free(data);

    var timer = try Timer.start();
    const start = timer.read();

    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&boundaries);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "Chunking (100 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkHashing(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 100 * 1024 * 1024; // 100 MB
    const data = try generateRandomData(allocator, data_size, 54321);
    defer allocator.free(data);

    var timer = try Timer.start();
    const start = timer.read();

    const hash = hashing.computeDataHash(data);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&hash);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "BLAKE3 Hashing (100 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkMerkleTree(allocator: std.mem.Allocator) !BenchmarkResult {
    // Create 100 fake chunk hashes
    var chunk_infos = std.ArrayList(hashing.MerkleNode).empty;
    defer chunk_infos.deinit(allocator);

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        var hash: [32]u8 = undefined;
        @memset(&hash, @as(u8, @intCast(i)));
        try chunk_infos.append(allocator, .{
            .hash = hash,
            .size = 65536,
        });
    }

    var timer = try Timer.start();
    const start = timer.read();

    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&file_hash);

    return BenchmarkResult{
        .name = "Merkle Tree (100 chunks)",
        .duration_ns = duration,
        .throughput_mbs = 0,
        .iterations = 1,
    };
}

pub fn benchmarkLZ4Compression(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024; // 50 MB
    const data = try generateRandomData(allocator, data_size, 98765);
    defer allocator.free(data);

    var timer = try Timer.start();
    const start = timer.read();

    const result = try compression.compress(allocator, data, .LZ4);
    defer allocator.free(result.data);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&result);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "LZ4 Compression (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkLZ4Decompression(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024; // 50 MB
    const data = try generateRandomData(allocator, data_size, 98765);
    defer allocator.free(data);

    const compressed = try compression.compress(allocator, data, .LZ4);
    defer allocator.free(compressed.data);

    var timer = try Timer.start();
    const start = timer.read();

    const decompressed = try compression.decompress(allocator, compressed.data, compressed.type, data_size);
    defer allocator.free(decompressed);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&decompressed);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "LZ4 Decompression (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkByteGrouping4LZ4(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024; // 50 MB
    // Create structured data (array of integers) for better compression
    const data = try allocator.alloc(u8, data_size);
    defer allocator.free(data);

    var i: usize = 0;
    while (i < data_size / 4) : (i += 1) {
        const value = @as(u32, @intCast(i));
        std.mem.writeInt(u32, data[i * 4 ..][0..4], value, .little);
    }

    var timer = try Timer.start();
    const start = timer.read();

    const result = try compression.compress(allocator, data, .ByteGrouping4LZ4);
    defer allocator.free(result.data);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&result);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "ByteGrouping4LZ4 (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkXorbSerialization(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024; // 50 MB
    const data = try generateRandomData(allocator, data_size, 11111);
    defer allocator.free(data);

    // Chunk the data first
    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    // Build xorb
    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        _ = try builder.addChunk(chunk);
    }

    var timer = try Timer.start();
    const start = timer.read();

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&serialized);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "Xorb Serialization (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkEndToEnd(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024; // 50 MB
    const data = try generateRandomData(allocator, data_size, 99999);
    defer allocator.free(data);

    var timer = try Timer.start();
    const start = timer.read();

    // 1. Chunk the data
    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    // 2. Hash each chunk and build merkle tree
    var chunk_infos = std.ArrayList(hashing.MerkleNode).empty;
    defer chunk_infos.deinit(allocator);

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        const chunk_hash = hashing.computeDataHash(chunk);
        try chunk_infos.append(allocator, .{
            .hash = chunk_hash,
            .size = @as(u64, @intCast(chunk.len)),
        });
    }

    // 3. Compute file hash
    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&file_hash);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "End-to-End (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn benchmarkEndToEndLarge(allocator: std.mem.Allocator) !BenchmarkResult {
    const data_size = 1024 * 1024 * 1024; // 1 GB
    std.debug.print("Allocating 1 GB for benchmark...\n", .{});
    const data = try generateRandomData(allocator, data_size, 99999);
    defer allocator.free(data);

    std.debug.print("Starting end-to-end benchmark...\n", .{});
    var timer = try Timer.start();
    const start = timer.read();

    // 1. Chunk the data
    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    // 2. Hash each chunk and build merkle tree
    var chunk_infos = std.ArrayList(hashing.MerkleNode).empty;
    defer chunk_infos.deinit(allocator);

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        const chunk_hash = hashing.computeDataHash(chunk);
        try chunk_infos.append(allocator, .{
            .hash = chunk_hash,
            .size = @as(u64, @intCast(chunk.len)),
        });
    }

    // 3. Compute file hash
    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const end = timer.read();
    const duration = end - start;

    std.mem.doNotOptimizeAway(&file_hash);

    const throughput = (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration))) * 1_000_000_000.0 / (1024.0 * 1024.0);

    return BenchmarkResult{
        .name = "End-to-End (1 GB)",
        .duration_ns = duration,
        .throughput_mbs = throughput,
        .iterations = 1,
    };
}

pub fn runAllBenchmarks(allocator: std.mem.Allocator) !void {
    std.debug.print("\n=== XET Protocol Performance Benchmarks ===\n\n", .{});

    const benchmarks = [_]struct {
        name: []const u8,
        func: *const fn (std.mem.Allocator) anyerror!BenchmarkResult,
    }{
        .{ .name = "Chunking", .func = benchmarkChunking },
        .{ .name = "Hashing", .func = benchmarkHashing },
        .{ .name = "Merkle Tree", .func = benchmarkMerkleTree },
        .{ .name = "LZ4 Compression", .func = benchmarkLZ4Compression },
        .{ .name = "LZ4 Decompression", .func = benchmarkLZ4Decompression },
        .{ .name = "ByteGrouping4LZ4", .func = benchmarkByteGrouping4LZ4 },
        .{ .name = "Xorb Serialization", .func = benchmarkXorbSerialization },
        .{ .name = "End-to-End", .func = benchmarkEndToEnd },
        .{ .name = "End-to-End Large", .func = benchmarkEndToEndLarge },
    };

    for (benchmarks) |bench| {
        const result = try bench.func(allocator);
        printResult(result);
    }

    std.debug.print("\n=== Benchmarks Complete ===\n", .{});
}

test "benchmark smoke test" {
    // Just verify that benchmarks can run without errors
    const allocator = std.testing.allocator;
    _ = try benchmarkChunking(allocator);
}
