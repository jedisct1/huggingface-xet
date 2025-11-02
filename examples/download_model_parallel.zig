const std = @import("std");
const xet = @import("xet");

/// Example: Download a model from Hugging Face using parallel chunk fetching
///
/// This example demonstrates how to use the parallel fetching API to download
/// models faster by fetching, decompressing, and hashing chunks in parallel.
///
/// Usage:
///   HF_TOKEN=your_token zig build run-example-parallel -- <repo_id> <file_hash_hex> <output_path> [num_threads]
///
/// Example:
///   HF_TOKEN=hf_xxx zig build run-example-parallel -- \
///     jedisct1/MiMo-7B-RL-GGUF \
///     04ed9c6064a24be1dbefbd7acd0f8749fc469e3d350e5c44804e686dac353506 \
///     model.gguf \
///     8
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 4) {
        std.debug.print("Usage: {s} <repo_id> <file_hash_hex> <output_path> [num_threads]\n", .{args[0]});
        std.debug.print("\nExample:\n", .{});
        std.debug.print("  HF_TOKEN=hf_xxx {s} \\\n", .{args[0]});
        std.debug.print("    jedisct1/MiMo-7B-RL-GGUF \\\n", .{});
        std.debug.print("    04ed9c6064a24be1dbefbd7acd0f8749fc469e3d350e5c44804e686dac353506 \\\n", .{});
        std.debug.print("    model.gguf \\\n", .{});
        std.debug.print("    8\n", .{});
        return error.InvalidArgs;
    }

    const repo_id = args[1];
    const file_hash_hex = args[2];
    const output_path = args[3];

    // Optional: number of threads (default = CPU count)
    const num_threads: ?usize = if (args.len > 4)
        try std.fmt.parseInt(usize, args[4], 10)
    else
        null;

    const thread_count = num_threads orelse blk: {
        const cpu_count = try std.Thread.getCpuCount();
        break :blk cpu_count;
    };

    std.debug.print("Downloading model with parallel fetching...\n", .{});
    std.debug.print("  Repository: {s}\n", .{repo_id});
    std.debug.print("  File hash:  {s}\n", .{file_hash_hex});
    std.debug.print("  Output:     {s}\n", .{output_path});
    std.debug.print("  Threads:    {d}\n", .{thread_count});
    std.debug.print("\n", .{});

    const config = xet.model_download.DownloadConfig{
        .repo_id = repo_id,
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = file_hash_hex,
    };

    var timer = try std.time.Timer.start();

    // Use parallel download
    var io_instance = std.Io.Threaded.init(allocator);
    defer io_instance.deinit();
    const io = io_instance.io();

    try xet.model_download.downloadModelToFileParallel(
        allocator,
        io,
        config,
        output_path,
        num_threads,
        false, // Don't compute hashes (focus on download speed)
    );

    const duration_ns = timer.read();
    const duration_ms = duration_ns / 1_000_000;

    std.debug.print("\nDownload complete!\n", .{});
    std.debug.print("  Time: {d}ms\n", .{duration_ms});

    // Get file size
    const file = try std.fs.cwd().openFile(output_path, .{});
    defer file.close();
    const file_size = try file.getEndPos();
    const size_mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);

    std.debug.print("  Size: {d:.2} MB\n", .{size_mb});

    const speed_mbps = size_mb / (@as(f64, @floatFromInt(duration_ms)) / 1000.0);
    std.debug.print("  Speed: {d:.2} MB/s\n", .{speed_mbps});
}
