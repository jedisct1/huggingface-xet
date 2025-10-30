const std = @import("std");
const xet = @import("xet");

/// Download a model from Hugging Face using the XET protocol
///
/// This example demonstrates how to use the high-level model download API.
/// The library handles all the complexity:
/// - Authentication with Hugging Face Hub
/// - Requesting XET tokens
/// - Querying CAS for file reconstruction info
/// - Fetching chunks from xorbs (with full deduplication support)
/// - Reconstructing and saving the file
///
/// Example model: MiMo-7B-RL-Q8_0.gguf from jedisct1/MiMo-7B-RL-GGUF
///
/// Usage:
///   1. Set HF_TOKEN environment variable with your Hugging Face token
///      Get one at: https://huggingface.co/settings/tokens
///   2. Run: zig build run-example-download
///
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    defer stdout_writer.interface.flush() catch {};
    const stdout = &stdout_writer.interface;

    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = std.fs.File.stderr().writer(&stderr_buffer);
    defer stderr_writer.interface.flush() catch {};
    const stderr = &stderr_writer.interface;

    // Configuration
    const config = xet.model_download.DownloadConfig{
        .repo_id = "jedisct1/MiMo-7B-RL-GGUF",
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = "04ed9c6064a24be1dbefbd7acd0f8749fc469e3d350e5c44804e686dac353506",
    };
    const output_path = "MiMo-7B-RL-Q8_0.gguf";

    try stdout.print("XET Protocol Model Download Example\n", .{});
    try stdout.print("====================================\n\n", .{});
    try stdout.print("Repository: {s}\n", .{config.repo_id});
    try stdout.print("File hash: {s}\n", .{config.file_hash_hex});
    try stdout.print("Output: {s}\n\n", .{output_path});

    try stdout.print("Downloading model using XET protocol...\n", .{});
    try stdout.print("This may take a while for large models.\n\n", .{});

    try stdout.flush();

    const start_time = try std.time.Instant.now();

    // Download the model using the high-level API
    xet.model_download.downloadModelToFile(allocator, config, output_path) catch |err| {
        try stderr.print("\nError: Download failed: {}\n", .{err});
        if (err == error.FileNotFound) {
            try stderr.print("Make sure HF_TOKEN environment variable is set.\n", .{});
            try stderr.print("Get a token at: https://huggingface.co/settings/tokens\n", .{});
        } else if (err == error.AuthenticationFailed) {
            try stderr.print("Authentication failed. Check that:\n", .{});
            try stderr.print("  - HF_TOKEN is valid and not expired\n", .{});
            try stderr.print("  - You have access to the repository\n", .{});
        }
        return err;
    };

    const end_time = try std.time.Instant.now();
    const elapsed_ns = end_time.since(start_time);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_ms;
    const elapsed_s = elapsed_ms / 1000.0;

    // Get file size for stats
    const file = try std.fs.cwd().openFile(output_path, .{});
    defer file.close();
    const file_size = try file.getEndPos();

    try stdout.print("Download complete!\n", .{});
    try stdout.print("  Time: {d:.2}s\n", .{elapsed_s});
    try stdout.print("  Size: {d} bytes ({d:.2} GB)\n", .{
        file_size,
        @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0 * 1024.0),
    });
    try stdout.print("  Speed: {d:.2} MB/s\n", .{
        @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0) / elapsed_s,
    });
    try stdout.print("  Output: {s}\n", .{output_path});

    try stdout.print("\n====================================\n", .{});
    try stdout.print("Model downloaded successfully!\n", .{});
}
