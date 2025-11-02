const std = @import("std");
const Allocator = std.mem.Allocator;
const cas_client = @import("cas_client.zig");
const reconstruction = @import("reconstruction.zig");

/// Configuration for downloading a model from Hugging Face
pub const DownloadConfig = struct {
    /// Repository ID (e.g., "jedisct1/MiMo-7B-RL-GGUF")
    repo_id: []const u8,
    /// Repository type ("model", "dataset", or "space")
    repo_type: []const u8 = "model",
    /// Git revision (e.g., "main", or a commit hash)
    revision: []const u8 = "main",
    /// XET file hash in API hex format (64 hex characters)
    file_hash_hex: []const u8,
    /// Hugging Face API token (if null, reads from HF_TOKEN environment variable)
    hf_token: ?[]const u8 = null,
};

/// Result from XET token exchange with Hugging Face Hub
const XetTokenResult = struct {
    access_token: []const u8,
    cas_url: []const u8,
    exp: i64,
    allocator: Allocator,
    json_parsed: std.json.Parsed(std.json.Value),

    pub fn deinit(self: *XetTokenResult) void {
        self.json_parsed.deinit();
    }
};

/// Request XET access token from Hugging Face Hub
fn requestXetToken(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
    hf_token: []const u8,
) !XetTokenResult {
    // Build token URL
    const token_url = try std.fmt.allocPrint(
        allocator,
        "https://huggingface.co/api/{s}s/{s}/xet-read-token/{s}",
        .{ config.repo_type, config.repo_id, config.revision },
    );
    defer allocator.free(token_url);

    // Initialize HTTP client
    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    // Prepare authorization header
    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{hf_token});
    defer allocator.free(auth_header);

    const extra_headers = [_]std.http.Header{
        .{ .name = "Authorization", .value = auth_header },
    };

    // Make HTTP request
    const uri = try std.Uri.parse(token_url);
    var req = try http_client.request(.GET, uri, .{
        .extra_headers = &extra_headers,
    });
    defer req.deinit();

    try req.sendBodiless();
    var response = try req.receiveHead(&.{});

    if (response.head.status != .ok) {
        return error.AuthenticationFailed;
    }

    // Parse JSON response
    var reader = response.reader(&.{});
    const token_body = try reader.allocRemaining(allocator, @enumFromInt(10 * 1024));
    defer allocator.free(token_body);

    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        token_body,
        .{},
    );
    errdefer parsed.deinit();

    const root = parsed.value.object;
    const access_token = root.get("accessToken").?.string;
    const cas_url = root.get("casUrl").?.string;
    const exp = root.get("exp").?.integer;

    return XetTokenResult{
        .access_token = access_token,
        .cas_url = cas_url,
        .exp = exp,
        .allocator = allocator,
        .json_parsed = parsed,
    };
}

/// Download a model from Hugging Face and write it to a file
///
/// This function handles the complete XET protocol flow:
/// 1. Authenticates with Hugging Face Hub (using HF_TOKEN)
/// 2. Requests XET access token and CAS URL
/// 3. Initializes CAS client
/// 4. Reconstructs the file from XET chunks
/// 5. Writes the reconstructed file to the specified path
///
/// Parameters:
/// - allocator: Memory allocator
/// - config: Download configuration (repository, file hash, etc.)
/// - output_path: Path where the file will be saved
///
/// Example:
/// ```zig
/// const config = DownloadConfig{
///     .repo_id = "jedisct1/MiMo-7B-RL-GGUF",
///     .repo_type = "model",
///     .revision = "main",
///     .file_hash_hex = "04ed9c6064a24be1dbefbd7acd0f8749fc469e3d350e5c44804e686dac353506",
/// };
/// try downloadModelToFile(allocator, config, "model.gguf");
/// ```
pub fn downloadModelToFile(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
    output_path: []const u8,
) !void {
    // Open output file for writing
    const file = try std.fs.cwd().createFile(output_path, .{});
    defer file.close();

    var file_buffer: [4096]u8 = undefined;
    var file_writer = file.writer(&file_buffer);
    defer file_writer.interface.flush() catch {};

    try downloadModelToWriter(allocator, io, config, &file_writer.interface);
}

/// Download a model from Hugging Face and write it to a writer
///
/// This is a lower-level function that gives you control over where the data is written.
/// It performs the same XET protocol flow as downloadModelToFile() but writes to any writer.
///
/// Parameters:
/// - allocator: Memory allocator
/// - config: Download configuration (repository, file hash, etc.)
/// - writer: Writer to receive the reconstructed file data
///
/// Example:
/// ```zig
/// var buffer = std.ArrayList(u8).empty;
/// defer buffer.deinit(allocator);
/// var writer = buffer.writer(allocator);
/// try downloadModelToWriter(allocator, config, &writer.interface);
/// ```
pub fn downloadModelToWriter(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
    writer: *std.Io.Writer,
) !void {
    // Get HF token (from config or environment)
    const hf_token = if (config.hf_token) |token|
        token
    else blk: {
        const token = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(token);
        break :blk token;
    };
    const should_free_token = config.hf_token == null;
    defer if (should_free_token) allocator.free(hf_token);

    // Request XET token from Hugging Face Hub
    var xet_token = try requestXetToken(allocator, io, config, hf_token);
    defer xet_token.deinit();

    // Convert file hash from API hex format to binary
    const file_hash = try cas_client.apiHexToHash(config.file_hash_hex);

    // Initialize CAS client
    var cas = try cas_client.CasClient.init(
        allocator,
        io,
        xet_token.cas_url,
        xet_token.access_token,
    );
    defer cas.deinit();

    // Reconstruct file using stream API
    var reconstructor = reconstruction.FileReconstructor.init(allocator, &cas);
    try reconstructor.reconstructStream(file_hash, writer);
}

/// Download a model from Hugging Face and write it to a writer using parallel fetching
///
/// This is similar to downloadModelToWriter() but uses parallel chunk fetching for better performance.
///
/// Parameters:
/// - allocator: Memory allocator
/// - config: Download configuration (repository, file hash, etc.)
/// - writer: Writer to receive the reconstructed file data
/// - num_threads: Number of worker threads (null = use CPU count)
/// - compute_hashes: Whether to compute hashes during fetching
pub fn downloadModelToWriterParallel(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
    writer: *std.Io.Writer,
    num_threads: ?usize,
    compute_hashes: bool,
) !void {
    // Get HF token (from config or environment)
    const hf_token = if (config.hf_token) |token|
        token
    else blk: {
        const token = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(token);
        break :blk token;
    };
    const should_free_token = config.hf_token == null;
    defer if (should_free_token) allocator.free(hf_token);

    // Request XET token from Hugging Face Hub
    var xet_token = try requestXetToken(allocator, io, config, hf_token);
    defer xet_token.deinit();

    // Convert file hash from API hex format to binary
    const file_hash = try cas_client.apiHexToHash(config.file_hash_hex);

    // Initialize CAS client
    var cas = try cas_client.CasClient.init(
        allocator,
        io,
        xet_token.cas_url,
        xet_token.access_token,
    );
    defer cas.deinit();

    // Reconstruct file using parallel stream API
    var reconstructor = reconstruction.FileReconstructor.init(allocator, &cas);
    try reconstructor.reconstructStreamParallel(file_hash, writer, num_threads, compute_hashes);
}

/// Download a model from Hugging Face and write it to a file using parallel fetching
///
/// This is similar to downloadModelToFile() but uses parallel chunk fetching for better performance.
///
/// Parameters:
/// - allocator: Memory allocator
/// - config: Download configuration (repository, file hash, etc.)
/// - output_path: Path where the file will be saved
/// - num_threads: Number of worker threads (null = use CPU count)
/// - compute_hashes: Whether to compute hashes during fetching
pub fn downloadModelToFileParallel(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
    output_path: []const u8,
    num_threads: ?usize,
    compute_hashes: bool,
) !void {
    // Open output file for writing
    const file = try std.fs.cwd().createFile(output_path, .{});
    defer file.close();

    var file_buffer: [4096]u8 = undefined;
    var file_writer = file.writer(&file_buffer);
    defer file_writer.interface.flush() catch {};

    try downloadModelToWriterParallel(allocator, io, config, &file_writer.interface, num_threads, compute_hashes);
}

/// Download a model from Hugging Face and return it as owned memory
///
/// This function downloads the entire model into memory and returns it as a slice.
/// Use this for small models or when you need the entire file in memory.
/// For large models, prefer downloadModelToFile() or downloadModelToWriter().
///
/// The returned slice is owned by the caller and must be freed with allocator.free().
///
/// Parameters:
/// - allocator: Memory allocator
/// - config: Download configuration (repository, file hash, etc.)
///
/// Returns: Owned slice containing the complete file data
///
/// Example:
/// ```zig
/// const config = DownloadConfig{
///     .repo_id = "user/small-model",
///     .file_hash_hex = "...",
/// };
/// const data = try downloadModel(allocator, config);
/// defer allocator.free(data);
/// ```
pub fn downloadModel(
    allocator: Allocator,
    io: std.Io,
    config: DownloadConfig,
) ![]u8 {
    // Get HF token (from config or environment)
    const hf_token = if (config.hf_token) |token|
        token
    else blk: {
        const token = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(token);
        break :blk token;
    };
    const should_free_token = config.hf_token == null;
    defer if (should_free_token) allocator.free(hf_token);

    // Request XET token from Hugging Face Hub
    var xet_token = try requestXetToken(allocator, io, config, hf_token);
    defer xet_token.deinit();

    // Convert file hash from API hex format to binary
    const file_hash = try cas_client.apiHexToHash(config.file_hash_hex);

    // Initialize CAS client
    var cas = try cas_client.CasClient.init(
        allocator,
        io,
        xet_token.cas_url,
        xet_token.access_token,
    );
    defer cas.deinit();

    // Reconstruct file in memory
    var reconstructor = reconstruction.FileReconstructor.init(allocator, &cas);
    return try reconstructor.reconstructFile(file_hash);
}
