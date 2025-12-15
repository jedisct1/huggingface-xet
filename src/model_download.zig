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

/// Information about a file in a HuggingFace repository
pub const FileInfo = struct {
    /// File path within the repository
    path: []const u8,
    /// File size in bytes
    size: u64,
    /// XET hash (if file is stored with XET protocol)
    xet_hash: ?[]const u8,

    allocator: Allocator,

    pub fn deinit(self: *FileInfo) void {
        self.allocator.free(self.path);
        if (self.xet_hash) |hash| {
            self.allocator.free(hash);
        }
    }
};

/// List of files in a repository
pub const FileList = struct {
    files: []FileInfo,
    allocator: Allocator,

    pub fn deinit(self: *FileList) void {
        for (self.files) |*file| {
            file.deinit();
        }
        self.allocator.free(self.files);
    }

    /// Get files that have XET hashes (large files stored with XET protocol)
    pub fn getXetFiles(self: *const FileList) []const FileInfo {
        var count: usize = 0;
        for (self.files) |file| {
            if (file.xet_hash != null) count += 1;
        }
        if (count == 0) return &.{};

        const result = self.allocator.alloc(FileInfo, count) catch return &.{};
        var i: usize = 0;
        for (self.files) |file| {
            if (file.xet_hash != null) {
                result[i] = file;
                i += 1;
            }
        }
        return result;
    }

    /// Find a file by path (exact match or suffix match)
    pub fn findFile(self: *const FileList, name: []const u8) ?*const FileInfo {
        for (self.files) |*file| {
            if (std.mem.eql(u8, file.path, name)) return file;
            if (std.mem.endsWith(u8, file.path, name)) return file;
        }
        return null;
    }
};

/// List files in a HuggingFace repository
pub fn listFiles(
    allocator: Allocator,
    io: std.Io,
    repo_id: []const u8,
    repo_type: []const u8,
    revision: []const u8,
    hf_token: ?[]const u8,
) !FileList {
    const token = if (hf_token) |t|
        t
    else blk: {
        const t = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(t);
        break :blk t;
    };
    const should_free_token = hf_token == null;
    defer if (should_free_token) allocator.free(token);

    const tree_url = try std.fmt.allocPrint(
        allocator,
        "https://huggingface.co/api/{s}s/{s}/tree/{s}",
        .{ repo_type, repo_id, revision },
    );
    defer allocator.free(tree_url);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
    defer allocator.free(auth_header);

    const extra_headers = [_]std.http.Header{
        .{ .name = "Authorization", .value = auth_header },
    };

    const uri = try std.Uri.parse(tree_url);
    var req = try http_client.request(.GET, uri, .{
        .extra_headers = &extra_headers,
    });
    defer req.deinit();

    try req.sendBodiless();
    var response = try req.receiveHead(&.{});

    if (response.head.status != .ok) {
        return error.ApiRequestFailed;
    }

    var reader = response.reader(&.{});
    const body = try reader.allocRemaining(allocator, @enumFromInt(1024 * 1024));
    defer allocator.free(body);

    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        body,
        .{},
    );
    defer parsed.deinit();

    const items = parsed.value.array;
    var files: std.ArrayList(FileInfo) = .empty;
    errdefer {
        for (files.items) |*f| f.deinit();
        files.deinit(allocator);
    }

    for (items.items) |item| {
        const obj = item.object;
        const file_type = obj.get("type") orelse continue;
        if (!std.mem.eql(u8, file_type.string, "file")) continue;

        const path_val = obj.get("path") orelse continue;
        const path = try allocator.dupe(u8, path_val.string);
        errdefer allocator.free(path);

        const size: u64 = if (obj.get("size")) |s| @intCast(s.integer) else 0;

        const xet_hash: ?[]const u8 = if (obj.get("xetHash")) |h|
            try allocator.dupe(u8, h.string)
        else
            null;

        try files.append(allocator, .{
            .path = path,
            .size = size,
            .xet_hash = xet_hash,
            .allocator = allocator,
        });
    }

    return .{
        .files = try files.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

/// Get XET hash for a specific file using the resolve endpoint
/// This is an alternative to listFiles() when you know the exact file path
pub fn getFileXetHash(
    allocator: Allocator,
    io: std.Io,
    repo_id: []const u8,
    revision: []const u8,
    filepath: []const u8,
    hf_token: ?[]const u8,
) ![]const u8 {
    const token = if (hf_token) |t|
        t
    else blk: {
        const t = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(t);
        break :blk t;
    };
    const should_free_token = hf_token == null;
    defer if (should_free_token) allocator.free(token);

    const resolve_url = try std.fmt.allocPrint(
        allocator,
        "https://huggingface.co/{s}/resolve/{s}/{s}",
        .{ repo_id, revision, filepath },
    );
    defer allocator.free(resolve_url);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
    defer allocator.free(auth_header);

    const extra_headers = [_]std.http.Header{
        .{ .name = "Authorization", .value = auth_header },
    };

    const uri = try std.Uri.parse(resolve_url);
    var req = try http_client.request(.HEAD, uri, .{
        .extra_headers = &extra_headers,
    });
    defer req.deinit();

    try req.sendBodiless();
    _ = try req.receiveHead(&.{ .max_redirects = 0 });

    const xet_hash_header = req.response.iterateHeaders(.{ .name = "x-xet-hash" }).next();
    if (xet_hash_header) |header| {
        return try allocator.dupe(u8, header.value);
    }

    return error.NoXetHash;
}

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
/// Each worker thread has its own IO instance for thread safety.
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
    const hf_token = if (config.hf_token) |token|
        token
    else blk: {
        const token = try std.process.getEnvVarOwned(allocator, "HF_TOKEN");
        errdefer allocator.free(token);
        break :blk token;
    };
    const should_free_token = config.hf_token == null;
    defer if (should_free_token) allocator.free(hf_token);

    var xet_token = try requestXetToken(allocator, io, config, hf_token);
    defer xet_token.deinit();

    const file_hash = try cas_client.apiHexToHash(config.file_hash_hex);

    var cas = try cas_client.CasClient.init(
        allocator,
        io,
        xet_token.cas_url,
        xet_token.access_token,
    );
    defer cas.deinit();

    var reconstructor = reconstruction.FileReconstructor.init(allocator, &cas);
    try reconstructor.reconstructStreamParallel(file_hash, writer, num_threads, compute_hashes);
}

/// Download a model from Hugging Face and write it to a file using parallel fetching
///
/// This is similar to downloadModelToFile() but uses parallel chunk fetching for better performance.
/// Each worker thread has its own IO instance for thread safety.
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
