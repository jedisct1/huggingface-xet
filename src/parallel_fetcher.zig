//! Parallel chunk fetcher using thread pool
//!
//! This module provides parallel downloading, decompression, and hashing of chunks.
//! Each worker thread has its own IO and HTTP client instance for thread safety.

const std = @import("std");
const Allocator = std.mem.Allocator;
const cas_client = @import("cas_client.zig");
const xorb = @import("xorb.zig");
const hashing = @import("hashing.zig");

/// Work item for parallel fetching
const ChunkWork = struct {
    term: cas_client.ReconstructionTerm,
    fetch_info: []cas_client.FetchInfo,
    index: usize,
};

/// Result of chunk processing
pub const ChunkResult = struct {
    data: []u8,
    hash: ?hashing.Hash,
    index: usize,
    allocator: Allocator,

    pub fn deinit(self: *ChunkResult) void {
        self.allocator.free(self.data);
    }
};

/// Context for worker threads (shared, read-only after init)
const WorkerContext = struct {
    allocator: Allocator,
    work_queue: *std.ArrayList(ChunkWork),
    results: []?ChunkResult,
    mutex: *std.Thread.Mutex,
    error_occurred: *std.atomic.Value(bool),
    first_error: *?anyerror,
    error_mutex: *std.Thread.Mutex,
    compute_hashes: bool,
};

/// Fetch data from a presigned URL using the provided HTTP client
fn fetchFromUrl(
    allocator: Allocator,
    http_client: *std.http.Client,
    url: []const u8,
    byte_range: ?struct { start: u64, end: u64 },
) ![]u8 {
    var range_header_buf: [64]u8 = undefined;
    var extra_headers_storage: [1]std.http.Header = undefined;
    var extra_headers_count: usize = 0;

    if (byte_range) |r| {
        const range_header = try std.fmt.bufPrint(
            &range_header_buf,
            "bytes={d}-{d}",
            .{ r.start, r.end },
        );
        extra_headers_storage[0] = .{ .name = "Range", .value = range_header };
        extra_headers_count = 1;
    }

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.GET, uri, .{
        .extra_headers = extra_headers_storage[0..extra_headers_count],
    });
    defer req.deinit();

    try req.sendBodiless();
    var response = try req.receiveHead(&.{});

    if (response.head.status != .ok and response.head.status != .partial_content) {
        return error.HttpError;
    }

    var reader = response.reader(&.{});
    return try reader.allocRemaining(allocator, @enumFromInt(128 * 1024 * 1024));
}

/// Process a single chunk work item
fn processChunk(
    ctx: *WorkerContext,
    http_client: *std.http.Client,
) !?ChunkResult {
    var work_item: ?ChunkWork = null;
    {
        ctx.mutex.lock();
        defer ctx.mutex.unlock();

        if (ctx.work_queue.items.len > 0) {
            work_item = ctx.work_queue.pop();
        }
    }

    if (work_item == null) return null;
    const work = work_item.?;

    const matching_fetch_info = blk: {
        for (work.fetch_info) |fetch_info| {
            if (fetch_info.range.start <= work.term.range.start and
                fetch_info.range.end >= work.term.range.end)
            {
                break :blk fetch_info;
            }
        }
        return error.NoMatchingFetchInfo;
    };

    const xorb_data = try fetchFromUrl(
        ctx.allocator,
        http_client,
        matching_fetch_info.url,
        .{ .start = matching_fetch_info.url_range.start, .end = matching_fetch_info.url_range.end },
    );
    defer ctx.allocator.free(xorb_data);

    const local_start = work.term.range.start - matching_fetch_info.range.start;
    const local_end = work.term.range.end - matching_fetch_info.range.start;

    var xorb_reader = xorb.XorbReader.init(ctx.allocator, xorb_data);
    const chunk_data = try xorb_reader.extractChunkRange(local_start, local_end);
    errdefer ctx.allocator.free(chunk_data);

    const chunk_hash = if (ctx.compute_hashes)
        hashing.computeDataHash(chunk_data)
    else
        null;

    return ChunkResult{
        .data = chunk_data,
        .hash = chunk_hash,
        .index = work.index,
        .allocator = ctx.allocator,
    };
}

/// Worker thread function - creates its own IO and HTTP client
fn workerThread(ctx: *WorkerContext) void {
    var io_instance = std.Io.Threaded.init(ctx.allocator);
    defer io_instance.deinit();
    const io = io_instance.io();

    var http_client = std.http.Client{ .allocator = ctx.allocator, .io = io };
    defer http_client.deinit();

    while (!ctx.error_occurred.load(.acquire)) {
        const result = processChunk(ctx, &http_client) catch |err| {
            ctx.error_mutex.lock();
            defer ctx.error_mutex.unlock();

            if (ctx.first_error.* == null) {
                ctx.first_error.* = err;
            }
            ctx.error_occurred.store(true, .release);
            return;
        };

        if (result) |chunk_result| {
            ctx.mutex.lock();
            defer ctx.mutex.unlock();

            ctx.results[chunk_result.index] = chunk_result;
        } else {
            break;
        }
    }
}

/// Parallel chunk fetcher
pub const ParallelFetcher = struct {
    allocator: Allocator,
    num_threads: usize,
    compute_hashes: bool,

    pub fn init(
        allocator: Allocator,
        num_threads: ?usize,
        compute_hashes: bool,
    ) ParallelFetcher {
        const thread_count = num_threads orelse @max(1, std.Thread.getCpuCount() catch 4);
        return ParallelFetcher{
            .allocator = allocator,
            .num_threads = thread_count,
            .compute_hashes = compute_hashes,
        };
    }

    /// Fetch all chunks in parallel and return results in order
    pub fn fetchAll(
        self: *ParallelFetcher,
        terms: []cas_client.ReconstructionTerm,
        fetch_info_map: std.StringHashMap([]cas_client.FetchInfo),
    ) ![]ChunkResult {
        if (terms.len == 0) {
            return &[_]ChunkResult{};
        }

        var work_queue: std.ArrayList(ChunkWork) = .empty;
        defer work_queue.deinit(self.allocator);

        for (terms, 0..) |term, i| {
            const hash_hex = try cas_client.hashToApiHex(term.hash, self.allocator);
            defer self.allocator.free(hash_hex);

            const fetch_infos = fetch_info_map.get(hash_hex) orelse return error.MissingFetchInfo;

            try work_queue.append(self.allocator, .{
                .term = term,
                .fetch_info = fetch_infos,
                .index = i,
            });
        }

        std.mem.reverse(ChunkWork, work_queue.items);

        const results = try self.allocator.alloc(?ChunkResult, terms.len);
        defer {
            for (results) |*opt_result| {
                if (opt_result.*) |*result| {
                    result.deinit();
                }
            }
            self.allocator.free(results);
        }
        @memset(results, null);

        var mutex = std.Thread.Mutex{};
        var error_occurred = std.atomic.Value(bool).init(false);
        var first_error: ?anyerror = null;
        var error_mutex = std.Thread.Mutex{};

        var ctx = WorkerContext{
            .allocator = self.allocator,
            .work_queue = &work_queue,
            .results = results,
            .mutex = &mutex,
            .error_occurred = &error_occurred,
            .first_error = &first_error,
            .error_mutex = &error_mutex,
            .compute_hashes = self.compute_hashes,
        };

        const threads = try self.allocator.alloc(std.Thread, self.num_threads);
        defer self.allocator.free(threads);

        for (threads) |*thread| {
            thread.* = try std.Thread.spawn(.{}, workerThread, .{&ctx});
        }

        for (threads) |thread| {
            thread.join();
        }

        if (error_occurred.load(.acquire)) {
            return first_error orelse error.UnknownError;
        }

        var ordered_results = try self.allocator.alloc(ChunkResult, terms.len);
        errdefer {
            for (ordered_results) |*result| {
                result.deinit();
            }
            self.allocator.free(ordered_results);
        }

        for (results, 0..) |opt_result, i| {
            if (opt_result) |result| {
                ordered_results[i] = result;
                results[i] = null;
            } else {
                return error.MissingResult;
            }
        }

        return ordered_results;
    }

    /// Fetch all chunks and write directly to writer
    pub fn fetchAndWrite(
        self: *ParallelFetcher,
        terms: []cas_client.ReconstructionTerm,
        fetch_info_map: std.StringHashMap([]cas_client.FetchInfo),
        writer: *std.Io.Writer,
    ) !void {
        const results = try self.fetchAll(terms, fetch_info_map);
        defer {
            for (results) |*result| {
                var mut_result = result.*;
                mut_result.deinit();
            }
            self.allocator.free(results);
        }

        for (results) |result| {
            try writer.writeAll(result.data);
        }
    }
};
