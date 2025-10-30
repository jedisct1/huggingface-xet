const std = @import("std");
const Allocator = std.mem.Allocator;
const cas_client = @import("cas_client.zig");
const xorb = @import("xorb.zig");
const shard = @import("shard.zig");

/// File reconstruction module
///
/// This module provides functionality for reconstructing files from the XET CAS
/// by fetching and assembling chunks based on file reconstruction information.
///
/// The reconstruction process involves:
/// 1. Query CAS for file reconstruction information (terms and fetch_info)
/// 2. For each term, download the xorb from the fetch_info URL
/// 3. Extract the specified chunk range from the xorb
/// 4. Assemble chunks in order to reconstruct the file
const XorbData = struct {
    data: []u8,
    local_start: u32,
    local_end: u32,
};

/// File reconstructor - reconstructs files from CAS
pub const FileReconstructor = struct {
    allocator: Allocator,
    cas: *cas_client.CasClient,

    pub fn init(allocator: Allocator, cas: *cas_client.CasClient) FileReconstructor {
        return FileReconstructor{
            .allocator = allocator,
            .cas = cas,
        };
    }

    fn fetchXorbForTerm(
        self: *FileReconstructor,
        term: cas_client.ReconstructionTerm,
        fetch_info_map: std.StringHashMap([]cas_client.FetchInfo),
    ) !XorbData {
        const hash_hex = try cas_client.hashToApiHex(term.hash, self.allocator);
        defer self.allocator.free(hash_hex);

        const fetch_infos = fetch_info_map.get(hash_hex) orelse return error.MissingFetchInfo;

        for (fetch_infos) |fetch_info| {
            if (fetch_info.range.start <= term.range.start and fetch_info.range.end >= term.range.end) {
                const xorb_data = try self.cas.fetchXorbFromUrl(
                    fetch_info.url,
                    .{ .start = fetch_info.url_range.start, .end = fetch_info.url_range.end },
                );

                const local_start = term.range.start - fetch_info.range.start;
                const local_end = term.range.end - fetch_info.range.start;

                return XorbData{
                    .data = xorb_data,
                    .local_start = local_start,
                    .local_end = local_end,
                };
            }
        }

        return error.NoMatchingFetchInfo;
    }

    /// Reconstruct a file from its hash
    /// Returns the complete file data
    pub fn reconstructFile(self: *FileReconstructor, file_hash: [32]u8) ![]u8 {
        const recon = try self.cas.getReconstruction(file_hash, null);
        defer {
            var mut_recon = recon;
            mut_recon.deinit();
        }

        var total_size: u64 = 0;
        for (recon.terms) |term| {
            total_size += term.unpacked_length;
        }

        var result = try self.allocator.alloc(u8, total_size);
        errdefer self.allocator.free(result);

        var offset: u64 = 0;
        for (recon.terms) |term| {
            const xorb_info = try self.fetchXorbForTerm(term, recon.fetch_info);
            defer self.allocator.free(xorb_info.data);

            var xorb_reader = xorb.XorbReader.init(self.allocator, xorb_info.data);
            const chunk_data = try xorb_reader.extractChunkRange(xorb_info.local_start, xorb_info.local_end);
            defer self.allocator.free(chunk_data);

            if (chunk_data.len != term.unpacked_length) {
                return error.SizeMismatch;
            }

            @memcpy(result[offset..][0..chunk_data.len], chunk_data);
            offset += chunk_data.len;
        }

        return result;
    }

    fn copyTermIntoRange(
        result: []u8,
        term_data: []const u8,
        pending_skip: *usize,
        result_offset: *usize,
        remaining: *usize,
    ) void {
        if (remaining.* == 0) return;

        var slice = term_data;
        if (pending_skip.* != 0) {
            const skip = @min(pending_skip.*, slice.len);
            slice = slice[skip..];
            pending_skip.* -= skip;
            if (slice.len == 0) return;
        }

        const to_copy = @min(slice.len, remaining.*);
        @memcpy(result[result_offset.* ..][0..to_copy], slice[0..to_copy]);
        result_offset.* += to_copy;
        remaining.* -= to_copy;
    }

    /// Reconstruct a range of bytes from a file
    /// Returns data for the specified byte range [start, end) (end is exclusive)
    pub fn reconstructRange(
        self: *FileReconstructor,
        file_hash: [32]u8,
        start: u64,
        end: u64,
    ) ![]u8 {
        const recon = try self.cas.getReconstruction(
            file_hash,
            .{ .start = start, .end = end - 1 },
        );
        defer {
            var mut_recon = recon;
            mut_recon.deinit();
        }

        const size_u64 = end - start;
        const result_len = std.math.cast(usize, size_u64) catch return error.RangeTooLarge;
        const result = try self.allocator.alloc(u8, result_len);
        errdefer self.allocator.free(result);

        var pending_skip = std.math.cast(usize, recon.offset_into_first_range) catch return error.OffsetTooLarge;
        var remaining = result_len;
        var result_offset: usize = 0;

        for (recon.terms) |term| {
            const xorb_info = try self.fetchXorbForTerm(term, recon.fetch_info);
            defer self.allocator.free(xorb_info.data);

            var xorb_reader = xorb.XorbReader.init(self.allocator, xorb_info.data);
            const chunk_data = try xorb_reader.extractChunkRange(xorb_info.local_start, xorb_info.local_end);
            defer self.allocator.free(chunk_data);

            copyTermIntoRange(result, chunk_data, &pending_skip, &result_offset, &remaining);
            if (remaining == 0) break;
        }

        if (remaining != 0) return error.SizeMismatch;

        return result;
    }

    /// Stream reconstruction - reconstruct file and write to writer
    pub fn reconstructStream(
        self: *FileReconstructor,
        file_hash: [32]u8,
        writer: *std.Io.Writer,
    ) !void {
        const recon = try self.cas.getReconstruction(file_hash, null);
        defer {
            var mut_recon = recon;
            mut_recon.deinit();
        }

        for (recon.terms) |term| {
            const xorb_info = try self.fetchXorbForTerm(term, recon.fetch_info);
            defer self.allocator.free(xorb_info.data);

            var xorb_reader = xorb.XorbReader.init(self.allocator, xorb_info.data);
            const chunk_data = try xorb_reader.extractChunkRange(xorb_info.local_start, xorb_info.local_end);
            defer self.allocator.free(chunk_data);

            try writer.writeAll(chunk_data);
        }
    }
};

// Tests
test "xorb reader - extract chunk range" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create a test xorb with multiple chunks
    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("Chunk 0");
    _ = try builder.addChunk("Chunk 1");
    _ = try builder.addChunk("Chunk 2");
    _ = try builder.addChunk("Chunk 3");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    // Extract chunks 1-3 (exclusive end)
    var reader = xorb.XorbReader.init(allocator, serialized);
    const range_data = try reader.extractChunkRange(1, 3);
    defer allocator.free(range_data);

    // Should contain "Chunk 1" + "Chunk 2"
    try testing.expectEqualSlices(u8, "Chunk 1Chunk 2", range_data);
}

test "xorb reader - extract single chunk as range" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Create a test xorb with multiple chunks
    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("Chunk 0");
    _ = try builder.addChunk("Chunk 1");
    _ = try builder.addChunk("Chunk 2");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    // Extract chunk 1 only
    var reader = xorb.XorbReader.init(allocator, serialized);
    const range_data = try reader.extractChunkRange(1, 2);
    defer allocator.free(range_data);

    try testing.expectEqualSlices(u8, "Chunk 1", range_data);
}

test "xorb reader - invalid range" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("Chunk 0");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    var reader = xorb.XorbReader.init(allocator, serialized);

    // Start >= end
    const result = reader.extractChunkRange(5, 5);
    try testing.expectError(error.InvalidRange, result);
}

test "xorb reader - range out of bounds" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("Chunk 0");
    _ = try builder.addChunk("Chunk 1");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    var reader = xorb.XorbReader.init(allocator, serialized);

    // Request chunks beyond what exists
    const result = reader.extractChunkRange(0, 10);
    try testing.expectError(error.RangeOutOfBounds, result);
}

test "copyTermIntoRange applies skip and truncation" {
    var buffer: [5]u8 = undefined;
    var result = buffer[0..];
    var pending_skip: usize = 3;
    var result_offset: usize = 0;
    var remaining: usize = result.len;

    const term_a = "abcdef";
    FileReconstructor.copyTermIntoRange(result, term_a, &pending_skip, &result_offset, &remaining);
    try std.testing.expectEqual(@as(usize, 0), pending_skip);
    try std.testing.expectEqual(@as(usize, 3), result_offset);
    try std.testing.expectEqual(@as(usize, 2), remaining);
    try std.testing.expectEqualSlices(u8, "def", result[0..result_offset]);

    const term_b = "ghij";
    FileReconstructor.copyTermIntoRange(result, term_b, &pending_skip, &result_offset, &remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining);
    try std.testing.expectEqualSlices(u8, "defgh", result[0..result_offset]);
}
