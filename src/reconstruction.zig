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

        const size = end - start;
        var result = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(result);

        var file_offset: u64 = start;
        var result_offset: usize = 0;

        for (recon.terms) |term| {
            const xorb_info = try self.fetchXorbForTerm(term, recon.fetch_info);
            defer self.allocator.free(xorb_info.data);

            var xorb_reader = xorb.XorbReader.init(self.allocator, xorb_info.data);
            const chunk_data = try xorb_reader.extractChunkRange(xorb_info.local_start, xorb_info.local_end);
            defer self.allocator.free(chunk_data);

            const term_start = file_offset;
            const term_end = file_offset + chunk_data.len;
            file_offset = term_end;

            const copy_start = if (term_start < start) start - term_start else 0;
            const copy_end = if (term_end > end) chunk_data.len - (term_end - end) else chunk_data.len;
            const copy_size = copy_end - copy_start;

            if (copy_size > 0) {
                @memcpy(result[result_offset..][0..copy_size], chunk_data[copy_start..copy_end]);
                result_offset += copy_size;
            }
        }

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
