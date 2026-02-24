#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../../msg/storage_node_messages.h"

namespace zb::optical_node {

struct ImageLocation {
    std::string image_id;
    uint64_t image_offset{0};
    uint64_t image_length{0};
};

class ImageStore {
public:
    ImageStore(std::string root,
               std::vector<std::string> disk_ids,
               uint64_t max_image_size_bytes,
               uint64_t disk_capacity_bytes,
               std::string mount_point_prefix);

    bool Init(std::string* error);

    zb::msg::Status WriteChunk(const std::string& disk_id,
                               const std::string& chunk_id,
                               const std::string& data,
                               ImageLocation* location);
    zb::msg::Status ReadChunk(const std::string& disk_id,
                              const std::string& chunk_id,
                              uint64_t offset,
                              uint64_t size,
                              std::string* out,
                              uint64_t* bytes_read) const;
    zb::msg::Status DeleteChunk(const std::string& disk_id, const std::string& chunk_id);
    zb::msg::DiskReportReply GetDiskReport() const;

private:
    struct ChunkRecord {
        std::string image_id;
        uint64_t offset{0};
        uint64_t length{0};
    };

    struct DiskContext {
        std::string disk_id;
        std::string root_path;
        std::string mount_point;
        std::string manifest_path;
        uint64_t capacity_bytes{0};
        uint64_t used_bytes{0};

        uint64_t next_image_index{1};
        std::string current_image_id;
        std::string current_image_path;
        uint64_t current_image_size{0};

        std::unordered_map<std::string, ChunkRecord> chunks;
    };

    static std::string BuildImageId(uint64_t index);
    static bool ParseImageIndex(const std::string& name, uint64_t* index);
    static std::string ToHex(uint64_t value, size_t width);
    static std::string BuildHashPrefix(const std::string& chunk_id);
    static std::string JoinChunkKey(const std::string& disk_id, const std::string& chunk_id);
    static std::vector<std::string> Split(const std::string& input, char delimiter);

    bool EnsureDiskContextLocked(const std::string& disk_id, DiskContext** ctx, std::string* error) const;
    bool RotateImageIfNeededLocked(DiskContext* ctx, uint64_t incoming_size, std::string* error);
    bool OpenCurrentImageLocked(DiskContext* ctx, std::fstream* stream, std::string* error) const;
    bool AppendManifestLocked(const DiskContext& ctx, const std::string& line, std::string* error) const;

    std::string root_;
    std::vector<std::string> disk_ids_;
    uint64_t max_image_size_bytes_{0};
    uint64_t disk_capacity_bytes_{0};
    std::string mount_point_prefix_;

    mutable std::mutex mu_;
    mutable std::unordered_map<std::string, DiskContext> disks_;
};

} // namespace zb::optical_node
