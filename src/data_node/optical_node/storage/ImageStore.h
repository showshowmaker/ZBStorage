#pragma once

#include <cstdint>
#include <fstream>
#include <list>
#include <mutex>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "../../../msg/storage_node_messages.h"

namespace zb::optical_node {

struct ImageLocation {
    std::string image_id;
    uint64_t image_offset{0};
    uint64_t image_length{0};
};

struct FileArchiveMeta {
    uint64_t inode_id{0};
    std::string file_id;
    std::string file_path;
    uint64_t file_size{0};
    uint64_t file_offset{0};
    uint32_t file_mode{0};
    uint32_t file_uid{0};
    uint32_t file_gid{0};
    uint64_t file_mtime{0};
    uint32_t file_chunk_index{0};
};

struct ImageFileExtent {
    std::string chunk_id;
    std::string image_id;
    uint64_t file_offset{0};
    uint64_t image_offset{0};
    uint64_t length{0};
};

struct ImageFileEntry {
    uint64_t inode_id{0};
    std::string file_id;
    std::string file_path;
    uint64_t file_size{0};
    uint32_t file_mode{0};
    uint32_t file_uid{0};
    uint32_t file_gid{0};
    uint64_t file_mtime{0};
    std::vector<ImageFileExtent> extents;
};

class ImageStore {
public:
    ImageStore(std::string root,
               std::string cache_root,
               std::vector<std::string> disk_ids,
               bool simulate_io,
               uint64_t optical_read_bytes_per_sec,
               uint64_t optical_write_bytes_per_sec,
               uint64_t cache_read_bytes_per_sec,
               uint64_t max_image_size_bytes,
               uint64_t disk_capacity_bytes,
               std::string mount_point_prefix,
               uint32_t cache_disc_slots,
               bool replay_images_on_init);

    bool Init(std::string* error);

    zb::msg::Status WriteChunk(const std::string& disk_id,
                               const std::string& chunk_id,
                               const std::string& data,
                               const FileArchiveMeta* file_meta,
                               ImageLocation* location);
    zb::msg::Status ReadChunk(const std::string& disk_id,
                              const std::string& chunk_id,
                              uint64_t offset,
                              uint64_t size,
                              const std::string& image_id_hint,
                              uint64_t image_offset_hint,
                              uint64_t image_length_hint,
                              std::string* out,
                              uint64_t* bytes_read) const;
    zb::msg::Status ReadArchivedFile(const std::string& disc_id,
                                     uint64_t inode_id,
                                     const std::string& file_id,
                                     uint64_t offset,
                                     uint64_t size,
                                     std::string* out,
                                     uint64_t* bytes_read) const;
    zb::msg::Status DeleteChunk(const std::string& disk_id, const std::string& chunk_id);
    zb::msg::DiskReportReply GetDiskReport() const;
    bool RebuildFileSystemMetadata(const std::string& disk_id,
                                   std::vector<ImageFileEntry>* files,
                                   std::string* error) const;

private:
    struct ImageRecordHeader {
        uint32_t magic{0};
        uint16_t version{0};
        uint16_t type{0};
        uint32_t meta_length{0};
        uint64_t data_length{0};
        uint32_t meta_crc32{0};
        uint32_t data_crc32{0};
    };

    struct ChunkRecord {
        std::string image_id;
        uint64_t offset{0};
        uint64_t length{0};
        FileArchiveMeta file_meta;
    };

    struct DiskContext {
        std::string disk_id;
        std::string root_path;
        std::string cache_root_path;
        std::string mount_point;
        std::string manifest_path;
        uint64_t capacity_bytes{0};
        uint64_t used_bytes{0};

        uint64_t next_image_index{1};
        std::string current_image_id;
        std::string current_image_path;
        uint64_t current_image_size{0};
        std::unordered_map<std::string, uint64_t> image_sizes;
        std::unordered_set<std::string> cached_images;
        std::list<std::string> cached_lru;
        std::unordered_map<std::string, std::list<std::string>::iterator> cached_lru_pos;

        std::unordered_map<std::string, ChunkRecord> chunks;
        std::unordered_map<std::string, ImageFileEntry> files;
    };

    static std::string BuildImageId(uint64_t index);
    static bool ParseImageIndex(const std::string& name, uint64_t* index);
    static std::string ToHex(uint64_t value, size_t width);
    static std::string BuildHashPrefix(const std::string& chunk_id);
    static std::string JoinChunkKey(const std::string& disk_id, const std::string& chunk_id);
    static void SleepByBytes(uint64_t bytes, uint64_t bytes_per_sec);
    static std::string BuildSimulatedChunkData(const std::string& chunk_id, uint64_t offset, uint64_t size);
    static std::vector<std::string> Split(const std::string& input, char delimiter);
    static uint32_t Crc32(const std::string& data);
    static void AppendU16(std::string* out, uint16_t value);
    static void AppendU32(std::string* out, uint32_t value);
    static void AppendU64(std::string* out, uint64_t value);
    static bool ReadU16(const std::string& in, size_t* cursor, uint16_t* value);
    static bool ReadU32(const std::string& in, size_t* cursor, uint32_t* value);
    static bool ReadU64(const std::string& in, size_t* cursor, uint64_t* value);
    static void AppendString(std::string* out, const std::string& value);
    static bool ReadString(const std::string& in, size_t* cursor, std::string* value);
    static std::string BuildFileId(const FileArchiveMeta* meta, uint64_t inode_id_hint, const std::string& chunk_id);
    static bool EncodeFileArchiveMeta(const FileArchiveMeta& meta,
                                      const std::string& disk_id,
                                      const std::string& chunk_id,
                                      uint64_t commit_ts_ms,
                                      std::string* out);
    static bool DecodeFileArchiveMeta(const std::string& data,
                                      FileArchiveMeta* meta,
                                      std::string* disk_id,
                                      std::string* chunk_id,
                                      uint64_t* commit_ts_ms);
    static std::string EncodeImageRecordHeader(const ImageRecordHeader& header);
    static bool DecodeImageRecordHeader(const char* bytes, size_t size, ImageRecordHeader* header);
    bool AppendRecordLocked(DiskContext* ctx,
                            const std::string& chunk_id,
                            const std::string& data,
                            const FileArchiveMeta* file_meta,
                            uint64_t* payload_offset,
                            std::string* error);
    bool ReplayImageFileLocked(DiskContext* ctx, const std::string& image_path, std::string* error);
    static void UpsertFileExtent(DiskContext* ctx,
                                 const std::string& chunk_id,
                                 const ChunkRecord& rec);

    bool EnsureDiskContextLocked(const std::string& disk_id, DiskContext** ctx, std::string* error) const;
    bool RotateImageIfNeededLocked(DiskContext* ctx, uint64_t incoming_size, std::string* error);
    bool OpenCurrentImageLocked(DiskContext* ctx, std::fstream* stream, std::string* error) const;
    bool AppendManifestLocked(const DiskContext& ctx, const std::string& line, std::string* error) const;
    void TouchCachedImageLocked(DiskContext* ctx, const std::string& image_id) const;
    void EnsureCacheSlotLocked(DiskContext* ctx) const;

    std::string root_;
    std::string cache_root_;
    std::vector<std::string> disk_ids_;
    bool simulate_io_{true};
    uint64_t optical_read_bytes_per_sec_{0};
    uint64_t optical_write_bytes_per_sec_{0};
    uint64_t cache_read_bytes_per_sec_{0};
    uint64_t max_image_size_bytes_{0};
    uint64_t disk_capacity_bytes_{0};
    std::string mount_point_prefix_;
    uint32_t cache_disc_slots_{4};
    bool replay_images_on_init_{false};

    mutable std::mutex mu_;
    mutable std::unordered_map<std::string, DiskContext> disks_;
};

} // namespace zb::optical_node
