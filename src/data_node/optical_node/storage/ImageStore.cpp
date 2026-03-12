#include "ImageStore.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <thread>
#include <unordered_set>

namespace zb::optical_node {

namespace {

namespace fs = std::filesystem;

constexpr uint32_t kImageRecordMagic = 0x4653425aU; // ZBSF
constexpr uint16_t kImageRecordVersion = 2;
constexpr uint16_t kImageRecordTypeFileData = 1;
constexpr uint16_t kFileMetaVersion = 1;
constexpr size_t kImageRecordHeaderSize = 28;

} // namespace

ImageStore::ImageStore(std::string root,
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
                       bool replay_images_on_init)
    : root_(std::move(root)),
      cache_root_(std::move(cache_root)),
      disk_ids_(std::move(disk_ids)),
      simulate_io_(simulate_io),
      optical_read_bytes_per_sec_(optical_read_bytes_per_sec == 0 ? 1 : optical_read_bytes_per_sec),
      optical_write_bytes_per_sec_(optical_write_bytes_per_sec == 0 ? 1 : optical_write_bytes_per_sec),
      cache_read_bytes_per_sec_(cache_read_bytes_per_sec == 0 ? 1 : cache_read_bytes_per_sec),
      max_image_size_bytes_(max_image_size_bytes == 0 ? (1024ULL * 1024ULL * 1024ULL) : max_image_size_bytes),
      disk_capacity_bytes_(disk_capacity_bytes == 0 ? (10ULL * 1024ULL * 1024ULL * 1024ULL) : disk_capacity_bytes),
      mount_point_prefix_(std::move(mount_point_prefix)),
      cache_disc_slots_(cache_disc_slots == 0 ? 1 : cache_disc_slots),
      replay_images_on_init_(replay_images_on_init) {
    if (cache_root_.empty()) {
        cache_root_ = (fs::path(root_) / "cache").string();
    }
}

bool ImageStore::Init(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    std::error_code ec;
    fs::create_directories(root_, ec);
    if (ec) {
        if (error) {
            *error = "Failed to create archive root: " + ec.message();
        }
        return false;
    }
    ec.clear();
    fs::create_directories(cache_root_, ec);
    if (ec) {
        if (error) {
            *error = "Failed to create cache root: " + ec.message();
        }
        return false;
    }

    for (const auto& disk_id : disk_ids_) {
        if (disk_id.empty()) {
            continue;
        }
        DiskContext ctx;
        ctx.disk_id = disk_id;
        ctx.root_path = (fs::path(root_) / disk_id).string();
        ctx.cache_root_path = (fs::path(cache_root_) / disk_id).string();
        ctx.mount_point = (fs::path(mount_point_prefix_) / disk_id).string();
        ctx.manifest_path = (fs::path(ctx.root_path) / "manifest.log").string();
        ctx.capacity_bytes = disk_capacity_bytes_;

        fs::create_directories(ctx.root_path, ec);
        if (ec) {
            if (error) {
                *error = "Failed to create disk archive dir for " + disk_id + ": " + ec.message();
            }
            return false;
        }
        ec.clear();
        fs::create_directories(ctx.cache_root_path, ec);
        if (ec) {
            if (error) {
                *error = "Failed to create cache dir for " + disk_id + ": " + ec.message();
            }
            return false;
        }

        uint64_t max_image_index = 0;
        for (const auto& entry : fs::directory_iterator(ctx.root_path, ec)) {
            if (ec) {
                break;
            }
            if (!entry.is_regular_file()) {
                continue;
            }
            std::string filename = entry.path().filename().string();
            if (entry.path().extension() == ".iso") {
                uint64_t image_index = 0;
                if (ParseImageIndex(filename, &image_index)) {
                    const uint64_t image_size = static_cast<uint64_t>(entry.file_size());
                    const std::string image_id = BuildImageId(image_index);
                    ctx.image_sizes[image_id] = image_size;
                    ctx.used_bytes += image_size;
                    ctx.next_image_index = std::max<uint64_t>(ctx.next_image_index, image_index + 1);
                    if (image_index >= max_image_index) {
                        max_image_index = image_index;
                        ctx.current_image_id = image_id;
                        ctx.current_image_path = entry.path().string();
                        ctx.current_image_size = image_size;
                    }
                }
            }
        }
        ec.clear();

        std::unordered_set<std::string> deleted_objects;
        std::ifstream manifest(ctx.manifest_path);
        uint64_t logical_used_bytes = 0;
        if (manifest) {
            std::string line;
            while (std::getline(manifest, line)) {
                std::vector<std::string> parts = Split(line, '|');
                if (parts.size() < 2) {
                    continue;
                }
                if (parts[0] == "W" && parts.size() >= 6) {
                    ObjectRecord rec;
                    rec.image_id = parts[2];
                    try {
                        rec.offset = static_cast<uint64_t>(std::stoull(parts[3]));
                        rec.length = static_cast<uint64_t>(std::stoull(parts[4]));
                        if (parts.size() >= 16) {
                            rec.file_meta.inode_id = static_cast<uint64_t>(std::stoull(parts[6]));
                            rec.file_meta.file_id = parts[7];
                            rec.file_meta.file_path = parts[8];
                            rec.file_meta.file_offset = static_cast<uint64_t>(std::stoull(parts[9]));
                            rec.file_meta.file_size = static_cast<uint64_t>(std::stoull(parts[10]));
                            rec.file_meta.file_mtime = static_cast<uint64_t>(std::stoull(parts[11]));
                            rec.file_meta.file_mode = static_cast<uint32_t>(std::stoul(parts[12]));
                            rec.file_meta.file_uid = static_cast<uint32_t>(std::stoul(parts[13]));
                            rec.file_meta.file_gid = static_cast<uint32_t>(std::stoul(parts[14]));
                            rec.file_meta.file_object_index = static_cast<uint32_t>(std::stoul(parts[15]));
                        }
                    } catch (...) {
                        continue;
                    }
                    ctx.objects[parts[1]] = rec;
                    logical_used_bytes += rec.length;
                    deleted_objects.erase(parts[1]);
                } else if (parts[0] == "D" && parts.size() >= 2) {
                    ctx.objects.erase(parts[1]);
                    deleted_objects.insert(parts[1]);
                }
            }
        }

        if (replay_images_on_init_) {
            std::vector<std::pair<uint64_t, std::string>> image_files;
            image_files.reserve(ctx.image_sizes.size());
            for (const auto& image_item : ctx.image_sizes) {
                uint64_t image_index = 0;
                if (!ParseImageIndex(image_item.first + ".iso", &image_index)) {
                    continue;
                }
                image_files.emplace_back(image_index, image_item.first);
            }
            std::sort(image_files.begin(), image_files.end());
            for (const auto& image : image_files) {
                const std::string image_path = (fs::path(ctx.root_path) / (image.second + ".iso")).string();
                std::string replay_error;
                if (!ReplayImageFileLocked(&ctx, image_path, &replay_error)) {
                    if (error) {
                        *error = replay_error;
                    }
                    return false;
                }
            }
        }

        for (const auto& deleted : deleted_objects) {
            ctx.objects.erase(deleted);
        }

        ctx.files.clear();
        for (const auto& object_item : ctx.objects) {
            UpsertFileExtent(&ctx, object_item.first, object_item.second);
            auto image_it = ctx.image_sizes.find(object_item.second.image_id);
            const uint64_t rec_end = object_item.second.offset + object_item.second.length;
            if (image_it == ctx.image_sizes.end() || rec_end > image_it->second) {
                ctx.image_sizes[object_item.second.image_id] = rec_end;
            }
        }

        if (ctx.used_bytes == 0 && logical_used_bytes > 0) {
            // Legacy simulation mode may not have image files persisted.
            ctx.used_bytes = logical_used_bytes;
        }

        if (ctx.current_image_id.empty()) {
            ctx.current_image_id = BuildImageId(1);
            ctx.current_image_path = (fs::path(ctx.root_path) / (ctx.current_image_id + ".iso")).string();
            ctx.current_image_size = 0;
            ctx.next_image_index = std::max<uint64_t>(ctx.next_image_index, 2);
            ctx.image_sizes[ctx.current_image_id] = 0;
        } else {
            auto image_it = ctx.image_sizes.find(ctx.current_image_id);
            if (image_it != ctx.image_sizes.end()) {
                ctx.current_image_size = image_it->second;
            }
            if (ctx.next_image_index == 1) {
                ctx.next_image_index = 2;
            }
        }

        disks_[ctx.disk_id] = std::move(ctx);
    }

    return true;
}

zb::msg::Status ImageStore::WriteObject(const std::string& disk_id,
                                        const std::string& object_id,
                                        const std::string& data,
                                        const FileArchiveMeta* file_meta,
                                        ImageLocation* location) {
    uint64_t write_delay_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        DiskContext* ctx = nullptr;
        std::string error;
        if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
            return zb::msg::Status::NotFound(error);
        }

        FileArchiveMeta normalized;
        if (file_meta) {
            normalized = *file_meta;
        }
        normalized.file_id = BuildFileId(file_meta, normalized.inode_id, object_id);
        if (normalized.file_path.empty() && normalized.inode_id != 0) {
            normalized.file_path = "/inode/" + std::to_string(normalized.inode_id);
        }

        std::string encoded_meta;
        const uint64_t commit_ts_ms =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count());
        if (!EncodeFileArchiveMeta(normalized, disk_id, object_id, commit_ts_ms, &encoded_meta)) {
            return zb::msg::Status::InternalError("failed to encode image metadata");
        }
        const uint64_t total_record_bytes =
            static_cast<uint64_t>(kImageRecordHeaderSize + encoded_meta.size() + data.size());

        if (ctx->capacity_bytes > 0 && ctx->used_bytes + total_record_bytes > ctx->capacity_bytes) {
            return zb::msg::Status::IoError("optical disk capacity exceeded");
        }

        if (!RotateImageIfNeededLocked(ctx, total_record_bytes, &error)) {
            return zb::msg::Status::IoError(error);
        }

        uint64_t payload_offset = 0;
        if (!AppendRecordLocked(ctx, object_id, data, &normalized, &payload_offset, &error)) {
            return zb::msg::Status::IoError(error);
        }

        ObjectRecord rec;
        rec.image_id = ctx->current_image_id;
        rec.offset = payload_offset;
        rec.length = static_cast<uint64_t>(data.size());
        rec.file_meta = normalized;
        ctx->objects[object_id] = rec;
        UpsertFileExtent(ctx, object_id, rec);
        ctx->image_sizes[rec.image_id] = std::max<uint64_t>(ctx->image_sizes[rec.image_id], ctx->current_image_size);
        ctx->used_bytes += total_record_bytes;

        std::ostringstream oss;
        oss << "W|" << object_id << "|" << rec.image_id << "|" << rec.offset << "|" << rec.length << "|" << disk_id
            << "|" << rec.file_meta.inode_id
            << "|" << rec.file_meta.file_id
            << "|" << rec.file_meta.file_path
            << "|" << rec.file_meta.file_offset
            << "|" << rec.file_meta.file_size
            << "|" << rec.file_meta.file_mtime
            << "|" << rec.file_meta.file_mode
            << "|" << rec.file_meta.file_uid
            << "|" << rec.file_meta.file_gid
            << "|" << rec.file_meta.file_object_index;
        if (!AppendManifestLocked(*ctx, oss.str(), &error)) {
            return zb::msg::Status::IoError(error);
        }

        if (location) {
            location->image_id = rec.image_id;
            location->image_offset = rec.offset;
            location->image_length = rec.length;
        }
        write_delay_bytes = rec.length;
    }

    SleepByBytes(write_delay_bytes, optical_write_bytes_per_sec_);
    return zb::msg::Status::Ok();
}

zb::msg::Status ImageStore::ReadObject(const std::string& disk_id,
                                       const std::string& object_id,
                                       uint64_t offset,
                                       uint64_t size,
                                       const std::string& image_id_hint,
                                      uint64_t image_offset_hint,
                                      uint64_t image_length_hint,
                                      std::string* out,
                                      uint64_t* bytes_read) const {
    if (!out) {
        return zb::msg::Status::InvalidArgument("output buffer is null");
    }
    if (bytes_read) {
        *bytes_read = 0;
    }

    ObjectRecord rec;
    std::string cache_image_path;
    uint64_t load_delay_bytes = 0;
    uint64_t read_delay_bytes = 0;
    bool use_simulated_payload = false;

    {
        std::lock_guard<std::mutex> lock(mu_);
        DiskContext* ctx = nullptr;
        std::string error;
        if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
            return zb::msg::Status::NotFound(error);
        }

        auto it = ctx->objects.find(object_id);
        if (!image_id_hint.empty()) {
            if (it != ctx->objects.end()) {
                rec = it->second;
            }
            rec.image_id = image_id_hint;
            rec.offset = image_offset_hint;
            if (image_length_hint > 0) {
                rec.length = image_length_hint;
            }
        } else {
            if (it == ctx->objects.end()) {
                return zb::msg::Status::NotFound("object not found");
            }
            rec = it->second;
        }

        if (rec.image_id.empty()) {
            return zb::msg::Status::NotFound("optical image location missing");
        }
        if (rec.length > 0 && offset >= rec.length) {
            out->clear();
            return zb::msg::Status::Ok();
        }

        if (size == 0) {
            out->clear();
            return zb::msg::Status::Ok();
        }
        if (rec.length > 0) {
            read_delay_bytes = std::min<uint64_t>(size, rec.length - offset);
        } else {
            read_delay_bytes = size;
        }
        if (ctx->cached_images.find(rec.image_id) == ctx->cached_images.end()) {
            uint64_t image_size = 0;
            auto image_it = ctx->image_sizes.find(rec.image_id);
            if (image_it != ctx->image_sizes.end()) {
                image_size = image_it->second;
            } else {
                image_size = rec.offset + rec.length;
            }
            load_delay_bytes = image_size;
            ctx->cached_images.insert(rec.image_id);
            TouchCachedImageLocked(ctx, rec.image_id);
            EnsureCacheSlotLocked(ctx);
        } else {
            TouchCachedImageLocked(ctx, rec.image_id);
        }
        const fs::path source_path = fs::path(ctx->root_path) / (rec.image_id + ".iso");
        std::error_code ec;
        if (!fs::exists(source_path, ec) || ec) {
            if (simulate_io_) {
                use_simulated_payload = true;
            } else {
                return zb::msg::Status::IoError("optical image missing: " + source_path.string());
            }
        }
        cache_image_path = source_path.string();
    }

    SleepByBytes(load_delay_bytes, optical_read_bytes_per_sec_);
    SleepByBytes(read_delay_bytes, cache_read_bytes_per_sec_);

    (void)use_simulated_payload;
    (void)cache_image_path;
    (void)object_id;
    (void)offset;
    out->assign(static_cast<size_t>(read_delay_bytes), 'x');
    if (bytes_read) {
        *bytes_read = static_cast<uint64_t>(out->size());
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status ImageStore::ReadArchivedFile(const std::string& disc_id,
                                             uint64_t inode_id,
                                             const std::string& file_id,
                                             uint64_t offset,
                                             uint64_t size,
                                             std::string* out,
                                             uint64_t* bytes_read) const {
    if (!out) {
        return zb::msg::Status::InvalidArgument("output buffer is null");
    }
    if (bytes_read) {
        *bytes_read = 0;
    }
    if (size == 0) {
        out->clear();
        return zb::msg::Status::Ok();
    }

    struct Segment {
        std::string image_path;
        uint64_t image_offset{0};
        uint64_t length{0};
        uint64_t dst_offset{0};
    };

    std::string effective_file_id = file_id;
    std::vector<Segment> segments;
    uint64_t load_delay_bytes = 0;
    uint64_t read_delay_bytes = 0;
    uint64_t response_size = 0;

    {
        std::lock_guard<std::mutex> lock(mu_);
        DiskContext* ctx = nullptr;
        std::string error;
        if (!EnsureDiskContextLocked(disc_id, &ctx, &error)) {
            return zb::msg::Status::NotFound(error);
        }

        const ImageFileEntry* entry = nullptr;
        if (!effective_file_id.empty()) {
            auto it = ctx->files.find(effective_file_id);
            if (it != ctx->files.end()) {
                entry = &it->second;
            }
        }
        if (!entry && inode_id != 0) {
            auto inode_it = ctx->files.find("inode-" + std::to_string(inode_id));
            if (inode_it != ctx->files.end()) {
                effective_file_id = inode_it->first;
                entry = &inode_it->second;
            }
        }
        if (!entry && inode_id != 0) {
            for (const auto& item : ctx->files) {
                if (item.second.inode_id == inode_id) {
                    effective_file_id = item.first;
                    entry = &item.second;
                    break;
                }
            }
        }
        if (!entry) {
            return zb::msg::Status::NotFound("archived file not found on disc");
        }
        if (offset >= entry->file_size) {
            out->clear();
            return zb::msg::Status::Ok();
        }

        response_size = std::min<uint64_t>(size, entry->file_size - offset);
        read_delay_bytes = response_size;
        const uint64_t req_start = offset;
        const uint64_t req_end = offset + response_size;
        segments.reserve(entry->extents.size());

        std::unordered_set<std::string> touched;
        for (const auto& extent : entry->extents) {
            const uint64_t ext_start = extent.file_offset;
            const uint64_t ext_end = extent.file_offset + extent.length;
            const uint64_t overlap_start = std::max<uint64_t>(ext_start, req_start);
            const uint64_t overlap_end = std::min<uint64_t>(ext_end, req_end);
            if (overlap_start >= overlap_end) {
                continue;
            }

            if (touched.insert(extent.image_id).second) {
                if (ctx->cached_images.find(extent.image_id) == ctx->cached_images.end()) {
                    uint64_t image_size = 0;
                    auto image_it = ctx->image_sizes.find(extent.image_id);
                    if (image_it != ctx->image_sizes.end()) {
                        image_size = image_it->second;
                    } else {
                        image_size = extent.image_offset + extent.length;
                    }
                    load_delay_bytes += image_size;
                    ctx->cached_images.insert(extent.image_id);
                }
                TouchCachedImageLocked(ctx, extent.image_id);
                EnsureCacheSlotLocked(ctx);
            } else {
                TouchCachedImageLocked(ctx, extent.image_id);
            }

            Segment seg;
            seg.image_path = (fs::path(ctx->root_path) / (extent.image_id + ".iso")).string();
            seg.image_offset = extent.image_offset + (overlap_start - ext_start);
            seg.length = overlap_end - overlap_start;
            seg.dst_offset = overlap_start - req_start;
            segments.push_back(std::move(seg));
        }
    }

    SleepByBytes(load_delay_bytes, optical_read_bytes_per_sec_);
    SleepByBytes(read_delay_bytes, cache_read_bytes_per_sec_);

    (void)segments;
    (void)effective_file_id;
    out->assign(static_cast<size_t>(response_size), 'x');

    if (bytes_read) {
        *bytes_read = response_size;
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status ImageStore::DeleteObject(const std::string& disk_id, const std::string& object_id) {
    std::lock_guard<std::mutex> lock(mu_);
    DiskContext* ctx = nullptr;
    std::string error;
    if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
        return zb::msg::Status::NotFound(error);
    }
    auto it = ctx->objects.find(object_id);
    if (it == ctx->objects.end()) {
        return zb::msg::Status::NotFound("object not found");
    }
    ctx->objects.erase(it);
    for (auto file_it = ctx->files.begin(); file_it != ctx->files.end();) {
        auto& extents = file_it->second.extents;
        extents.erase(std::remove_if(extents.begin(),
                                     extents.end(),
                                     [&](const ImageFileExtent& extent) { return extent.object_id == object_id; }),
                      extents.end());
        if (extents.empty()) {
            file_it = ctx->files.erase(file_it);
        } else {
            ++file_it;
        }
    }
    if (!AppendManifestLocked(*ctx, "D|" + object_id, &error)) {
        return zb::msg::Status::IoError(error);
    }
    return zb::msg::Status::Ok();
}

zb::msg::DiskReportReply ImageStore::GetDiskReport() const {
    std::lock_guard<std::mutex> lock(mu_);
    zb::msg::DiskReportReply reply;
    for (const auto& item : disks_) {
        const DiskContext& ctx = item.second;
        zb::msg::DiskReport report;
        report.id = ctx.disk_id;
        report.mount_point = ctx.mount_point;
        report.capacity_bytes = ctx.capacity_bytes;
        report.free_bytes = ctx.capacity_bytes > ctx.used_bytes ? (ctx.capacity_bytes - ctx.used_bytes) : 0;
        report.is_healthy = true;
        reply.reports.push_back(std::move(report));
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

bool ImageStore::RebuildFileSystemMetadata(const std::string& disk_id,
                                           std::vector<ImageFileEntry>* files,
                                           std::string* error) const {
    if (!files) {
        if (error) {
            *error = "files output is null";
        }
        return false;
    }
    files->clear();

    std::lock_guard<std::mutex> lock(mu_);
    DiskContext* ctx = nullptr;
    if (!EnsureDiskContextLocked(disk_id, &ctx, error)) {
        return false;
    }

    files->reserve(ctx->files.size());
    for (const auto& item : ctx->files) {
        files->push_back(item.second);
    }
    std::sort(files->begin(), files->end(), [](const ImageFileEntry& lhs, const ImageFileEntry& rhs) {
        if (lhs.file_path != rhs.file_path) {
            return lhs.file_path < rhs.file_path;
        }
        return lhs.file_id < rhs.file_id;
    });
    return true;
}

std::string ImageStore::BuildImageId(uint64_t index) {
    return "image_" + std::to_string(index);
}

bool ImageStore::ParseImageIndex(const std::string& name, uint64_t* index) {
    if (!index) {
        return false;
    }
    const std::string prefix = "image_";
    const std::string suffix = ".iso";
    if (name.size() <= prefix.size() + suffix.size()) {
        return false;
    }
    if (name.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }
    std::string number = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
    try {
        *index = static_cast<uint64_t>(std::stoull(number));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

std::string ImageStore::ToHex(uint64_t value, size_t width) {
    static const char kHex[] = "0123456789abcdef";
    std::string out(width, '0');
    for (size_t i = 0; i < width; ++i) {
        size_t shift = (width - 1 - i) * 4;
        out[i] = kHex[(value >> shift) & 0xF];
    }
    return out;
}

std::string ImageStore::BuildHashPrefix(const std::string& object_id) {
    std::hash<std::string> hasher;
    return ToHex(static_cast<uint64_t>(hasher(object_id)), 8);
}

std::string ImageStore::JoinObjectKey(const std::string& disk_id, const std::string& object_id) {
    return disk_id + "/" + object_id;
}

void ImageStore::SleepByBytes(uint64_t bytes, uint64_t bytes_per_sec) {
    if (bytes == 0 || bytes_per_sec == 0) {
        return;
    }
    const uint64_t ns_per_sec = 1000000000ULL;
    const uint64_t duration_ns = (bytes * ns_per_sec + bytes_per_sec - 1) / bytes_per_sec;
    std::this_thread::sleep_for(std::chrono::nanoseconds(duration_ns));
}

std::string ImageStore::BuildSimulatedObjectData(const std::string& object_id, uint64_t offset, uint64_t size) {
    if (size == 0) {
        return {};
    }
    std::string data(static_cast<size_t>(size), '\0');
    std::hash<std::string> hasher;
    const uint64_t seed = static_cast<uint64_t>(hasher(object_id));
    for (uint64_t i = 0; i < size; ++i) {
        const uint64_t v = seed + offset + i;
        data[static_cast<size_t>(i)] = static_cast<char>('a' + (v % 26));
    }
    return data;
}

std::vector<std::string> ImageStore::Split(const std::string& input, char delimiter) {
    std::vector<std::string> out;
    std::string token;
    std::istringstream stream(input);
    while (std::getline(stream, token, delimiter)) {
        out.push_back(token);
    }
    return out;
}

uint32_t ImageStore::Crc32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFFU;
    for (unsigned char byte : data) {
        crc ^= static_cast<uint32_t>(byte);
        for (int i = 0; i < 8; ++i) {
            const uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0xEDB88320U & mask);
        }
    }
    return ~crc;
}

void ImageStore::AppendU16(std::string* out, uint16_t value) {
    if (!out) {
        return;
    }
    out->push_back(static_cast<char>(value & 0xFF));
    out->push_back(static_cast<char>((value >> 8) & 0xFF));
}

void ImageStore::AppendU32(std::string* out, uint32_t value) {
    if (!out) {
        return;
    }
    out->push_back(static_cast<char>(value & 0xFF));
    out->push_back(static_cast<char>((value >> 8) & 0xFF));
    out->push_back(static_cast<char>((value >> 16) & 0xFF));
    out->push_back(static_cast<char>((value >> 24) & 0xFF));
}

void ImageStore::AppendU64(std::string* out, uint64_t value) {
    if (!out) {
        return;
    }
    for (int i = 0; i < 8; ++i) {
        out->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
    }
}

bool ImageStore::ReadU16(const std::string& in, size_t* cursor, uint16_t* value) {
    if (!cursor || !value || *cursor + sizeof(uint16_t) > in.size()) {
        return false;
    }
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(in.data() + *cursor);
    *value = static_cast<uint16_t>(ptr[0]) | (static_cast<uint16_t>(ptr[1]) << 8);
    *cursor += sizeof(uint16_t);
    return true;
}

bool ImageStore::ReadU32(const std::string& in, size_t* cursor, uint32_t* value) {
    if (!cursor || !value || *cursor + sizeof(uint32_t) > in.size()) {
        return false;
    }
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(in.data() + *cursor);
    *value = static_cast<uint32_t>(ptr[0]) |
             (static_cast<uint32_t>(ptr[1]) << 8) |
             (static_cast<uint32_t>(ptr[2]) << 16) |
             (static_cast<uint32_t>(ptr[3]) << 24);
    *cursor += sizeof(uint32_t);
    return true;
}

bool ImageStore::ReadU64(const std::string& in, size_t* cursor, uint64_t* value) {
    if (!cursor || !value || *cursor + sizeof(uint64_t) > in.size()) {
        return false;
    }
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(in.data() + *cursor);
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= (static_cast<uint64_t>(ptr[i]) << (8 * i));
    }
    *value = v;
    *cursor += sizeof(uint64_t);
    return true;
}

void ImageStore::AppendString(std::string* out, const std::string& value) {
    if (!out) {
        return;
    }
    AppendU32(out, static_cast<uint32_t>(value.size()));
    out->append(value);
}

bool ImageStore::ReadString(const std::string& in, size_t* cursor, std::string* value) {
    if (!cursor || !value) {
        return false;
    }
    uint32_t length = 0;
    if (!ReadU32(in, cursor, &length)) {
        return false;
    }
    if (*cursor + length > in.size()) {
        return false;
    }
    value->assign(in.data() + *cursor, in.data() + *cursor + length);
    *cursor += length;
    return true;
}

std::string ImageStore::BuildFileId(const FileArchiveMeta* meta,
                                    uint64_t inode_id_hint,
                                    const std::string& object_id) {
    if (meta && !meta->file_id.empty()) {
        return meta->file_id;
    }
    uint64_t inode_id = inode_id_hint;
    if (inode_id == 0 && meta) {
        inode_id = meta->inode_id;
    }
    if (inode_id != 0) {
        return "inode-" + std::to_string(inode_id);
    }
    return "object-" + object_id;
}

bool ImageStore::EncodeFileArchiveMeta(const FileArchiveMeta& meta,
                                       const std::string& disk_id,
                                       const std::string& object_id,
                                       uint64_t commit_ts_ms,
                                       std::string* out) {
    if (!out) {
        return false;
    }
    out->clear();

    AppendU16(out, kFileMetaVersion);
    AppendU64(out, commit_ts_ms);
    AppendU64(out, meta.inode_id);
    AppendU64(out, meta.file_size);
    AppendU64(out, meta.file_offset);
    AppendU32(out, meta.file_mode);
    AppendU32(out, meta.file_uid);
    AppendU32(out, meta.file_gid);
    AppendU64(out, meta.file_mtime);
    AppendU32(out, meta.file_object_index);
    AppendString(out, disk_id);
    AppendString(out, object_id);
    AppendString(out, meta.file_id);
    AppendString(out, meta.file_path);
    return true;
}

bool ImageStore::DecodeFileArchiveMeta(const std::string& data,
                                       FileArchiveMeta* meta,
                                       std::string* disk_id,
                                       std::string* object_id,
                                       uint64_t* commit_ts_ms) {
    if (!meta) {
        return false;
    }
    *meta = FileArchiveMeta{};
    if (disk_id) {
        disk_id->clear();
    }
    if (object_id) {
        object_id->clear();
    }
    if (commit_ts_ms) {
        *commit_ts_ms = 0;
    }

    size_t cursor = 0;
    uint16_t version = 0;
    if (!ReadU16(data, &cursor, &version) || version != kFileMetaVersion) {
        return false;
    }
    uint64_t commit_ts = 0;
    if (!ReadU64(data, &cursor, &commit_ts)) {
        return false;
    }
    if (commit_ts_ms) {
        *commit_ts_ms = commit_ts;
    }
    if (!ReadU64(data, &cursor, &meta->inode_id) ||
        !ReadU64(data, &cursor, &meta->file_size) ||
        !ReadU64(data, &cursor, &meta->file_offset) ||
        !ReadU32(data, &cursor, &meta->file_mode) ||
        !ReadU32(data, &cursor, &meta->file_uid) ||
        !ReadU32(data, &cursor, &meta->file_gid) ||
        !ReadU64(data, &cursor, &meta->file_mtime) ||
        !ReadU32(data, &cursor, &meta->file_object_index)) {
        return false;
    }

    std::string parsed_disk_id;
    std::string parsed_object_id;
    if (!ReadString(data, &cursor, &parsed_disk_id) ||
        !ReadString(data, &cursor, &parsed_object_id) ||
        !ReadString(data, &cursor, &meta->file_id) ||
        !ReadString(data, &cursor, &meta->file_path)) {
        return false;
    }
    if (cursor != data.size()) {
        return false;
    }
    if (disk_id) {
        *disk_id = std::move(parsed_disk_id);
    }
    if (object_id) {
        *object_id = std::move(parsed_object_id);
    }
    return true;
}

std::string ImageStore::EncodeImageRecordHeader(const ImageRecordHeader& header) {
    std::string out;
    out.reserve(kImageRecordHeaderSize);
    AppendU32(&out, header.magic);
    AppendU16(&out, header.version);
    AppendU16(&out, header.type);
    AppendU32(&out, header.meta_length);
    AppendU64(&out, header.data_length);
    AppendU32(&out, header.meta_crc32);
    AppendU32(&out, header.data_crc32);
    return out;
}

bool ImageStore::DecodeImageRecordHeader(const char* bytes, size_t size, ImageRecordHeader* header) {
    if (!bytes || !header || size != kImageRecordHeaderSize) {
        return false;
    }
    std::string buf(bytes, size);
    size_t cursor = 0;
    if (!ReadU32(buf, &cursor, &header->magic) ||
        !ReadU16(buf, &cursor, &header->version) ||
        !ReadU16(buf, &cursor, &header->type) ||
        !ReadU32(buf, &cursor, &header->meta_length) ||
        !ReadU64(buf, &cursor, &header->data_length) ||
        !ReadU32(buf, &cursor, &header->meta_crc32) ||
        !ReadU32(buf, &cursor, &header->data_crc32)) {
        return false;
    }
    return cursor == size;
}

bool ImageStore::AppendRecordLocked(DiskContext* ctx,
                                    const std::string& object_id,
                                    const std::string& data,
                                    const FileArchiveMeta* file_meta,
                                    uint64_t* payload_offset,
                                    std::string* error) {
    if (!ctx || !payload_offset) {
        if (error) {
            *error = "invalid append record args";
        }
        return false;
    }

    FileArchiveMeta meta;
    if (file_meta) {
        meta = *file_meta;
    }
    meta.file_id = BuildFileId(file_meta, meta.inode_id, object_id);
    if (meta.file_path.empty() && meta.inode_id != 0) {
        meta.file_path = "/inode/" + std::to_string(meta.inode_id);
    }

    const uint64_t commit_ts_ms =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count());
    std::string encoded_meta;
    if (!EncodeFileArchiveMeta(meta, ctx->disk_id, object_id, commit_ts_ms, &encoded_meta)) {
        if (error) {
            *error = "failed to encode file metadata";
        }
        return false;
    }

    ImageRecordHeader header;
    header.magic = kImageRecordMagic;
    header.version = kImageRecordVersion;
    header.type = kImageRecordTypeFileData;
    header.meta_length = static_cast<uint32_t>(encoded_meta.size());
    header.data_length = static_cast<uint64_t>(data.size());
    header.meta_crc32 = Crc32(encoded_meta);
    header.data_crc32 = Crc32(data);

    std::fstream stream;
    if (!OpenCurrentImageLocked(ctx, &stream, error)) {
        return false;
    }
    stream.seekp(0, std::ios::end);
    if (!stream.good()) {
        if (error) {
            *error = "failed to seek image tail";
        }
        return false;
    }

    const uint64_t record_begin = static_cast<uint64_t>(stream.tellp());
    const std::string header_bytes = EncodeImageRecordHeader(header);
    stream.write(header_bytes.data(), static_cast<std::streamsize>(header_bytes.size()));
    stream.write(encoded_meta.data(), static_cast<std::streamsize>(encoded_meta.size()));
    if (!data.empty()) {
        stream.write(data.data(), static_cast<std::streamsize>(data.size()));
    }
    if (!stream.good()) {
        if (error) {
            *error = "failed to append image record";
        }
        return false;
    }
    stream.flush();

    *payload_offset = record_begin + static_cast<uint64_t>(header_bytes.size()) + static_cast<uint64_t>(encoded_meta.size());
    ctx->current_image_size = *payload_offset + static_cast<uint64_t>(data.size());
    return true;
}

bool ImageStore::ReplayImageFileLocked(DiskContext* ctx, const std::string& image_path, std::string* error) {
    if (!ctx) {
        if (error) {
            *error = "disk context is null";
        }
        return false;
    }
    std::error_code ec;
    if (!fs::exists(image_path, ec) || ec) {
        return true;
    }

    const std::string filename = fs::path(image_path).filename().string();
    uint64_t image_index = 0;
    if (!ParseImageIndex(filename, &image_index)) {
        return true;
    }
    const std::string image_id = BuildImageId(image_index);

    std::ifstream input(image_path, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open image for replay: " + image_path;
        }
        return false;
    }

    uint64_t cursor = 0;
    while (true) {
        char header_bytes[kImageRecordHeaderSize];
        input.read(header_bytes, static_cast<std::streamsize>(kImageRecordHeaderSize));
        const std::streamsize got = input.gcount();
        if (got == 0) {
            break;
        }
        if (got != static_cast<std::streamsize>(kImageRecordHeaderSize)) {
            break;
        }

        ImageRecordHeader header;
        if (!DecodeImageRecordHeader(header_bytes, kImageRecordHeaderSize, &header) ||
            header.magic != kImageRecordMagic ||
            header.version != kImageRecordVersion ||
            header.type != kImageRecordTypeFileData) {
            break;
        }

        cursor += kImageRecordHeaderSize;
        std::string meta_payload(static_cast<size_t>(header.meta_length), '\0');
        if (header.meta_length > 0) {
            input.read(meta_payload.data(), static_cast<std::streamsize>(header.meta_length));
            if (input.gcount() != static_cast<std::streamsize>(header.meta_length)) {
                break;
            }
        }

        const uint64_t payload_offset = cursor + static_cast<uint64_t>(header.meta_length);
        cursor = payload_offset;
        if (header.data_length > 0) {
            input.seekg(static_cast<std::streamoff>(header.data_length), std::ios::cur);
            if (!input.good()) {
                break;
            }
            cursor += header.data_length;
        }
        if (Crc32(meta_payload) != header.meta_crc32) {
            continue;
        }

        FileArchiveMeta file_meta;
        std::string meta_disk_id;
        std::string meta_object_id;
        uint64_t commit_ts_ms = 0;
        if (!DecodeFileArchiveMeta(meta_payload, &file_meta, &meta_disk_id, &meta_object_id, &commit_ts_ms)) {
            continue;
        }
        (void)commit_ts_ms;
        if (!meta_disk_id.empty() && meta_disk_id != ctx->disk_id) {
            continue;
        }
        if (meta_object_id.empty()) {
            continue;
        }

        ObjectRecord rec;
        rec.image_id = image_id;
        rec.offset = payload_offset;
        rec.length = header.data_length;
        rec.file_meta = file_meta;
        ctx->objects[meta_object_id] = rec;
    }
    return true;
}

void ImageStore::UpsertFileExtent(DiskContext* ctx,
                                  const std::string& object_id,
                                  const ObjectRecord& rec) {
    if (!ctx || object_id.empty()) {
        return;
    }
    for (auto file_it = ctx->files.begin(); file_it != ctx->files.end();) {
        auto& extents = file_it->second.extents;
        extents.erase(std::remove_if(extents.begin(),
                                     extents.end(),
                                     [&](const ImageFileExtent& extent) { return extent.object_id == object_id; }),
                      extents.end());
        if (extents.empty()) {
            file_it = ctx->files.erase(file_it);
        } else {
            ++file_it;
        }
    }

    const bool has_file_identity = rec.file_meta.inode_id != 0 ||
                                   !rec.file_meta.file_id.empty() ||
                                   !rec.file_meta.file_path.empty();
    if (!has_file_identity) {
        return;
    }

    std::string file_id = rec.file_meta.file_id;
    if (file_id.empty()) {
        file_id = BuildFileId(&rec.file_meta, rec.file_meta.inode_id, object_id);
    }

    ImageFileEntry& entry = ctx->files[file_id];
    entry.file_id = file_id;
    if (rec.file_meta.inode_id != 0) {
        entry.inode_id = rec.file_meta.inode_id;
    }
    if (!rec.file_meta.file_path.empty()) {
        entry.file_path = rec.file_meta.file_path;
    } else if (entry.file_path.empty() && entry.inode_id != 0) {
        entry.file_path = "/inode/" + std::to_string(entry.inode_id);
    }
    if (rec.file_meta.file_size != 0) {
        entry.file_size = rec.file_meta.file_size;
    }
    if (rec.file_meta.file_mode != 0) {
        entry.file_mode = rec.file_meta.file_mode;
    }
    if (rec.file_meta.file_uid != 0) {
        entry.file_uid = rec.file_meta.file_uid;
    }
    if (rec.file_meta.file_gid != 0) {
        entry.file_gid = rec.file_meta.file_gid;
    }
    if (rec.file_meta.file_mtime != 0) {
        entry.file_mtime = rec.file_meta.file_mtime;
    }

    ImageFileExtent extent;
    extent.object_id = object_id;
    extent.image_id = rec.image_id;
    extent.file_offset = rec.file_meta.file_offset;
    extent.image_offset = rec.offset;
    extent.length = rec.length;
    entry.extents.push_back(std::move(extent));
    std::sort(entry.extents.begin(), entry.extents.end(), [](const ImageFileExtent& lhs, const ImageFileExtent& rhs) {
        if (lhs.file_offset != rhs.file_offset) {
            return lhs.file_offset < rhs.file_offset;
        }
        return lhs.object_id < rhs.object_id;
    });
}

bool ImageStore::EnsureDiskContextLocked(const std::string& disk_id,
                                         DiskContext** ctx,
                                         std::string* error) const {
    if (!ctx) {
        return false;
    }
    auto it = disks_.find(disk_id);
    if (it == disks_.end()) {
        if (error) {
            *error = "unknown disk_id: " + disk_id;
        }
        return false;
    }
    *ctx = const_cast<DiskContext*>(&it->second);
    return true;
}

bool ImageStore::RotateImageIfNeededLocked(DiskContext* ctx, uint64_t incoming_size, std::string* error) {
    if (!ctx) {
        if (error) {
            *error = "disk context is null";
        }
        return false;
    }
    if (ctx->current_image_id.empty()) {
        ctx->current_image_id = BuildImageId(1);
        ctx->current_image_path = (fs::path(ctx->root_path) / (ctx->current_image_id + ".iso")).string();
        auto it = ctx->image_sizes.find(ctx->current_image_id);
        ctx->current_image_size = (it == ctx->image_sizes.end()) ? 0 : it->second;
        ctx->next_image_index = std::max<uint64_t>(ctx->next_image_index, 2);
        ctx->image_sizes[ctx->current_image_id] = ctx->current_image_size;
        return true;
    }
    if (ctx->current_image_size + incoming_size <= max_image_size_bytes_) {
        return true;
    }

    const uint64_t next_index = ctx->next_image_index++;
    ctx->current_image_id = BuildImageId(next_index);
    ctx->current_image_path = (fs::path(ctx->root_path) / (ctx->current_image_id + ".iso")).string();
    auto it = ctx->image_sizes.find(ctx->current_image_id);
    ctx->current_image_size = (it == ctx->image_sizes.end()) ? 0 : it->second;
    ctx->image_sizes[ctx->current_image_id] = ctx->current_image_size;
    return true;
}

bool ImageStore::OpenCurrentImageLocked(DiskContext* ctx, std::fstream* stream, std::string* error) const {
    if (!ctx || !stream) {
        if (error) {
            *error = "invalid image stream argument";
        }
        return false;
    }

    stream->open(ctx->current_image_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!stream->is_open()) {
        stream->clear();
        stream->open(ctx->current_image_path, std::ios::out | std::ios::binary);
        if (!stream->is_open()) {
            if (error) {
                *error = "failed to create optical image: " + ctx->current_image_path;
            }
            return false;
        }
        stream->close();
        stream->open(ctx->current_image_path, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!stream->is_open()) {
        if (error) {
            *error = "failed to open optical image: " + ctx->current_image_path;
        }
        return false;
    }
    return true;
}

bool ImageStore::AppendManifestLocked(const DiskContext& ctx, const std::string& line, std::string* error) const {
    std::ofstream out(ctx.manifest_path, std::ios::out | std::ios::app);
    if (!out) {
        if (error) {
            *error = "failed to open manifest: " + ctx.manifest_path;
        }
        return false;
    }
    out << line << "\n";
    if (!out.good()) {
        if (error) {
            *error = "failed to write manifest: " + ctx.manifest_path;
        }
        return false;
    }
    return true;
}

void ImageStore::TouchCachedImageLocked(DiskContext* ctx, const std::string& image_id) const {
    if (!ctx || image_id.empty()) {
        return;
    }
    auto it = ctx->cached_lru_pos.find(image_id);
    if (it != ctx->cached_lru_pos.end()) {
        ctx->cached_lru.erase(it->second);
    }
    ctx->cached_lru.push_back(image_id);
    auto lru_it = ctx->cached_lru.end();
    --lru_it;
    ctx->cached_lru_pos[image_id] = lru_it;
}

void ImageStore::EnsureCacheSlotLocked(DiskContext* ctx) const {
    if (!ctx) {
        return;
    }
    const size_t slots = static_cast<size_t>(std::max<uint32_t>(1, cache_disc_slots_));
    while (ctx->cached_images.size() > slots && !ctx->cached_lru.empty()) {
        const std::string evict_id = ctx->cached_lru.front();
        ctx->cached_lru.pop_front();
        ctx->cached_lru_pos.erase(evict_id);
        ctx->cached_images.erase(evict_id);
    }
}

} // namespace zb::optical_node
