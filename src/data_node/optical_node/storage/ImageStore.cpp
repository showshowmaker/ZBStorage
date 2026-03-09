#include "ImageStore.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <thread>

namespace zb::optical_node {

namespace {

namespace fs = std::filesystem;

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
                       std::string mount_point_prefix)
    : root_(std::move(root)),
      cache_root_(std::move(cache_root)),
      disk_ids_(std::move(disk_ids)),
      simulate_io_(simulate_io),
      optical_read_bytes_per_sec_(optical_read_bytes_per_sec == 0 ? 1 : optical_read_bytes_per_sec),
      optical_write_bytes_per_sec_(optical_write_bytes_per_sec == 0 ? 1 : optical_write_bytes_per_sec),
      cache_read_bytes_per_sec_(cache_read_bytes_per_sec == 0 ? 1 : cache_read_bytes_per_sec),
      max_image_size_bytes_(max_image_size_bytes == 0 ? (1024ULL * 1024ULL * 1024ULL) : max_image_size_bytes),
      disk_capacity_bytes_(disk_capacity_bytes == 0 ? (10ULL * 1024ULL * 1024ULL * 1024ULL) : disk_capacity_bytes),
      mount_point_prefix_(std::move(mount_point_prefix)) {
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
        ctx.mount_point = fs::path(mount_point_prefix_) / disk_id;
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

        std::ifstream manifest(ctx.manifest_path);
        uint64_t logical_used_bytes = 0;
        if (manifest) {
            std::string line;
            while (std::getline(manifest, line)) {
                std::vector<std::string> parts = Split(line, '|');
                if (parts.size() < 3) {
                    continue;
                }
                if (parts[0] == "W" && parts.size() == 6) {
                    ChunkRecord rec;
                    rec.image_id = parts[2];
                    rec.offset = static_cast<uint64_t>(std::stoull(parts[3]));
                    rec.length = static_cast<uint64_t>(std::stoull(parts[4]));
                    ctx.chunks[parts[1]] = rec;
                    logical_used_bytes += rec.length;
                    auto image_it = ctx.image_sizes.find(rec.image_id);
                    const uint64_t rec_end = rec.offset + rec.length;
                    if (image_it == ctx.image_sizes.end() || rec_end > image_it->second) {
                        ctx.image_sizes[rec.image_id] = rec_end;
                    }
                } else if (parts[0] == "D" && parts.size() >= 2) {
                    ctx.chunks.erase(parts[1]);
                }
            }
        }

        if (ctx.used_bytes == 0 && logical_used_bytes > 0) {
            // Simulation mode may not have real image files; keep logical usage for scheduling/report.
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

zb::msg::Status ImageStore::WriteChunk(const std::string& disk_id,
                                       const std::string& chunk_id,
                                       const std::string& data,
                                       ImageLocation* location) {
    uint64_t write_delay_bytes = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        DiskContext* ctx = nullptr;
        std::string error;
        if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
            return zb::msg::Status::NotFound(error);
        }

        if (!RotateImageIfNeededLocked(ctx, static_cast<uint64_t>(data.size()), &error)) {
            return zb::msg::Status::IoError(error);
        }

        uint64_t offset = ctx->current_image_size;
        if (!simulate_io_) {
            std::fstream stream;
            if (!OpenCurrentImageLocked(ctx, &stream, &error)) {
                return zb::msg::Status::IoError(error);
            }
            stream.seekp(0, std::ios::end);
            if (!stream.good()) {
                return zb::msg::Status::IoError("Failed to seek image tail");
            }
            offset = static_cast<uint64_t>(stream.tellp());
            stream.write(data.data(), static_cast<std::streamsize>(data.size()));
            if (!stream.good()) {
                return zb::msg::Status::IoError("Failed to append image data");
            }
            stream.flush();
        }

        ChunkRecord rec;
        rec.image_id = ctx->current_image_id;
        rec.offset = offset;
        rec.length = static_cast<uint64_t>(data.size());
        ctx->chunks[chunk_id] = rec;
        ctx->current_image_size = offset + rec.length;
        ctx->image_sizes[rec.image_id] = std::max<uint64_t>(ctx->image_sizes[rec.image_id], ctx->current_image_size);
        ctx->used_bytes += rec.length;

        std::ostringstream oss;
        oss << "W|" << chunk_id << "|" << rec.image_id << "|" << rec.offset << "|" << rec.length << "|" << disk_id;
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

zb::msg::Status ImageStore::ReadChunk(const std::string& disk_id,
                                      const std::string& chunk_id,
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

    ChunkRecord rec;
    std::string cache_image_path;
    uint64_t load_delay_bytes = 0;
    uint64_t read_delay_bytes = 0;
    bool simulate = true;

    {
        std::lock_guard<std::mutex> lock(mu_);
        DiskContext* ctx = nullptr;
        std::string error;
        if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
            return zb::msg::Status::NotFound(error);
        }

        auto it = ctx->chunks.find(chunk_id);
        if (it == ctx->chunks.end()) {
            return zb::msg::Status::NotFound("chunk not found");
        }
        rec = it->second;
        simulate = simulate_io_;
        if (offset >= rec.length) {
            out->clear();
            return zb::msg::Status::Ok();
        }

        read_delay_bytes = std::min<uint64_t>(size, rec.length - offset);
        if (ctx->cached_images.find(rec.image_id) == ctx->cached_images.end()) {
            uint64_t image_size = 0;
            auto image_it = ctx->image_sizes.find(rec.image_id);
            if (image_it != ctx->image_sizes.end()) {
                image_size = image_it->second;
            } else {
                image_size = rec.offset + rec.length;
            }
            load_delay_bytes = image_size;

            if (!simulate_io_) {
                const fs::path source_path = fs::path(ctx->root_path) / (rec.image_id + ".iso");
                const fs::path cache_path = fs::path(ctx->cache_root_path) / (rec.image_id + ".iso");
                std::error_code ec;
                if (!fs::exists(source_path, ec) || ec) {
                    return zb::msg::Status::IoError("optical image missing: " + source_path.string());
                }
                ec.clear();
                fs::copy_file(source_path, cache_path, fs::copy_options::overwrite_existing, ec);
                if (ec) {
                    return zb::msg::Status::IoError("failed to cache optical image: " + ec.message());
                }
            }
            ctx->cached_images.insert(rec.image_id);
        }

        cache_image_path = (fs::path(ctx->cache_root_path) / (rec.image_id + ".iso")).string();
    }

    SleepByBytes(load_delay_bytes, optical_read_bytes_per_sec_);
    SleepByBytes(read_delay_bytes, cache_read_bytes_per_sec_);

    if (simulate) {
        *out = BuildSimulatedChunkData(chunk_id, offset, read_delay_bytes);
        if (bytes_read) {
            *bytes_read = static_cast<uint64_t>(out->size());
        }
        return zb::msg::Status::Ok();
    }

    std::ifstream input(cache_image_path, std::ios::binary);
    if (!input) {
        return zb::msg::Status::IoError("failed to open cached image");
    }
    input.seekg(static_cast<std::streamoff>(rec.offset + offset));
    if (!input.good()) {
        return zb::msg::Status::IoError("failed to seek cached image");
    }

    out->assign(static_cast<size_t>(read_delay_bytes), '\0');
    input.read(out->data(), static_cast<std::streamsize>(read_delay_bytes));
    std::streamsize got = input.gcount();
    if (got < 0) {
        return zb::msg::Status::IoError("failed to read cached image");
    }
    out->resize(static_cast<size_t>(got));
    if (bytes_read) {
        *bytes_read = static_cast<uint64_t>(got);
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status ImageStore::DeleteChunk(const std::string& disk_id, const std::string& chunk_id) {
    std::lock_guard<std::mutex> lock(mu_);
    DiskContext* ctx = nullptr;
    std::string error;
    if (!EnsureDiskContextLocked(disk_id, &ctx, &error)) {
        return zb::msg::Status::NotFound(error);
    }
    auto it = ctx->chunks.find(chunk_id);
    if (it == ctx->chunks.end()) {
        return zb::msg::Status::NotFound("chunk not found");
    }
    ctx->chunks.erase(it);
    if (!AppendManifestLocked(*ctx, "D|" + chunk_id, &error)) {
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

std::string ImageStore::BuildHashPrefix(const std::string& chunk_id) {
    std::hash<std::string> hasher;
    return ToHex(static_cast<uint64_t>(hasher(chunk_id)), 8);
}

std::string ImageStore::JoinChunkKey(const std::string& disk_id, const std::string& chunk_id) {
    return disk_id + "/" + chunk_id;
}

void ImageStore::SleepByBytes(uint64_t bytes, uint64_t bytes_per_sec) {
    if (bytes == 0 || bytes_per_sec == 0) {
        return;
    }
    const uint64_t ns_per_sec = 1000000000ULL;
    const uint64_t duration_ns = (bytes * ns_per_sec + bytes_per_sec - 1) / bytes_per_sec;
    std::this_thread::sleep_for(std::chrono::nanoseconds(duration_ns));
}

std::string ImageStore::BuildSimulatedChunkData(const std::string& chunk_id, uint64_t offset, uint64_t size) {
    if (size == 0) {
        return {};
    }
    std::string data(static_cast<size_t>(size), '\0');
    std::hash<std::string> hasher;
    const uint64_t seed = static_cast<uint64_t>(hasher(chunk_id));
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

} // namespace zb::optical_node
