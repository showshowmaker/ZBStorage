#include "MasstreeBulkMetaGenerator.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "MasstreeDecimalUtils.h"
#include "MasstreeManifest.h"
#include "MasstreeOpticalProfile.h"
#include "mds.pb.h"

namespace fs = std::filesystem;

namespace zb::mds {

namespace {

constexpr uint32_t kDefaultDirMode = 0755;
constexpr uint32_t kDefaultFileMode = 0644;

bool NormalizePath(std::string path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    std::string out;
    out.reserve(path.size() + 1);
    bool prev_slash = false;
    for (char ch : path) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
            out.push_back(ch);
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    if (out.empty()) {
        out = "/";
    }
    *normalized = std::move(out);
    return true;
}

void AppendLe16(std::string* out, uint16_t value) {
    out->push_back(static_cast<char>(value & 0xFFU));
    out->push_back(static_cast<char>((value >> 8U) & 0xFFU));
}

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

void AppendLe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

std::string FixedWidthDecimal(uint64_t value, size_t width) {
    std::ostringstream out;
    out.width(static_cast<std::streamsize>(width));
    out.fill('0');
    out << value;
    return out.str();
}

std::string DirName(const char prefix, uint64_t value) {
    return std::string(1, prefix) + FixedWidthDecimal(value, 6);
}

std::string FileName(uint64_t value) {
    return "f" + FixedWidthDecimal(value, 9);
}

uint64_t CeilDiv(uint64_t lhs, uint64_t rhs) {
    return rhs == 0 ? 0 : ((lhs + rhs - 1) / rhs);
}

uint64_t NowSeconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
}

bool WriteExact(std::ofstream* out, const std::string& data) {
    return out && out->write(data.data(), static_cast<std::streamsize>(data.size())).good();
}

bool WriteInodeRecord(std::ofstream* out, uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error) {
    if (!out) {
        if (error) {
            *error = "inode record output is unavailable";
        }
        return false;
    }
    std::string payload;
    if (!attr.SerializeToString(&payload)) {
        if (error) {
            *error = "failed to serialize inode record";
        }
        return false;
    }
    std::string record;
    record.reserve(sizeof(uint64_t) + sizeof(uint32_t) + payload.size());
    AppendLe64(&record, inode_id);
    AppendLe32(&record, static_cast<uint32_t>(payload.size()));
    record.append(payload);
    if (!WriteExact(out, record)) {
        if (error) {
            *error = "failed to write inode record";
        }
        return false;
    }
    return true;
}

bool WriteDentryRecord(std::ofstream* out,
                       uint64_t parent_inode,
                       const std::string& name,
                       uint64_t child_inode,
                       zb::rpc::InodeType type,
                       std::string* error) {
    if (!out) {
        if (error) {
            *error = "dentry record output is unavailable";
        }
        return false;
    }
    if (name.size() > 0xFFFFU) {
        if (error) {
            *error = "dentry name exceeds record encoding limit";
        }
        return false;
    }
    std::string record;
    record.reserve(sizeof(uint64_t) + sizeof(uint16_t) + name.size() + sizeof(uint64_t) + sizeof(uint8_t));
    AppendLe64(&record, parent_inode);
    AppendLe16(&record, static_cast<uint16_t>(name.size()));
    record.append(name);
    AppendLe64(&record, child_inode);
    record.push_back(static_cast<char>(type));
    if (!WriteExact(out, record)) {
        if (error) {
            *error = "failed to write dentry record";
        }
        return false;
    }
    return true;
}

zb::rpc::InodeAttr BuildDirectoryAttr(uint64_t inode_id,
                                      uint32_t nlink,
                                      uint64_t now_sec) {
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_DIR);
    attr.set_mode(kDefaultDirMode);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(0);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(nlink);
    attr.set_object_unit_size(0);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    return attr;
}

zb::rpc::InodeAttr BuildFileAttr(uint64_t inode_id, uint64_t file_size_bytes, uint64_t now_sec) {
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_FILE);
    attr.set_mode(kDefaultFileMode);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(file_size_bytes);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(1);
    attr.set_object_unit_size(0);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    return attr;
}

uint64_t HashBytes(const std::string& value) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : value) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t GenerateFileSizeBytes(uint64_t namespace_seed,
                               uint64_t generation_seed,
                               uint64_t file_index,
                               const MasstreeOpticalProfile& profile) {
    const uint64_t span = profile.max_file_size_bytes - profile.min_file_size_bytes;
    const uint64_t seed = namespace_seed ^
                          (generation_seed << 1U) ^
                          (file_index * 11400714819323198485ULL);
    return profile.min_file_size_bytes + (seed % (span + 1ULL));
}

bool WriteVerifyManifest(const fs::path& path,
                         const MasstreeNamespaceManifest& manifest,
                         std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to open verify manifest: " + path.string();
        }
        return false;
    }
    out << "masstree_namespace_verify_v1\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "path_prefix=" << manifest.path_prefix << "\n";
    out << "root_inode_id=" << manifest.root_inode_id << "\n";
    out << "inode_min=" << manifest.inode_min << "\n";
    out << "inode_max=" << manifest.inode_max << "\n";
    out << "inode_count=" << manifest.inode_count << "\n";
    out << "dentry_count=" << manifest.dentry_count << "\n";
    out << "file_count=" << manifest.file_count << "\n";
    out << "level1_dir_count=" << manifest.level1_dir_count << "\n";
    out << "leaf_dir_count=" << manifest.leaf_dir_count << "\n";
    out << "max_files_per_leaf_dir=" << manifest.max_files_per_leaf_dir << "\n";
    out << "max_subdirs_per_dir=" << manifest.max_subdirs_per_dir << "\n";
    out << "min_file_size_bytes=" << manifest.min_file_size_bytes << "\n";
    out << "max_file_size_bytes=" << manifest.max_file_size_bytes << "\n";
    out << "avg_file_size_bytes=" << manifest.avg_file_size_bytes << "\n";
    out << "total_file_bytes=" << manifest.total_file_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write verify manifest: " + path.string();
        }
        return false;
    }
    return true;
}

} // namespace

bool MasstreeBulkMetaGenerator::Generate(const Request& request,
                                         Result* result,
                                         std::string* error) const {
    if (result) {
        *result = Result();
    }
    if (request.output_root.empty() || request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.inode_start == 0 || request.file_count == 0 ||
        request.max_files_per_leaf_dir == 0 || request.max_subdirs_per_dir == 0) {
        if (error) {
            *error = "invalid masstree bulk meta generator request";
        }
        return false;
    }

    std::string normalized_path_prefix;
    if (!NormalizePath(request.path_prefix, &normalized_path_prefix)) {
        if (error) {
            *error = "invalid masstree bulk meta generator path_prefix";
        }
        return false;
    }

    const uint64_t leaf_dir_count = CeilDiv(request.file_count, request.max_files_per_leaf_dir);
    const uint64_t level1_dir_count = std::max<uint64_t>(1, CeilDiv(leaf_dir_count, request.max_subdirs_per_dir));
    const uint64_t level1_inode_count = level1_dir_count;
    const uint64_t leaf_inode_count = leaf_dir_count;
    const uint64_t inode_count = 1 + level1_inode_count + leaf_inode_count + request.file_count;
    const uint64_t dentry_count = level1_dir_count + leaf_dir_count + request.file_count;
    const uint64_t inode_min = request.inode_start;
    const uint64_t inode_max = request.inode_start + inode_count - 1;
    const uint64_t now_sec = NowSeconds();
    const MasstreeOpticalProfile optical_profile = MasstreeOpticalProfile::Fixed();
    const uint64_t namespace_seed = HashBytes(request.namespace_id);
    const uint64_t generation_seed = HashBytes(request.generation_id);

    const fs::path staging_dir = fs::path(request.output_root) /
                                 request.namespace_id /
                                 (request.generation_id + ".staging");
    std::error_code ec;
    if (fs::exists(staging_dir, ec)) {
        if (error) {
            *error = "masstree bulk meta generator staging dir already exists: " + staging_dir.string();
        }
        return false;
    }
    if (!fs::create_directories(staging_dir, ec) || ec) {
        if (error) {
            *error = "failed to create masstree bulk meta generator staging dir: " + staging_dir.string();
        }
        return false;
    }

    const fs::path inode_records_path = staging_dir / "inode.records";
    const fs::path dentry_records_path = staging_dir / "dentry.records";
    const fs::path manifest_path = staging_dir / "manifest.txt";
    const fs::path verify_manifest_path = staging_dir / "verify_manifest.txt";

    std::ofstream inode_out(inode_records_path, std::ios::binary | std::ios::trunc);
    std::ofstream dentry_out(dentry_records_path, std::ios::binary | std::ios::trunc);
    if (!inode_out || !dentry_out) {
        if (error) {
            *error = "failed to open masstree bulk meta generator record outputs";
        }
        return false;
    }

    uint64_t next_inode = request.inode_start;
    const uint64_t root_inode_id = next_inode++;
    const uint64_t level1_inode_start = next_inode;
    next_inode += level1_inode_count;
    const uint64_t leaf_inode_start = next_inode;
    next_inode += leaf_inode_count;
    const uint64_t file_inode_start = next_inode;

    if (!WriteInodeRecord(&inode_out,
                          root_inode_id,
                          BuildDirectoryAttr(root_inode_id, static_cast<uint32_t>(2 + level1_dir_count), now_sec),
                          error)) {
        return false;
    }

    for (uint64_t level1 = 0; level1 < level1_dir_count; ++level1) {
        const uint64_t level1_inode = level1_inode_start + level1;
        const std::string level1_name = DirName('d', level1);
        const uint64_t leaves_in_this_level1 =
            std::min<uint64_t>(request.max_subdirs_per_dir,
                               leaf_dir_count - level1 * static_cast<uint64_t>(request.max_subdirs_per_dir));
        if (!WriteInodeRecord(&inode_out,
                              level1_inode,
                              BuildDirectoryAttr(level1_inode, static_cast<uint32_t>(2 + leaves_in_this_level1), now_sec),
                              error)) {
            return false;
        }
        if (!WriteDentryRecord(&dentry_out,
                               root_inode_id,
                               level1_name,
                               level1_inode,
                               zb::rpc::INODE_DIR,
                               error)) {
            return false;
        }
    }

    for (uint64_t leaf = 0; leaf < leaf_dir_count; ++leaf) {
        const uint64_t level1 = leaf / request.max_subdirs_per_dir;
        const uint64_t slot = leaf % request.max_subdirs_per_dir;
        const uint64_t leaf_inode = leaf_inode_start + leaf;
        const uint64_t files_in_leaf =
            std::min<uint64_t>(request.max_files_per_leaf_dir,
                               request.file_count - leaf * static_cast<uint64_t>(request.max_files_per_leaf_dir));
        const std::string leaf_name = DirName('s', slot);
        if (!WriteInodeRecord(&inode_out,
                              leaf_inode,
                              BuildDirectoryAttr(leaf_inode, static_cast<uint32_t>(2 + files_in_leaf), now_sec),
                              error)) {
            return false;
        }
        if (!WriteDentryRecord(&dentry_out,
                               level1_inode_start + level1,
                               leaf_name,
                               leaf_inode,
                               zb::rpc::INODE_DIR,
                               error)) {
            return false;
        }
    }

    MasstreeDecimalAccumulator total_file_bytes_accumulator;
    for (uint64_t file_index = 0; file_index < request.file_count; ++file_index) {
        const uint64_t leaf = file_index / request.max_files_per_leaf_dir;
        const uint64_t file_inode = file_inode_start + file_index;
        const uint64_t file_size_bytes =
            GenerateFileSizeBytes(namespace_seed, generation_seed, file_index, optical_profile);
        total_file_bytes_accumulator.Add(file_size_bytes);
        if (!WriteInodeRecord(&inode_out,
                              file_inode,
                              BuildFileAttr(file_inode, file_size_bytes, now_sec),
                              error)) {
            return false;
        }
        if (!WriteDentryRecord(&dentry_out,
                               leaf_inode_start + leaf,
                               FileName(file_index),
                               file_inode,
                               zb::rpc::INODE_FILE,
                               error)) {
            return false;
        }
    }

    inode_out.flush();
    dentry_out.flush();
    if (!inode_out.good() || !dentry_out.good()) {
        if (error) {
            *error = "failed to flush masstree bulk meta generator outputs";
        }
        return false;
    }
    inode_out.close();
    dentry_out.close();
    if (inode_out.fail() || dentry_out.fail()) {
        if (error) {
            *error = "failed to close masstree bulk meta generator outputs";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    manifest.namespace_id = request.namespace_id;
    manifest.path_prefix = normalized_path_prefix;
    manifest.generation_id = request.generation_id;
    manifest.manifest_path = manifest_path.string();
    manifest.inode_records_path = inode_records_path.string();
    manifest.dentry_records_path = dentry_records_path.string();
    manifest.verify_manifest_path = verify_manifest_path.string();
    manifest.inode_blob_path = (staging_dir / "inode_blob.bin").string();
    manifest.dentry_data_path = (staging_dir / "dentry_data.bin").string();
    manifest.root_inode_id = root_inode_id;
    manifest.inode_min = inode_min;
    manifest.inode_max = inode_max;
    manifest.inode_count = inode_count;
    manifest.dentry_count = dentry_count;
    manifest.file_count = request.file_count;
    manifest.level1_dir_count = level1_dir_count;
    manifest.leaf_dir_count = leaf_dir_count;
    manifest.max_files_per_leaf_dir = request.max_files_per_leaf_dir;
    manifest.max_subdirs_per_dir = request.max_subdirs_per_dir;
    manifest.min_file_size_bytes = optical_profile.min_file_size_bytes;
    manifest.max_file_size_bytes = optical_profile.max_file_size_bytes;
    manifest.total_file_bytes = total_file_bytes_accumulator.ToString();
    manifest.avg_file_size_bytes = request.file_count == 0 ? 0 :
                                   total_file_bytes_accumulator.DivideBy(request.file_count);

    if (!manifest.SaveToFile(manifest_path.string(), error) ||
        !WriteVerifyManifest(verify_manifest_path, manifest, error)) {
        return false;
    }

    if (result) {
        result->staging_dir = staging_dir.string();
        result->manifest_path = manifest_path.string();
        result->inode_records_path = inode_records_path.string();
        result->dentry_records_path = dentry_records_path.string();
        result->verify_manifest_path = verify_manifest_path.string();
        result->root_inode_id = root_inode_id;
        result->inode_min = inode_min;
        result->inode_max = inode_max;
        result->inode_count = inode_count;
        result->dentry_count = dentry_count;
        result->level1_dir_count = level1_dir_count;
        result->leaf_dir_count = leaf_dir_count;
        result->avg_file_size_bytes = manifest.avg_file_size_bytes;
        result->total_file_bytes = manifest.total_file_bytes;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
