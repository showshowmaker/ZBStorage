#include "ArchiveImportService.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <queue>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#include <rocksdb/iterator.h>

#include "ArchiveBloomFilter.h"
#include "ArchiveFormat.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"

namespace fs = std::filesystem;

namespace zb::mds {

namespace {

struct InodeRecord {
    uint64_t inode_id{0};
    std::string payload;
};

struct DentryRecord {
    uint64_t parent_inode{0};
    std::string name;
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

constexpr char kArchiveSegmentMagic[8] = {'A', 'R', 'C', 'D', 'A', 'T', '1', '\0'};
constexpr char kArchiveIndexMagic[8] = {'A', 'R', 'C', 'I', 'D', 'X', '1', '\0'};

#ifdef _WIN32
std::string FormatWinError(const std::string& prefix, DWORD code) {
    LPSTR buffer = nullptr;
    const DWORD flags = FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    const DWORD length = FormatMessageA(flags,
                                        nullptr,
                                        code,
                                        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                        reinterpret_cast<LPSTR>(&buffer),
                                        0,
                                        nullptr);
    std::string message = prefix + " (code=" + std::to_string(code) + ")";
    if (length != 0 && buffer) {
        std::string text(buffer, length);
        while (!text.empty() && (text.back() == '\r' || text.back() == '\n')) {
            text.pop_back();
        }
        message += ": " + text;
    }
    if (buffer) {
        LocalFree(buffer);
    }
    return message;
}

bool SyncRegularFile(const fs::path& path, std::string* error) {
    const std::wstring wide_path = path.wstring();
    HANDLE handle = CreateFileW(wide_path.c_str(),
                                GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr,
                                OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL,
                                nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        if (error) {
            *error = FormatWinError("failed to open file for sync: " + path.string(), GetLastError());
        }
        return false;
    }
    const BOOL ok = FlushFileBuffers(handle);
    const DWORD flush_error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!ok) {
        if (error) {
            *error = FormatWinError("failed to sync file: " + path.string(), flush_error);
        }
        return false;
    }
    return true;
}

bool SyncDirectory(const fs::path& path, std::string* error) {
    const std::wstring wide_path = path.wstring();
    HANDLE handle = CreateFileW(wide_path.c_str(),
                                FILE_LIST_DIRECTORY,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                                nullptr,
                                OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS,
                                nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        if (error) {
            *error = FormatWinError("failed to open directory for sync: " + path.string(), GetLastError());
        }
        return false;
    }
    const BOOL ok = FlushFileBuffers(handle);
    const DWORD flush_error = ok ? ERROR_SUCCESS : GetLastError();
    CloseHandle(handle);
    if (!ok && (flush_error == ERROR_ACCESS_DENIED || flush_error == ERROR_INVALID_FUNCTION)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!ok) {
        if (error) {
            *error = FormatWinError("failed to sync directory: " + path.string(), flush_error);
        }
        return false;
    }
    return true;
}
#else
std::string FormatErrno(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}

bool SyncRegularFile(const fs::path& path, std::string* error) {
    const int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (error) {
            *error = FormatErrno("failed to open file for sync: " + path.string());
        }
        return false;
    }
    const int rc = fsync(fd);
    const int saved_errno = (rc == 0) ? 0 : errno;
    close(fd);
    if (rc != 0) {
        errno = saved_errno;
        if (error) {
            *error = FormatErrno("failed to sync file: " + path.string());
        }
        return false;
    }
    return true;
}

bool SyncDirectory(const fs::path& path, std::string* error) {
    const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        if (error) {
            *error = FormatErrno("failed to open directory for sync: " + path.string());
        }
        return false;
    }
    const int rc = fsync(fd);
    const int saved_errno = (rc == 0) ? 0 : errno;
    close(fd);
    if (rc != 0) {
        errno = saved_errno;
        if (error) {
            *error = FormatErrno("failed to sync directory: " + path.string());
        }
        return false;
    }
    return true;
}
#endif

bool SyncParentDirectory(const fs::path& path, std::string* error) {
    const fs::path parent = path.parent_path();
    if (parent.empty()) {
        if (error) {
            error->clear();
        }
        return true;
    }
    return SyncDirectory(parent, error);
}

bool FlushCloseAndSync(std::ofstream* out, const fs::path& path, std::string* error) {
    if (!out) {
        if (error) {
            *error = "invalid output stream";
        }
        return false;
    }
    out->flush();
    if (!out->good()) {
        if (error) {
            *error = "failed to flush output file: " + path.string();
        }
        return false;
    }
    out->close();
    if (out->fail()) {
        if (error) {
            *error = "failed to close output file: " + path.string();
        }
        return false;
    }
    if (!SyncRegularFile(path, error)) {
        return false;
    }
    if (!SyncParentDirectory(path, error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

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

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(path);
    while (std::getline(stream, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool GetInode(RocksMetaStore* store,
              uint64_t inode_id,
              zb::rpc::InodeAttr* attr,
              std::string* payload,
              std::string* error) {
    if (!store || inode_id == 0 || !attr || !payload) {
        if (error) {
            *error = "invalid inode lookup args";
        }
        return false;
    }
    if (!store->Get(InodeKey(inode_id), payload, error)) {
        return false;
    }
    std::string decode_error;
    if (!MetaCodec::DecodeUnifiedInodeAttr(*payload, attr, &decode_error)) {
        if (error) {
            *error = decode_error.empty() ? "invalid inode payload" : decode_error;
        }
        return false;
    }
    return true;
}

bool ResolvePath(RocksMetaStore* store,
                 const std::string& path,
                 uint64_t* inode_id,
                 std::string* error) {
    if (!store || !inode_id) {
        if (error) {
            *error = "invalid resolve path args";
        }
        return false;
    }
    std::string normalized_path;
    if (!NormalizePath(path, &normalized_path)) {
        if (error) {
            *error = "invalid path prefix";
        }
        return false;
    }
    uint64_t current = kRootInodeId;
    if (normalized_path == "/") {
        *inode_id = current;
        return true;
    }
    for (const auto& part : SplitPath(normalized_path)) {
        std::string payload;
        if (!store->Get(DentryKey(current, part), &payload, error)) {
            return false;
        }
        if (!MetaCodec::DecodeUInt64(payload, &current)) {
            if (error) {
                *error = "invalid dentry payload";
            }
            return false;
        }
    }
    *inode_id = current;
    return true;
}

bool CollectNamespace(RocksMetaStore* store,
                      uint64_t root_inode,
                      std::vector<InodeRecord>* inode_records,
                      std::vector<DentryRecord>* dentry_records,
                      std::string* error) {
    if (!store || !inode_records || !dentry_records || !store->db()) {
        if (error) {
            *error = "invalid collect namespace args";
        }
        return false;
    }

    inode_records->clear();
    dentry_records->clear();
    std::queue<uint64_t> pending;
    std::unordered_set<uint64_t> visited;
    pending.push(root_inode);

    while (!pending.empty()) {
        const uint64_t inode_id = pending.front();
        pending.pop();
        if (!visited.insert(inode_id).second) {
            continue;
        }

        zb::rpc::InodeAttr attr;
        std::string inode_payload;
        if (!GetInode(store, inode_id, &attr, &inode_payload, error)) {
            return false;
        }
        inode_records->push_back({inode_id, inode_payload});

        if (attr.type() != zb::rpc::INODE_DIR) {
            continue;
        }

        const std::string prefix = DentryPrefix(inode_id);
        std::unique_ptr<rocksdb::Iterator> it(store->db()->NewIterator(rocksdb::ReadOptions()));
        for (it->Seek(prefix); it->Valid(); it->Next()) {
            const std::string key = it->key().ToString();
            if (key.rfind(prefix, 0) != 0) {
                break;
            }
            uint64_t child_inode = 0;
            if (!MetaCodec::DecodeUInt64(it->value().ToString(), &child_inode)) {
                continue;
            }
            zb::rpc::InodeAttr child_attr;
            std::string child_payload;
            if (!GetInode(store, child_inode, &child_attr, &child_payload, error)) {
                return false;
            }
            dentry_records->push_back({inode_id, key.substr(prefix.size()), child_inode, child_attr.type()});
            pending.push(child_inode);
        }
    }

    std::sort(inode_records->begin(), inode_records->end(), [](const InodeRecord& lhs, const InodeRecord& rhs) {
        return lhs.inode_id < rhs.inode_id;
    });
    std::sort(dentry_records->begin(), dentry_records->end(), [](const DentryRecord& lhs, const DentryRecord& rhs) {
        if (lhs.parent_inode != rhs.parent_inode) {
            return lhs.parent_inode < rhs.parent_inode;
        }
        return lhs.name < rhs.name;
    });
    return true;
}

void AppendLe16(std::string* out, uint16_t value) {
    out->push_back(static_cast<char>(value & 0xff));
    out->push_back(static_cast<char>((value >> 8) & 0xff));
}

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

void AppendLe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

void AppendBe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        const size_t shift = (sizeof(uint64_t) - 1 - i) * 8;
        out->push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::string EncodeInodeKey(uint64_t inode_id) {
    std::string key;
    key.reserve(sizeof(uint64_t));
    AppendBe64(&key, inode_id);
    return key;
}

std::string EncodeDentryKey(uint64_t parent_inode, const std::string& name) {
    std::string key;
    key.reserve(sizeof(uint64_t) + name.size());
    AppendBe64(&key, parent_inode);
    key.append(name);
    return key;
}

std::string SerializeInodeRecord(const InodeRecord& record) {
    std::string out;
    out.reserve(sizeof(uint64_t) + sizeof(uint32_t) + record.payload.size());
    AppendBe64(&out, record.inode_id);
    AppendLe32(&out, static_cast<uint32_t>(record.payload.size()));
    out.append(record.payload);
    return out;
}

std::string SerializeDentryRecord(const DentryRecord& record) {
    std::string out;
    out.reserve(sizeof(uint64_t) + sizeof(uint16_t) + record.name.size() + sizeof(uint64_t) + sizeof(uint8_t));
    AppendBe64(&out, record.parent_inode);
    AppendLe16(&out, static_cast<uint16_t>(record.name.size()));
    out.append(record.name);
    AppendBe64(&out, record.child_inode);
    out.push_back(static_cast<char>(record.type));
    return out;
}

bool WriteExact(std::ofstream* out, const std::string& data) {
    return out && out->write(data.data(), static_cast<std::streamsize>(data.size())).good();
}

bool WriteSegmentHeader(std::ofstream* out,
                        ArchiveTableKind kind,
                        uint32_t page_size_bytes,
                        uint64_t page_count) {
    if (!out) {
        return false;
    }
    std::string header;
    header.append(kArchiveSegmentMagic, sizeof(kArchiveSegmentMagic));
    header.push_back(static_cast<char>(kind));
    header.push_back(static_cast<char>(kArchiveFormatVersion));
    header.push_back('\0');
    header.push_back('\0');
    AppendLe32(&header, page_size_bytes);
    AppendLe64(&header, page_count);
    return WriteExact(out, header);
}

bool WriteIndexHeader(std::ofstream* out, ArchiveTableKind kind, uint64_t page_count) {
    if (!out) {
        return false;
    }
    std::string header;
    header.append(kArchiveIndexMagic, sizeof(kArchiveIndexMagic));
    header.push_back(static_cast<char>(kind));
    header.push_back(static_cast<char>(kArchiveFormatVersion));
    header.push_back('\0');
    header.push_back('\0');
    AppendLe64(&header, page_count);
    return WriteExact(out, header);
}

class SegmentWriter {
public:
    SegmentWriter(ArchiveTableKind kind,
                  uint32_t page_size_bytes,
                  const fs::path& data_path,
                  const fs::path& index_path)
        : kind_(kind),
          page_size_bytes_(page_size_bytes),
          data_path_(data_path),
          index_path_(index_path) {
    }

    bool Open(std::string* error) {
        data_out_.open(data_path_, std::ios::binary | std::ios::trunc);
        index_out_.open(index_path_, std::ios::binary | std::ios::trunc);
        if (!data_out_ || !index_out_) {
            if (error) {
                *error = "failed to open archive segment outputs";
            }
            return false;
        }
        return WriteSegmentHeader(&data_out_, kind_, page_size_bytes_, 0) &&
               WriteIndexHeader(&index_out_, kind_, 0);
    }

    bool Append(const std::string& record, const std::string& key, std::string* error) {
        const size_t next_record_count = records_.size() + 1;
        const size_t next_bytes = current_record_bytes_ + record.size();
        if (!CanFit(next_record_count, next_bytes)) {
            if (records_.empty()) {
                if (error) {
                    *error = "archive record exceeds configured page_size_bytes";
                }
                return false;
            }
            if (!Flush(error)) {
                return false;
            }
        }
        records_.push_back(record);
        keys_.push_back(key);
        current_record_bytes_ += record.size();
        return true;
    }

    bool Finish(std::string* error) {
        if (!Flush(error)) {
            return false;
        }
        data_out_.seekp(0, std::ios::beg);
        index_out_.seekp(0, std::ios::beg);
        if (!WriteSegmentHeader(&data_out_, kind_, page_size_bytes_, page_count_) ||
            !WriteIndexHeader(&index_out_, kind_, page_count_)) {
            if (error) {
                *error = "failed to finalize archive segment outputs";
            }
            return false;
        }
        if (!FlushCloseAndSync(&data_out_, data_path_, error) ||
            !FlushCloseAndSync(&index_out_, index_path_, error)) {
            return false;
        }
        return true;
    }

private:
    bool CanFit(size_t record_count, size_t record_bytes) const {
        return kArchivePageHeaderSize + record_count * sizeof(uint32_t) + record_bytes <= page_size_bytes_;
    }

    bool Flush(std::string* error) {
        if (records_.empty()) {
            return true;
        }
        std::string page(page_size_bytes_, '\0');
        uint32_t data_begin = page_size_bytes_;
        for (size_t i = 0; i < records_.size(); ++i) {
            data_begin -= static_cast<uint32_t>(records_[i].size());
            std::copy(records_[i].begin(), records_[i].end(), page.begin() + data_begin);
            const uint32_t offset = data_begin;
            const size_t offset_pos = kArchivePageHeaderSize + i * sizeof(uint32_t);
            page[offset_pos + 0] = static_cast<char>(offset & 0xff);
            page[offset_pos + 1] = static_cast<char>((offset >> 8) & 0xff);
            page[offset_pos + 2] = static_cast<char>((offset >> 16) & 0xff);
            page[offset_pos + 3] = static_cast<char>((offset >> 24) & 0xff);
        }
        const uint32_t record_count = static_cast<uint32_t>(records_.size());
        page[0] = static_cast<char>(record_count & 0xff);
        page[1] = static_cast<char>((record_count >> 8) & 0xff);
        page[2] = static_cast<char>((record_count >> 16) & 0xff);
        page[3] = static_cast<char>((record_count >> 24) & 0xff);
        page[4] = static_cast<char>(data_begin & 0xff);
        page[5] = static_cast<char>((data_begin >> 8) & 0xff);
        page[6] = static_cast<char>((data_begin >> 16) & 0xff);
        page[7] = static_cast<char>((data_begin >> 24) & 0xff);
        if (!WriteExact(&data_out_, page)) {
            if (error) {
                *error = "failed to write archive segment page: " + data_path_.string();
            }
            return false;
        }
        std::string index_entry;
        AppendLe64(&index_entry, page_count_);
        AppendLe16(&index_entry, static_cast<uint16_t>(keys_.back().size()));
        index_entry.append(keys_.back());
        if (!WriteExact(&index_out_, index_entry)) {
            if (error) {
                *error = "failed to write archive sparse index entry: " + index_path_.string();
            }
            return false;
        }
        ++page_count_;
        records_.clear();
        keys_.clear();
        current_record_bytes_ = 0;
        return true;
    }

    ArchiveTableKind kind_{ArchiveTableKind::kUnknown};
    uint32_t page_size_bytes_{0};
    fs::path data_path_;
    fs::path index_path_;
    std::ofstream data_out_;
    std::ofstream index_out_;
    uint64_t page_count_{0};
    size_t current_record_bytes_{0};
    std::vector<std::string> records_;
    std::vector<std::string> keys_;
};

bool WriteManifestTmp(const fs::path& path,
                      const ArchiveImportService::Request& request,
                      uint64_t root_inode_id,
                      uint64_t inode_min,
                      uint64_t inode_max,
                      uint64_t inode_count,
                      std::string* error) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to open archive manifest.tmp: " + path.string();
        }
        return false;
    }
    out << "archive_meta_manifest_v1\n";
    out << "namespace_id=" << request.namespace_id << "\n";
    out << "path_prefix=" << request.path_prefix << "\n";
    out << "generation_id=" << request.generation_id << "\n";
    out << "inode_root=inode.seg\n";
    out << "inode_index_root=inode.idx\n";
    out << "inode_bloom_root=inode.bloom\n";
    out << "dentry_root=dentry.seg\n";
    out << "dentry_index_root=dentry.idx\n";
    out << "root_inode_id=" << root_inode_id << "\n";
    out << "inode_min=" << inode_min << "\n";
    out << "inode_max=" << inode_max << "\n";
    out << "inode_count=" << inode_count << "\n";
    out << "page_size_bytes=" << request.page_size_bytes << "\n";
    if (!out.good()) {
        if (error) {
            *error = "failed to write archive manifest.tmp: " + path.string();
        }
        return false;
    }
    if (!FlushCloseAndSync(&out, path, error)) {
        return false;
    }
    return true;
}

} // namespace

ArchiveImportService::ArchiveImportService(RocksMetaStore* source_store,
                                           ArchiveNamespaceCatalog* catalog)
    : source_store_(source_store),
      catalog_(catalog) {
}

bool ArchiveImportService::ImportPathPrefix(const Request& request,
                                            Result* result,
                                            std::string* error) const {
    if (!source_store_ || !catalog_ || request.archive_root.empty() || request.namespace_id.empty() ||
        request.generation_id.empty() || request.path_prefix.empty()) {
        if (error) {
            *error = "invalid archive import request";
        }
        return false;
    }

    Request normalized = request;
    if (normalized.page_size_bytes == 0) {
        normalized.page_size_bytes = kArchiveDefaultPageSizeBytes;
    }
    if (normalized.page_size_bytes < 4096) {
        if (error) {
            *error = "archive import page_size_bytes must be >= 4096";
        }
        return false;
    }
    if (!NormalizePath(normalized.path_prefix, &normalized.path_prefix)) {
        if (error) {
            *error = "invalid archive import path_prefix";
        }
        return false;
    }

    uint64_t root_inode = 0;
    if (!ResolvePath(source_store_, normalized.path_prefix, &root_inode, error)) {
        return false;
    }

    std::vector<InodeRecord> inode_records;
    std::vector<DentryRecord> dentry_records;
    if (!CollectNamespace(source_store_, root_inode, &inode_records, &dentry_records, error)) {
        return false;
    }

    const uint64_t inode_min = inode_records.empty() ? 0 : inode_records.front().inode_id;
    const uint64_t inode_max = inode_records.empty() ? 0 : inode_records.back().inode_id;

    std::vector<uint64_t> inode_buckets;
    inode_buckets.reserve(inode_records.size() / 1024 + 1);
    uint64_t last_bucket = std::numeric_limits<uint64_t>::max();

    ArchiveBloomFilter bloom;
    if (!bloom.Initialize(static_cast<uint64_t>(inode_records.size()), 0.01, error)) {
        return false;
    }
    for (const auto& record : inode_records) {
        bloom.AddUInt64(record.inode_id);
        const uint64_t bucket_id = ArchiveInodeBucketId(record.inode_id);
        if (inode_buckets.empty() || bucket_id != last_bucket) {
            inode_buckets.push_back(bucket_id);
            last_bucket = bucket_id;
        }
    }

    ArchiveGenerationPublisher publisher(catalog_);
    std::string staging_dir;
    if (!publisher.PrepareStagingDir(normalized.archive_root,
                                     normalized.namespace_id,
                                     normalized.generation_id,
                                     &staging_dir,
                                     error)) {
        return false;
    }

    const fs::path staging(staging_dir);
    const fs::path inode_seg = staging / "inode.seg";
    const fs::path inode_idx = staging / "inode.idx";
    const fs::path inode_bloom = staging / "inode.bloom";
    const fs::path dentry_seg = staging / "dentry.seg";
    const fs::path dentry_idx = staging / "dentry.idx";
    const fs::path manifest_tmp = staging / "manifest.tmp";

    auto cleanup = [&]() {
        std::string cleanup_error;
        publisher.CleanupStagingDir(staging_dir, &cleanup_error);
    };

    SegmentWriter inode_writer(ArchiveTableKind::kInode, normalized.page_size_bytes, inode_seg, inode_idx);
    SegmentWriter dentry_writer(ArchiveTableKind::kDentry, normalized.page_size_bytes, dentry_seg, dentry_idx);
    if (!inode_writer.Open(error) || !dentry_writer.Open(error)) {
        cleanup();
        return false;
    }
    for (const auto& record : inode_records) {
        if (!inode_writer.Append(SerializeInodeRecord(record), EncodeInodeKey(record.inode_id), error)) {
            cleanup();
            return false;
        }
    }
    for (const auto& record : dentry_records) {
        if (!dentry_writer.Append(SerializeDentryRecord(record), EncodeDentryKey(record.parent_inode, record.name), error)) {
            cleanup();
            return false;
        }
    }
    if (!inode_writer.Finish(error) || !dentry_writer.Finish(error)) {
        cleanup();
        return false;
    }
    if (!bloom.SaveToFile(inode_bloom.string(), error) ||
        !SyncRegularFile(inode_bloom, error) ||
        !SyncParentDirectory(inode_bloom, error) ||
        !WriteManifestTmp(manifest_tmp,
                          normalized,
                          root_inode,
                          inode_min,
                          inode_max,
                          static_cast<uint64_t>(inode_records.size()),
                          error)) {
        cleanup();
        return false;
    }

    ArchiveGenerationPublisher::PublishRequest publish_request;
    publish_request.archive_root = normalized.archive_root;
    publish_request.namespace_id = normalized.namespace_id;
    publish_request.generation_id = normalized.generation_id;
    publish_request.path_prefix = normalized.path_prefix;
    publish_request.manifest_tmp_path = manifest_tmp.string();
    publish_request.inode_min = inode_min;
    publish_request.inode_max = inode_max;
    publish_request.inode_count = static_cast<uint64_t>(inode_records.size());
    publish_request.inode_bucket_ids = std::move(inode_buckets);
    publish_request.publish_route = normalized.publish_route;
    std::string final_manifest_path;
    if (!publisher.Publish(publish_request, &final_manifest_path, error)) {
        cleanup();
        return false;
    }

    if (result) {
        result->manifest_path = final_manifest_path;
        result->inode_count = static_cast<uint64_t>(inode_records.size());
        result->dentry_count = static_cast<uint64_t>(dentry_records.size());
        result->inode_min = inode_min;
        result->inode_max = inode_max;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
