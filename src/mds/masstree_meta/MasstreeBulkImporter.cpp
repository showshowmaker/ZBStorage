#include "MasstreeBulkImporter.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <vector>

#include "MasstreeDecimalUtils.h"
#include "MasstreeIndexRuntime.h"
#include "MasstreeInodeRecordCodec.h"
#include "MasstreeManifest.h"
#include "MasstreeOpticalAllocator.h"
#include "MasstreePageLayout.h"
#include "MasstreePageReader.h"
#include "MasstreeOpticalProfile.h"
#include "mds.pb.h"

namespace zb::mds {

namespace {

struct InodeRecordView {
    uint64_t inode_id{0};
    std::string payload;
};

struct DentryRecordView {
    uint64_t parent_inode{0};
    std::string name;
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

constexpr size_t kMasstreeIoBufferBytes = 8U * 1024U * 1024U;

template <typename Stream>
void ConfigureStreamBuffer(Stream* stream, std::vector<char>* buffer) {
    if (!stream || !buffer || buffer->empty()) {
        return;
    }
    stream->rdbuf()->pubsetbuf(buffer->data(), static_cast<std::streamsize>(buffer->size()));
}

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8U);
}

uint32_t DecodeLe32(const char* data) {
    uint32_t value = 0;
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

bool ReadExact(std::ifstream* input, char* data, size_t len) {
    return input && data && input->read(data, static_cast<std::streamsize>(len)).good();
}

bool ReadString(std::ifstream* input, size_t len, std::string* out) {
    if (!out) {
        return false;
    }
    out->assign(len, '\0');
    return len == 0 ? true : ReadExact(input, &(*out)[0], len);
}

bool ReadInodeRecord(std::ifstream* input, InodeRecordView* record, std::string* error) {
    if (!input || !record) {
        if (error) {
            *error = "invalid inode record read args";
        }
        return false;
    }
    char inode_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, inode_buf, sizeof(inode_buf))) {
        if (input->eof()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        if (error) {
            *error = "failed to read inode record header";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!ReadExact(input, len_buf, sizeof(len_buf))) {
        if (error) {
            *error = "corrupted inode record length";
        }
        return false;
    }
    record->inode_id = DecodeLe64(inode_buf);
    const uint32_t payload_len = DecodeLe32(len_buf);
    if (!ReadString(input, payload_len, &record->payload)) {
        if (error) {
            *error = "corrupted inode record payload";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ReadDentryRecord(std::ifstream* input, DentryRecordView* record, std::string* error) {
    if (!input || !record) {
        if (error) {
            *error = "invalid dentry record read args";
        }
        return false;
    }
    char parent_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, parent_buf, sizeof(parent_buf))) {
        if (input->eof()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        if (error) {
            *error = "failed to read dentry record header";
        }
        return false;
    }
    char name_len_buf[sizeof(uint16_t)] = {};
    if (!ReadExact(input, name_len_buf, sizeof(name_len_buf))) {
        if (error) {
            *error = "corrupted dentry record name length";
        }
        return false;
    }
    const uint16_t name_len = DecodeLe16(name_len_buf);
    if (!ReadString(input, name_len, &record->name)) {
        if (error) {
            *error = "corrupted dentry record name";
        }
        return false;
    }
    char child_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, child_buf, sizeof(child_buf))) {
        if (error) {
            *error = "corrupted dentry record child inode";
        }
        return false;
    }
    char raw_type = 0;
    if (!ReadExact(input, &raw_type, sizeof(raw_type))) {
        if (error) {
            *error = "corrupted dentry record type";
        }
        return false;
    }
    record->parent_inode = DecodeLe64(parent_buf);
    record->child_inode = DecodeLe64(child_buf);
    record->type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(raw_type));
    if (error) {
        error->clear();
    }
    return true;
}

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

bool WriteExact(std::ofstream* output, const std::string& data) {
    return output && output->write(data.data(), static_cast<std::streamsize>(data.size())).good();
}

bool WriteLengthPrefixedRecord(std::ofstream* output, const std::string& payload) {
    std::string wire_record;
    wire_record.reserve(sizeof(uint32_t) + payload.size());
    AppendLe32(&wire_record, static_cast<uint32_t>(payload.size()));
    wire_record.append(payload);
    return WriteExact(output, wire_record);
}

bool BuildDentryPages(std::ifstream* dentry_in,
                      const MasstreeNamespaceManifest& manifest,
                      MasstreeIndexRuntime* runtime,
                      uint32_t verify_dentry_samples,
                      uint64_t* dentry_imported,
                      uint64_t* dentry_page_count,
                      uint64_t* verified_dentry_samples,
                      std::string* error) {
    if (!dentry_in || !runtime || !dentry_imported || !dentry_page_count || !verified_dentry_samples) {
        if (error) {
            *error = "invalid masstree dentry sparse build args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.dentry_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.dentry_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree dentry sparse outputs";
        }
        return false;
    }

    std::vector<MasstreeDentryPageEntry> current_page;
    current_page.reserve(1024);
    size_t current_payload_bytes = sizeof(uint32_t);
    std::vector<DentryRecordView> verify_samples;
    verify_samples.reserve(verify_dentry_samples);
    DentryRecordView dentry_record;

    auto flush_page = [&](std::string* flush_error) -> bool {
        if (current_page.empty()) {
            return true;
        }
        std::string page_payload;
        if (!EncodeMasstreeDentryPage(current_page, &page_payload, flush_error)) {
            return false;
        }
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, page_payload)) {
            if (flush_error) {
                *flush_error = "failed to write masstree dentry page";
            }
            return false;
        }
        MasstreeDentrySparseEntry sparse_entry;
        sparse_entry.page_offset = page_offset;
        sparse_entry.max_parent_inode = current_page.back().parent_inode;
        sparse_entry.max_name = current_page.back().name;
        std::string sparse_payload;
        if (!EncodeMasstreeDentrySparseEntry(sparse_entry, &sparse_payload, flush_error) ||
            !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
            !runtime->PutDentryPageBoundary(manifest.namespace_id,
                                            sparse_entry.max_parent_inode,
                                            sparse_entry.max_name,
                                            sparse_entry.page_offset,
                                            flush_error)) {
            return false;
        }
        ++(*dentry_page_count);
        current_page.clear();
        current_payload_bytes = sizeof(uint32_t);
        return true;
    };

    while (ReadDentryRecord(dentry_in, &dentry_record, error)) {
        MasstreeDentryPageEntry page_entry;
        page_entry.parent_inode = dentry_record.parent_inode;
        page_entry.name = dentry_record.name;
        page_entry.child_inode = dentry_record.child_inode;
        page_entry.type = dentry_record.type;
        const size_t encoded_size = EncodedMasstreeDentryPageEntrySize(page_entry);
        const size_t total_with_entry = current_payload_bytes + encoded_size;
        if (!current_page.empty() && total_with_entry > manifest.page_size_bytes) {
            if (!flush_page(error)) {
                return false;
            }
        }
        current_page.push_back(std::move(page_entry));
        current_payload_bytes += encoded_size;
        ++(*dentry_imported);
        if (verify_samples.size() < verify_dentry_samples) {
            verify_samples.push_back(dentry_record);
        }
    }
    if (error && !error->empty()) {
        return false;
    }
    if (!flush_page(error)) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree dentry sparse outputs";
        }
        return false;
    }

    for (const auto& sample : verify_samples) {
        MasstreeDentrySparseEntry sparse_entry;
        if (!runtime->FindDentryPageBoundary(manifest.namespace_id,
                                             sample.parent_inode,
                                             sample.name,
                                             &sparse_entry,
                                             error)) {
            if (error && error->empty()) {
                *error = "masstree importer dentry sparse verification failed";
            }
            return false;
        }
        MasstreeDentryPage page;
        if (!MasstreePageReader::LoadDentryPage(manifest.dentry_pages_path,
                                                sparse_entry.page_offset,
                                                &page,
                                                error)) {
            return false;
        }
        MasstreeDentryPageEntry found;
        if (!MasstreePageReader::FindDentryInPage(page,
                                                  sample.parent_inode,
                                                  sample.name,
                                                  &found,
                                                  error)) {
            if (error && error->empty()) {
                *error = "masstree importer dentry page verification failed";
            }
            return false;
        }
        if (found.child_inode != sample.child_inode || found.type != sample.type) {
            if (error) {
                *error = "masstree importer dentry page verification mismatch";
            }
            return false;
        }
        ++(*verified_dentry_samples);
    }
    return true;
}

bool BuildInodePages(std::ifstream* inode_in,
                     const MasstreeNamespaceManifest& manifest,
                     MasstreeIndexRuntime* runtime,
                     const MasstreeOpticalProfile& optical_profile,
                     const MasstreeOpticalClusterCursor& start_cursor,
                     uint32_t verify_inode_samples,
                     uint64_t* inode_imported,
                     uint64_t* inode_page_count,
                     uint64_t* inode_pages_bytes,
                     uint64_t* verified_inode_samples,
                     uint64_t* file_count,
                     uint64_t* start_global_image_id,
                     uint64_t* end_global_image_id,
                     uint64_t* avg_file_size_bytes,
                     std::string* total_file_bytes,
                     MasstreeOpticalClusterCursor* end_cursor,
                     std::string* error) {
    if (!inode_in || !runtime || !inode_imported || !inode_page_count || !inode_pages_bytes ||
        !verified_inode_samples || !file_count || !start_global_image_id || !end_global_image_id ||
        !avg_file_size_bytes || !total_file_bytes || !end_cursor) {
        if (error) {
            *error = "invalid masstree inode sparse build args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.inode_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.inode_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree inode sparse outputs";
        }
        return false;
    }

    MasstreeOpticalAllocator allocator(optical_profile, start_cursor);
    MasstreeDecimalAccumulator total_file_bytes_accumulator;
    std::vector<MasstreeInodePageEntry> current_page;
    current_page.reserve(1024);
    size_t current_payload_bytes = sizeof(uint32_t);
    std::vector<uint64_t> verify_samples;
    verify_samples.reserve(verify_inode_samples);
    InodeRecordView inode_record;

    auto flush_page = [&](std::string* flush_error) -> bool {
        if (current_page.empty()) {
            return true;
        }
        std::string page_payload;
        if (!EncodeMasstreeInodePage(current_page, &page_payload, flush_error)) {
            return false;
        }
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, page_payload)) {
            if (flush_error) {
                *flush_error = "failed to write masstree inode page";
            }
            return false;
        }
        MasstreeInodeSparseEntry sparse_entry;
        sparse_entry.page_offset = page_offset;
        sparse_entry.max_inode_id = current_page.back().inode_id;
        std::string sparse_payload;
        if (!EncodeMasstreeInodeSparseEntry(sparse_entry, &sparse_payload, flush_error) ||
            !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
            !runtime->PutInodePageBoundary(manifest.namespace_id,
                                           sparse_entry.max_inode_id,
                                           sparse_entry.page_offset,
                                           flush_error)) {
            return false;
        }
        *inode_pages_bytes += sizeof(uint32_t) + page_payload.size();
        ++(*inode_page_count);
        current_page.clear();
        current_payload_bytes = sizeof(uint32_t);
        return true;
    };

    while (ReadInodeRecord(inode_in, &inode_record, error)) {
        zb::rpc::InodeAttr attr;
        if (!attr.ParseFromString(inode_record.payload)) {
            if (error) {
                *error = "failed to parse inode record payload";
            }
            return false;
        }

        MasstreeInodeRecord blob_record;
        blob_record.attr = attr;
        if (attr.type() == zb::rpc::INODE_FILE) {
            uint64_t global_image_id = 0;
            if (!allocator.Allocate(attr.size(), &global_image_id, error)) {
                return false;
            }
            blob_record.has_optical_image = true;
            blob_record.optical_image_global_id = global_image_id;
            if (*file_count == 0) {
                *start_global_image_id = global_image_id;
            }
            *end_global_image_id = global_image_id;
            ++(*file_count);
            total_file_bytes_accumulator.Add(attr.size());
        }

        std::string encoded_record_payload;
        if (!MasstreeInodeRecordCodec::Encode(blob_record, &encoded_record_payload, error)) {
            return false;
        }

        MasstreeInodePageEntry page_entry;
        page_entry.inode_id = inode_record.inode_id;
        page_entry.payload = std::move(encoded_record_payload);
        const size_t encoded_size = EncodedMasstreeInodePageEntrySize(page_entry);
        const size_t total_with_entry = current_payload_bytes + encoded_size;
        if (!current_page.empty() && total_with_entry > manifest.page_size_bytes) {
            if (!flush_page(error)) {
                return false;
            }
        }
        current_page.push_back(std::move(page_entry));
        current_payload_bytes += encoded_size;
        ++(*inode_imported);
        if (verify_samples.size() < verify_inode_samples) {
            verify_samples.push_back(inode_record.inode_id);
        }
    }
    if (error && !error->empty()) {
        return false;
    }
    if (!flush_page(error)) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree inode sparse outputs";
        }
        return false;
    }

    for (uint64_t inode_id : verify_samples) {
        MasstreeInodeSparseEntry sparse_entry;
        if (!runtime->FindInodePageBoundary(manifest.namespace_id, inode_id, &sparse_entry, error)) {
            if (error && error->empty()) {
                *error = "masstree importer inode sparse verification failed";
            }
            return false;
        }
        MasstreeInodePage page;
        if (!MasstreePageReader::LoadInodePage(manifest.inode_pages_path,
                                               sparse_entry.page_offset,
                                               &page,
                                               error)) {
            return false;
        }
        MasstreeInodePageEntry found;
        if (!MasstreePageReader::FindInodeInPage(page, inode_id, &found, error)) {
            if (error && error->empty()) {
                *error = "masstree importer inode page verification failed";
            }
            return false;
        }
        MasstreeInodeRecord decoded;
        if (!MasstreeInodeRecordCodec::Decode(found.payload, &decoded, error) ||
            decoded.attr.inode_id() != inode_id) {
            if (error && error->empty()) {
                *error = "masstree importer inode page verification mismatch";
            }
            return false;
        }
        ++(*verified_inode_samples);
    }

    *total_file_bytes = total_file_bytes_accumulator.ToString();
    *avg_file_size_bytes = *file_count == 0 ? 0 : total_file_bytes_accumulator.DivideBy(*file_count);
    *end_cursor = allocator.cursor();
    return true;
}

bool WriteImportSummary(const std::string& summary_path,
                        const MasstreeNamespaceManifest& manifest,
                        const MasstreeBulkImporter::Result& result,
                        std::string* error) {
    std::ofstream out(summary_path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree import summary: " + summary_path;
        }
        return false;
    }
    out << "masstree_import_summary_v2\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "inode_imported=" << result.inode_imported << "\n";
    out << "dentry_imported=" << result.dentry_imported << "\n";
    out << "dentry_page_count=" << result.dentry_page_count << "\n";
    out << "inode_page_count=" << result.inode_page_count << "\n";
    out << "inode_pages_bytes=" << result.inode_pages_bytes << "\n";
    out << "verified_inode_samples=" << result.verified_inode_samples << "\n";
    out << "verified_dentry_samples=" << result.verified_dentry_samples << "\n";
    out << "file_count=" << result.file_count << "\n";
    out << "total_file_bytes=" << result.total_file_bytes << "\n";
    out << "avg_file_size_bytes=" << result.avg_file_size_bytes << "\n";
    out << "start_global_image_id=" << result.start_global_image_id << "\n";
    out << "end_global_image_id=" << result.end_global_image_id << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree import summary: " + summary_path;
        }
        return false;
    }
    return true;
}

bool WriteClusterStats(const std::string& path,
                       const MasstreeNamespaceManifest& manifest,
                       const MasstreeBulkImporter::Result& result,
                       const MasstreeOpticalProfile& profile,
                       std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree cluster stats: " + path;
        }
        return false;
    }
    out << "masstree_cluster_stats_v1\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "optical_node_count=" << profile.optical_node_count << "\n";
    out << "optical_disk_count=" << (static_cast<uint64_t>(profile.optical_node_count) *
                                      static_cast<uint64_t>(profile.disks_per_node)) << "\n";
    out << "total_capacity_bytes=" << profile.TotalCapacityBytesDecimal() << "\n";
    out << "file_count=" << result.file_count << "\n";
    out << "total_file_bytes=" << result.total_file_bytes << "\n";
    out << "avg_file_size_bytes=" << result.avg_file_size_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree cluster stats: " + path;
        }
        return false;
    }
    return true;
}

bool WriteAllocationSummary(const std::string& path,
                            const MasstreeBulkImporter::Result& result,
                            std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree allocation summary: " + path;
        }
        return false;
    }
    out << "masstree_allocation_summary_v1\n";
    out << "file_count=" << result.file_count << "\n";
    out << "start_global_image_id=" << result.start_global_image_id << "\n";
    out << "end_global_image_id=" << result.end_global_image_id << "\n";
    out << "start_cursor=" << result.start_cursor.node_index << "," << result.start_cursor.disk_index << ","
        << result.start_cursor.image_index_in_disk << "," << result.start_cursor.image_used_bytes << "\n";
    out << "end_cursor=" << result.end_cursor.node_index << "," << result.end_cursor.disk_index << ","
        << result.end_cursor.image_index_in_disk << "," << result.end_cursor.image_used_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree allocation summary: " + path;
        }
        return false;
    }
    return true;
}

} // namespace

bool MasstreeBulkImporter::Import(const Request& request,
                                  MasstreeIndexRuntime* runtime,
                                  Result* result,
                                  std::string* error) const {
    if (result) {
        *result = Result();
    }
    if (request.manifest_path.empty()) {
        if (error) {
            *error = "masstree bulk importer manifest_path is empty";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    if (!MasstreeNamespaceManifest::LoadFromFile(request.manifest_path, &manifest, error)) {
        return false;
    }

    const MasstreeOpticalProfile optical_profile = MasstreeOpticalProfile::Fixed();
    MasstreeIndexRuntime local_runtime;
    MasstreeIndexRuntime* active_runtime = runtime ? runtime : &local_runtime;
    std::string init_error;
    if (!active_runtime->Init(&init_error)) {
        if (error) {
            *error = init_error;
        }
        return false;
    }

    std::ifstream inode_in;
    std::ifstream dentry_in;
    std::vector<char> inode_in_buffer(kMasstreeIoBufferBytes);
    std::vector<char> dentry_in_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&inode_in, &inode_in_buffer);
    ConfigureStreamBuffer(&dentry_in, &dentry_in_buffer);
    inode_in.open(manifest.inode_records_path, std::ios::binary);
    dentry_in.open(manifest.dentry_records_path, std::ios::binary);
    if (!inode_in || !dentry_in) {
        if (error) {
            *error = "failed to open masstree importer inputs/outputs";
        }
        return false;
    }

    Result local_result;
    local_result.start_cursor = request.start_cursor;
    local_result.end_cursor = request.start_cursor;
    local_result.total_file_bytes = "0";
    manifest.page_size_bytes = request.page_size_bytes >= 4096U
                                   ? request.page_size_bytes
                                   : (manifest.page_size_bytes == 0
                                          ? kMasstreeDefaultPageSizeBytes
                                          : manifest.page_size_bytes);
    const std::filesystem::path staging_dir_path = std::filesystem::path(request.manifest_path).parent_path();
    if (manifest.inode_pages_path.empty()) {
        manifest.inode_pages_path = (staging_dir_path / "inode_pages.seg").string();
    }
    if (manifest.inode_sparse_index_path.empty()) {
        manifest.inode_sparse_index_path = (staging_dir_path / "inode_sparse.idx").string();
    }
    if (manifest.dentry_pages_path.empty()) {
        manifest.dentry_pages_path = (staging_dir_path / "dentry_pages.seg").string();
    }
    if (manifest.dentry_sparse_index_path.empty()) {
        manifest.dentry_sparse_index_path = (staging_dir_path / "dentry_sparse.idx").string();
    }
    if (!BuildInodePages(&inode_in,
                         manifest,
                         active_runtime,
                         optical_profile,
                         request.start_cursor,
                         request.verify_inode_samples,
                         &local_result.inode_imported,
                         &local_result.inode_page_count,
                         &local_result.inode_pages_bytes,
                         &local_result.verified_inode_samples,
                         &local_result.file_count,
                         &local_result.start_global_image_id,
                         &local_result.end_global_image_id,
                         &local_result.avg_file_size_bytes,
                         &local_result.total_file_bytes,
                         &local_result.end_cursor,
                         error)) {
        return false;
    }

    if (!BuildDentryPages(&dentry_in,
                          manifest,
                          active_runtime,
                          request.verify_dentry_samples,
                          &local_result.dentry_imported,
                          &local_result.dentry_page_count,
                          &local_result.verified_dentry_samples,
                          error)) {
        return false;
    }

    if (local_result.inode_imported != manifest.inode_count) {
        if (error) {
            *error = "masstree importer inode count mismatch";
        }
        return false;
    }
    if (local_result.dentry_imported != manifest.dentry_count) {
        if (error) {
            *error = "masstree importer dentry count mismatch";
        }
        return false;
    }
    if (local_result.file_count != manifest.file_count) {
        if (error) {
            *error = "masstree importer file count mismatch";
        }
        return false;
    }

    const std::string summary_path = (staging_dir_path / "import_summary.txt").string();
    const std::string cluster_stats_path = (staging_dir_path / "cluster_stats.txt").string();
    const std::string allocation_summary_path = (staging_dir_path / "allocation_summary.txt").string();

    manifest.cluster_stats_path = cluster_stats_path;
    manifest.allocation_summary_path = allocation_summary_path;
    manifest.min_file_size_bytes = optical_profile.min_file_size_bytes;
    manifest.max_file_size_bytes = optical_profile.max_file_size_bytes;
    manifest.avg_file_size_bytes = local_result.avg_file_size_bytes;
    manifest.total_file_bytes = local_result.total_file_bytes;
    manifest.start_global_image_id = local_result.start_global_image_id;
    manifest.end_global_image_id = local_result.end_global_image_id;
    manifest.start_cursor_node_index = local_result.start_cursor.node_index;
    manifest.start_cursor_disk_index = local_result.start_cursor.disk_index;
    manifest.start_cursor_image_index = local_result.start_cursor.image_index_in_disk;
    manifest.start_cursor_image_used_bytes = local_result.start_cursor.image_used_bytes;
    manifest.end_cursor_node_index = local_result.end_cursor.node_index;
    manifest.end_cursor_disk_index = local_result.end_cursor.disk_index;
    manifest.end_cursor_image_index = local_result.end_cursor.image_index_in_disk;
    manifest.end_cursor_image_used_bytes = local_result.end_cursor.image_used_bytes;
    manifest.layout_version = 3;
    manifest.inode_page_count = local_result.inode_page_count;
    manifest.dentry_page_count = local_result.dentry_page_count;

    if (!manifest.SaveToFile(request.manifest_path, error) ||
        !WriteImportSummary(summary_path, manifest, local_result, error) ||
        !WriteClusterStats(cluster_stats_path, manifest, local_result, optical_profile, error) ||
        !WriteAllocationSummary(allocation_summary_path, local_result, error)) {
        return false;
    }

    if (result) {
        *result = local_result;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
