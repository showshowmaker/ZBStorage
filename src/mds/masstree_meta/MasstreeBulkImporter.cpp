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
    out << "inode_blob_bytes=" << result.inode_blob_bytes << "\n";
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

    std::ifstream inode_in(manifest.inode_records_path, std::ios::binary);
    std::ifstream dentry_in(manifest.dentry_records_path, std::ios::binary);
    std::ofstream inode_blob_out(manifest.inode_blob_path, std::ios::binary | std::ios::trunc);
    if (!inode_in || !dentry_in || !inode_blob_out) {
        if (error) {
            *error = "failed to open masstree importer inputs/outputs";
        }
        return false;
    }

    std::vector<uint64_t> inode_samples;
    std::vector<DentryRecordView> dentry_samples;
    inode_samples.reserve(request.verify_inode_samples);
    dentry_samples.reserve(request.verify_dentry_samples);

    Result local_result;
    local_result.start_cursor = request.start_cursor;
    local_result.end_cursor = request.start_cursor;
    local_result.total_file_bytes = "0";

    MasstreeOpticalAllocator allocator(optical_profile, request.start_cursor);
    MasstreeDecimalAccumulator total_file_bytes_accumulator;
    InodeRecordView inode_record;
    while (ReadInodeRecord(&inode_in, &inode_record, error)) {
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
            if (local_result.file_count == 0) {
                local_result.start_global_image_id = global_image_id;
            }
            local_result.end_global_image_id = global_image_id;
            ++local_result.file_count;
            total_file_bytes_accumulator.Add(attr.size());
        }

        std::string encoded_blob_payload;
        if (!MasstreeInodeRecordCodec::Encode(blob_record, &encoded_blob_payload, error)) {
            return false;
        }

        const uint64_t current_offset = local_result.inode_blob_bytes;
        std::string blob_wire_record;
        blob_wire_record.reserve(sizeof(uint32_t) + encoded_blob_payload.size());
        AppendLe32(&blob_wire_record, static_cast<uint32_t>(encoded_blob_payload.size()));
        blob_wire_record.append(encoded_blob_payload);
        if (!WriteExact(&inode_blob_out, blob_wire_record)) {
            if (error) {
                *error = "failed to write inode blob payload";
            }
            return false;
        }
        if (!active_runtime->PutInodeOffset(manifest.namespace_id, inode_record.inode_id, current_offset, error)) {
            return false;
        }
        local_result.inode_blob_bytes += blob_wire_record.size();
        ++local_result.inode_imported;
        if (inode_samples.size() < request.verify_inode_samples) {
            inode_samples.push_back(inode_record.inode_id);
        }
    }
    if (error && !error->empty()) {
        return false;
    }

    DentryRecordView dentry_record;
    while (ReadDentryRecord(&dentry_in, &dentry_record, error)) {
        if (!active_runtime->PutDentryValue(manifest.namespace_id,
                                            dentry_record.parent_inode,
                                            dentry_record.name,
                                            dentry_record.child_inode,
                                            dentry_record.type,
                                            error)) {
            return false;
        }
        ++local_result.dentry_imported;
        if (dentry_samples.size() < request.verify_dentry_samples) {
            dentry_samples.push_back(dentry_record);
        }
    }
    if (error && !error->empty()) {
        return false;
    }

    inode_blob_out.flush();
    if (!inode_blob_out.good()) {
        if (error) {
            *error = "failed to flush inode blob";
        }
        return false;
    }
    inode_blob_out.close();
    if (inode_blob_out.fail()) {
        if (error) {
            *error = "failed to close inode blob";
        }
        return false;
    }

    local_result.total_file_bytes = total_file_bytes_accumulator.ToString();
    local_result.avg_file_size_bytes = local_result.file_count == 0 ? 0 :
                                       total_file_bytes_accumulator.DivideBy(local_result.file_count);
    local_result.end_cursor = allocator.cursor();

    for (uint64_t inode_id : inode_samples) {
        uint64_t offset = 0;
        if (!active_runtime->GetInodeOffset(manifest.namespace_id, inode_id, &offset, error)) {
            if (error && error->empty()) {
                *error = "masstree importer inode verification failed";
            }
            return false;
        }
        ++local_result.verified_inode_samples;
    }

    for (const auto& sample : dentry_samples) {
        MasstreePackedDentryValue value;
        if (!active_runtime->GetDentryValue(manifest.namespace_id,
                                            sample.parent_inode,
                                            sample.name,
                                            &value,
                                            error)) {
            if (error && error->empty()) {
                *error = "masstree importer dentry verification failed";
            }
            return false;
        }
        if (value.child_inode != sample.child_inode || value.type != sample.type) {
            if (error) {
                *error = "masstree importer dentry verification mismatch";
            }
            return false;
        }
        ++local_result.verified_dentry_samples;
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

    const std::filesystem::path staging_dir = std::filesystem::path(request.manifest_path).parent_path();
    const std::string summary_path = (staging_dir / "import_summary.txt").string();
    const std::string cluster_stats_path = (staging_dir / "cluster_stats.txt").string();
    const std::string allocation_summary_path = (staging_dir / "allocation_summary.txt").string();

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
