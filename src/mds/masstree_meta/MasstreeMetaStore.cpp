#include "MasstreeMetaStore.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string_view>

#include "MasstreeInodeRecordCodec.h"
#include "MasstreeOpticalProfile.h"

namespace zb::mds {

namespace {

constexpr const char* kMasstreeReaddirTokenPrefix = "zb-masstree-readdir-v1:";

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

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8U);
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
            *error = "invalid masstree inode record read args";
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
            *error = "failed to read masstree inode record header";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!ReadExact(input, len_buf, sizeof(len_buf))) {
        if (error) {
            *error = "corrupted masstree inode record length";
        }
        return false;
    }
    record->inode_id = DecodeLe64(inode_buf);
    const uint32_t payload_len = DecodeLe32(len_buf);
    if (!ReadString(input, payload_len, &record->payload)) {
        if (error) {
            *error = "corrupted masstree inode record payload";
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
            *error = "invalid masstree dentry record read args";
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
            *error = "failed to read masstree dentry record header";
        }
        return false;
    }
    char name_len_buf[sizeof(uint16_t)] = {};
    if (!ReadExact(input, name_len_buf, sizeof(name_len_buf))) {
        if (error) {
            *error = "corrupted masstree dentry record name length";
        }
        return false;
    }
    const uint16_t name_len = DecodeLe16(name_len_buf);
    if (!ReadString(input, name_len, &record->name)) {
        if (error) {
            *error = "corrupted masstree dentry record name";
        }
        return false;
    }
    char child_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, child_buf, sizeof(child_buf))) {
        if (error) {
            *error = "corrupted masstree dentry record child inode";
        }
        return false;
    }
    char raw_type = 0;
    if (!ReadExact(input, &raw_type, sizeof(raw_type))) {
        if (error) {
            *error = "corrupted masstree dentry record type";
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

char EncodeHexNibble(unsigned char value) {
    return static_cast<char>(value < 10 ? ('0' + value) : ('a' + (value - 10)));
}

bool DecodeHexNibble(char ch, unsigned char* value) {
    if (!value) {
        return false;
    }
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (uch >= '0' && uch <= '9') {
        *value = static_cast<unsigned char>(uch - '0');
        return true;
    }
    if (uch >= 'a' && uch <= 'f') {
        *value = static_cast<unsigned char>(uch - 'a' + 10);
        return true;
    }
    if (uch >= 'A' && uch <= 'F') {
        *value = static_cast<unsigned char>(uch - 'A' + 10);
        return true;
    }
    return false;
}

std::string HexEncode(std::string_view input) {
    std::string output;
    output.reserve(input.size() * 2U);
    for (unsigned char ch : input) {
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch >> 4U)));
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch & 0x0FU)));
    }
    return output;
}

bool HexDecode(std::string_view input, std::string* output) {
    if (!output || (input.size() % 2U) != 0U) {
        return false;
    }
    output->clear();
    output->reserve(input.size() / 2U);
    for (size_t i = 0; i < input.size(); i += 2U) {
        unsigned char hi = 0;
        unsigned char lo = 0;
        if (!DecodeHexNibble(input[i], &hi) || !DecodeHexNibble(input[i + 1U], &lo)) {
            return false;
        }
        output->push_back(static_cast<char>((hi << 4U) | lo));
    }
    return true;
}

std::string EncodeMasstreeReaddirToken(const std::string& generation_id, const std::string& last_name) {
    std::string payload = generation_id;
    payload.push_back('\0');
    payload.append(last_name);
    return std::string(kMasstreeReaddirTokenPrefix) + HexEncode(payload);
}

bool DecodeMasstreeReaddirToken(const std::string& token,
                                std::string* generation_id,
                                std::string* last_name) {
    if (!generation_id || !last_name ||
        token.rfind(kMasstreeReaddirTokenPrefix, 0) != 0) {
        return false;
    }
    std::string payload;
    if (!HexDecode(token.substr(std::char_traits<char>::length(kMasstreeReaddirTokenPrefix)), &payload)) {
        return false;
    }
    const size_t separator = payload.find('\0');
    if (separator == std::string::npos) {
        return false;
    }
    *generation_id = payload.substr(0, separator);
    *last_name = payload.substr(separator + 1U);
    return true;
}

} // namespace

bool MasstreeMetaStore::ResolvePath(const MasstreeNamespaceRoute& route,
                                    const std::string& path,
                                    uint64_t* inode_id,
                                    zb::rpc::InodeAttr* attr,
                                    std::string* error) const {
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }

    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    std::string relative_path;
    if (!StripRoutePrefix(route.path_prefix, path, &relative_path, error)) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(relative_path);
    uint64_t current = generation->manifest->root_inode_id;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return ReadInodeAttr(*generation, current, attr, error);
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    for (const auto& name : parts) {
        uint64_t next = 0;
        if (!FindDentry(*generation, current, name, &next, nullptr, error)) {
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return ReadInodeAttr(*generation, current, attr, error);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::GetInode(const MasstreeNamespaceRoute& route,
                                 uint64_t inode_id,
                                 zb::rpc::InodeAttr* attr,
                                 std::string* error) const {
    if (!attr || inode_id == 0) {
        if (error) {
            *error = "invalid masstree inode lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    return ReadInodeAttr(*generation, inode_id, attr, error);
}

bool MasstreeMetaStore::GetOpticalFileLocation(const MasstreeNamespaceRoute& route,
                                               uint64_t inode_id,
                                               zb::rpc::OpticalFileLocation* location,
                                               std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid masstree optical location lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    MasstreeInodeRecord record;
    if (!ReadInodeRecord(*generation, inode_id, &record, error)) {
        return false;
    }
    if (record.attr.type() != zb::rpc::INODE_FILE || !record.has_optical_image) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const MasstreeOpticalProfile profile = MasstreeOpticalProfile::Fixed();
    uint32_t node_index = 0;
    uint32_t disk_index = 0;
    uint32_t image_index_in_disk = 0;
    if (!profile.DecodeGlobalImageId(record.optical_image_global_id,
                                     &node_index,
                                     &disk_index,
                                     &image_index_in_disk)) {
        if (error) {
            *error = "invalid masstree optical image id";
        }
        return false;
    }

    location->Clear();
    location->set_node_id(profile.NodeId(node_index));
    location->set_disk_id(profile.DiskId(disk_index));
    location->set_image_id(profile.ImageId(record.optical_image_global_id));
    location->set_file_id("obj-" + std::to_string(inode_id) + "-0");
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::Readdir(const MasstreeNamespaceRoute& route,
                                const std::string& path,
                                const std::string& start_after,
                                uint32_t limit,
                                std::vector<zb::rpc::Dentry>* entries,
                                bool* has_more,
                                std::string* next_token,
                                std::string* error) const {
    if (!entries) {
        if (error) {
            *error = "entries output is null";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    uint64_t inode_id = 0;
    zb::rpc::InodeAttr attr;
    if (!ResolvePath(route, path, &inode_id, &attr, error)) {
        return false;
    }
    if (attr.type() != zb::rpc::INODE_DIR) {
        if (error) {
            *error = "not a directory";
        }
        return false;
    }

    std::string decoded_start_after;
    if (start_after.empty()) {
        decoded_start_after.clear();
    } else if (start_after.rfind(kMasstreeReaddirTokenPrefix, 0) == 0) {
        std::string token_generation_id;
        if (!DecodeMasstreeReaddirToken(start_after, &token_generation_id, &decoded_start_after)) {
            if (error) {
                *error = "invalid masstree readdir token";
            }
            return false;
        }
        if (token_generation_id != generation->manifest->generation_id) {
            if (error) {
                *error = "masstree readdir token generation mismatch";
            }
            return false;
        }
    } else {
        decoded_start_after = start_after;
    }

    std::vector<MasstreeIndexRuntime::DentryScanEntry> scanned;
    std::string raw_next_name;
    if (!generation->runtime->ScanDentryValues(generation->manifest->namespace_id,
                                               inode_id,
                                               decoded_start_after,
                                               limit,
                                               &scanned,
                                               has_more,
                                               &raw_next_name,
                                               error)) {
        return false;
    }

    entries->reserve(scanned.size());
    for (const auto& hit : scanned) {
        zb::rpc::Dentry entry;
        entry.set_name(hit.name);
        entry.set_inode_id(hit.value.child_inode);
        entry.set_type(hit.value.type);
        entries->push_back(std::move(entry));
    }
    if (next_token && has_more && *has_more && !raw_next_name.empty()) {
        *next_token = EncodeMasstreeReaddirToken(generation->manifest->generation_id, raw_next_name);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::EnsureGenerationLoaded(const MasstreeNamespaceRoute& route,
                                               std::shared_ptr<LoadedGeneration>* generation,
                                               std::string* error) const {
    if (!generation) {
        if (error) {
            *error = "generation output is null";
        }
        return false;
    }
    const std::string manifest_path = ResolveManifestPath(route);
    if (manifest_path.empty()) {
        if (error) {
            *error = "masstree manifest path is empty";
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        auto it = generation_cache_.find(manifest_path);
        if (it != generation_cache_.end()) {
            *generation = it->second;
            if (error) {
                error->clear();
            }
            return true;
        }
    }

    MasstreeNamespaceManifest parsed;
    if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path, &parsed, error)) {
        return false;
    }

    auto loaded = std::make_shared<LoadedGeneration>();
    loaded->manifest = std::make_shared<MasstreeNamespaceManifest>(std::move(parsed));
    loaded->runtime = std::make_shared<MasstreeIndexRuntime>();
    if (!loaded->runtime->Init(error)) {
        return false;
    }
    if (!LoadGenerationData(*loaded->manifest, loaded->runtime.get(), error)) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        generation_cache_[manifest_path] = loaded;
    }
    *generation = std::move(loaded);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::LoadGenerationData(const MasstreeNamespaceManifest& manifest,
                                           MasstreeIndexRuntime* runtime,
                                           std::string* error) const {
    if (!runtime) {
        if (error) {
            *error = "masstree runtime is null";
        }
        return false;
    }

    std::ifstream inode_blob_in(manifest.inode_blob_path, std::ios::binary);
    std::ifstream dentry_in(manifest.dentry_records_path, std::ios::binary);
    if (!inode_blob_in || !dentry_in) {
        if (error) {
            *error = "failed to open masstree generation materialization inputs";
        }
        return false;
    }

    uint64_t inode_count = 0;
    uint64_t dentry_count = 0;
    uint64_t current_offset = 0;
    while (true) {
        char len_buf[sizeof(uint32_t)] = {};
        if (!ReadExact(&inode_blob_in, len_buf, sizeof(len_buf))) {
            if (inode_blob_in.eof()) {
                break;
            }
            if (error) {
                *error = "failed to read masstree inode blob header";
            }
            return false;
        }
        const uint32_t payload_len = DecodeLe32(len_buf);
        std::string payload(payload_len, '\0');
        if (payload_len != 0 &&
            !inode_blob_in.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
            if (error) {
                *error = "failed to read masstree inode blob payload";
            }
            return false;
        }
        MasstreeInodeRecord inode_record;
        if (!MasstreeInodeRecordCodec::Decode(payload, &inode_record, error)) {
            return false;
        }
        if (!runtime->PutInodeOffset(manifest.namespace_id, inode_record.attr.inode_id(), current_offset, error)) {
            return false;
        }
        current_offset += static_cast<uint64_t>(sizeof(uint32_t) + payload_len);
        ++inode_count;
    }

    DentryRecordView dentry_record;
    while (ReadDentryRecord(&dentry_in, &dentry_record, error)) {
        if (!runtime->PutDentryValue(manifest.namespace_id,
                                     dentry_record.parent_inode,
                                     dentry_record.name,
                                     dentry_record.child_inode,
                                     dentry_record.type,
                                     error)) {
            return false;
        }
        ++dentry_count;
    }
    if (error && !error->empty()) {
        return false;
    }

    if (inode_count != manifest.inode_count) {
        if (error) {
            *error = "masstree generation inode count mismatch";
        }
        return false;
    }
    if (dentry_count != manifest.dentry_count) {
        if (error) {
            *error = "masstree generation dentry count mismatch";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::ReadInodeAttr(const LoadedGeneration& generation,
                                      uint64_t inode_id,
                                      zb::rpc::InodeAttr* attr,
                                      std::string* error) const {
    MasstreeInodeRecord record;
    if (!ReadInodeRecord(generation, inode_id, &record, error)) {
        return false;
    }
    *attr = std::move(record.attr);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::ReadInodeRecord(const LoadedGeneration& generation,
                                        uint64_t inode_id,
                                        MasstreeInodeRecord* record,
                                        std::string* error) const {
    if (!record || inode_id == 0) {
        if (error) {
            *error = "invalid masstree inode record read args";
        }
        return false;
    }
    uint64_t offset = 0;
    if (!generation.runtime->GetInodeOffset(generation.manifest->namespace_id, inode_id, &offset, error)) {
        return false;
    }

    std::ifstream blob(generation.manifest->inode_blob_path, std::ios::binary);
    if (!blob) {
        if (error) {
            *error = "failed to open masstree inode blob: " + generation.manifest->inode_blob_path;
        }
        return false;
    }
    blob.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!blob.good()) {
        if (error) {
            *error = "failed to seek masstree inode blob";
        }
        return false;
    }

    char len_buf[sizeof(uint32_t)] = {};
    if (!blob.read(len_buf, static_cast<std::streamsize>(sizeof(len_buf))).good()) {
        if (error) {
            *error = "failed to read masstree inode blob header";
        }
        return false;
    }
    const uint32_t payload_len = DecodeLe32(len_buf);
    std::string payload(payload_len, '\0');
    if (payload_len != 0 &&
        !blob.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
        if (error) {
            *error = "failed to read masstree inode blob payload";
        }
        return false;
    }
    if (!MasstreeInodeRecordCodec::Decode(payload, record, error)) {
        return false;
    }
    if (record->attr.inode_id() != inode_id) {
        if (error) {
            *error = "masstree inode blob payload inode mismatch";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::FindDentry(const LoadedGeneration& generation,
                                   uint64_t parent_inode,
                                   const std::string& name,
                                   uint64_t* child_inode,
                                   zb::rpc::InodeType* type,
                                   std::string* error) const {
    MasstreePackedDentryValue value;
    if (!generation.runtime->GetDentryValue(generation.manifest->namespace_id,
                                            parent_inode,
                                            name,
                                            &value,
                                            error)) {
        return false;
    }
    if (child_inode) {
        *child_inode = value.child_inode;
    }
    if (type) {
        *type = value.type;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::StripRoutePrefix(const std::string& route_prefix,
                                         const std::string& path,
                                         std::string* relative_path,
                                         std::string* error) const {
    if (!relative_path) {
        if (error) {
            *error = "relative_path output is null";
        }
        return false;
    }
    std::string normalized_route;
    std::string normalized_path;
    if (!NormalizePath(route_prefix, &normalized_route) || !NormalizePath(path, &normalized_path)) {
        if (error) {
            *error = "invalid masstree route path";
        }
        return false;
    }
    if (normalized_path == normalized_route) {
        *relative_path = "/";
        if (error) {
            error->clear();
        }
        return true;
    }
    const std::string prefix = normalized_route + "/";
    if (normalized_path.rfind(prefix, 0) != 0) {
        if (error) {
            *error = "path does not belong to masstree route";
        }
        return false;
    }
    *relative_path = normalized_path.substr(normalized_route.size());
    if (relative_path->empty()) {
        *relative_path = "/";
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string MasstreeMetaStore::ResolveManifestPath(const MasstreeNamespaceRoute& route) {
    return route.manifest_path;
}

} // namespace zb::mds
