#include "ArchiveBatchStager.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

namespace zb::mds {

uint64_t ArchiveBatchStager::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string ArchiveBatchStager::BuildBatchId(uint64_t now_ms) {
    return "batch_" + std::to_string(now_ms);
}

std::vector<std::string> ArchiveBatchStager::SplitTabs(const std::string& line) {
    std::vector<std::string> out;
    std::string token;
    std::istringstream stream(line);
    while (std::getline(stream, token, '\t')) {
        out.push_back(token);
    }
    return out;
}

bool ArchiveBatchStager::Init(const std::string& staging_dir, Options options, std::string* error) {
    if (staging_dir.empty()) {
        if (error) {
            *error = "archive staging dir is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    staging_dir_ = staging_dir;
    object_dir_ = (fs::path(staging_dir_) / "objects").string();
    manifest_path_ = (fs::path(staging_dir_) / "batch.manifest").string();
    options_ = options;
    if (options_.disc_size_bytes == 0) {
        options_.disc_size_bytes = 1;
    }
    if (!EnsureDirsLocked(error)) {
        return false;
    }
    objects_.clear();
    object_order_.clear();
    batch_id_.clear();
    created_ts_ms_ = 0;
    sealed_ = false;
    total_bytes_ = 0;
    if (!LoadManifestLocked(error)) {
        return false;
    }
    if (batch_id_.empty()) {
        batch_id_ = BuildBatchId(NowMilliseconds());
        created_ts_ms_ = NowMilliseconds();
        if (!PersistManifestLocked(error)) {
            return false;
        }
    }
    inited_ = true;
    return true;
}

bool ArchiveBatchStager::StageObject(const ArchiveCandidateEntry& candidate,
                                    const std::string& lease_id,
                                    const std::string& op_id,
                                    const std::string& object_key,
                                    uint64_t version,
                                    const std::string& data,
                                    bool* inserted,
                                    bool* deferred,
                                    std::string* error) {
    if (inserted) {
        *inserted = false;
    }
    if (deferred) {
        *deferred = false;
    }
    const std::string object_id = candidate.ArchiveObjectId();
    if (object_id.empty()) {
        if (error) {
            *error = "archive object id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_) {
        if (error) {
            *error = "archive batch stager not initialized";
        }
        return false;
    }
    if (sealed_) {
        if (error) {
            *error = "current archive batch is sealed";
        }
        return false;
    }
    auto existing = objects_.find(object_id);
    if (existing != objects_.end()) {
        return true;
    }
    const uint64_t data_size = static_cast<uint64_t>(data.size());
    if (data_size > options_.disc_size_bytes) {
        if (error) {
            *error = "object size exceeds disc capacity";
        }
        return false;
    }
    if (total_bytes_ > 0 && total_bytes_ + data_size > options_.disc_size_bytes) {
        // Keep current batch <= disc capacity and spill this object to the next batch.
        sealed_ = true;
        if (!PersistManifestLocked(error)) {
            return false;
        }
        if (deferred) {
            *deferred = true;
        }
        return true;
    }

    std::string data_path = ObjectFilePathLocked(object_id);
    const std::string tmp_path = data_path + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!out.is_open()) {
            if (error) {
                *error = "failed to open staging object temp file";
            }
            return false;
        }
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
        out.flush();
        if (!out.good()) {
            if (error) {
                *error = "failed to write staging object data";
            }
            return false;
        }
    }
    std::error_code ec;
    fs::remove(data_path, ec);
    ec.clear();
    fs::rename(tmp_path, data_path, ec);
    if (ec) {
        if (error) {
            *error = "failed to move staging object data";
        }
        return false;
    }

    StagedArchiveObject staged;
    staged.candidate = candidate;
    staged.lease_id = lease_id;
    staged.op_id = op_id;
    staged.object_key = object_key;
    staged.version = version;
    staged.size_bytes = data_size;
    staged.data_file = data_path;
    staged.done = false;

    objects_[object_id] = std::move(staged);
    object_order_.push_back(object_id);
    total_bytes_ += data_size;
    if (IsReadyToSealLocked()) {
        sealed_ = true;
    }
    if (!PersistManifestLocked(error)) {
        auto it = objects_.find(object_id);
        if (it != objects_.end()) {
            objects_.erase(it);
        }
        if (!object_order_.empty() && object_order_.back() == object_id) {
            object_order_.pop_back();
        }
        total_bytes_ = total_bytes_ >= data_size
                           ? (total_bytes_ - data_size)
                           : 0;
        (void)RemoveObjectFileNoThrow(data_path);
        return false;
    }
    if (inserted) {
        *inserted = true;
    }
    return true;
}

bool ArchiveBatchStager::HasSealedBatch() const {
    std::lock_guard<std::mutex> lock(mu_);
    return inited_ && sealed_;
}

bool ArchiveBatchStager::SealIfReady(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_) {
        if (error) {
            *error = "archive batch stager not initialized";
        }
        return false;
    }
    if (sealed_) {
        return true;
    }
    if (!IsReadyToSealLocked()) {
        return true;
    }
    sealed_ = true;
    return PersistManifestLocked(error);
}

std::vector<StagedArchiveObject> ArchiveBatchStager::SnapshotSealedBatch() const {
    std::vector<StagedArchiveObject> out;
    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_ || !sealed_) {
        return out;
    }
    out.reserve(object_order_.size());
    for (const auto& object_id : object_order_) {
        auto it = objects_.find(object_id);
        if (it == objects_.end()) {
            continue;
        }
        if (it->second.done) {
            continue;
        }
        out.push_back(it->second);
    }
    return out;
}

bool ArchiveBatchStager::ReadObjectData(const StagedArchiveObject& staged, std::string* data, std::string* error) const {
    if (!data) {
        if (error) {
            *error = "output data buffer is null";
        }
        return false;
    }
    std::ifstream in(staged.data_file, std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        if (error) {
            *error = "failed to open staged data file";
        }
        return false;
    }
    std::ostringstream oss;
    oss << in.rdbuf();
    if (!in.good() && !in.eof()) {
        if (error) {
            *error = "failed to read staged data file";
        }
        return false;
    }
    *data = oss.str();
    return true;
}

bool ArchiveBatchStager::UpdateObjectLease(const std::string& object_id,
                                     const std::string& lease_id,
                                     const std::string& op_id,
                                     uint64_t version,
                                     std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = objects_.find(object_id);
    if (it == objects_.end()) {
        if (error) {
            *error = "staged object not found";
        }
        return false;
    }
    it->second.lease_id = lease_id;
    it->second.op_id = op_id;
    it->second.version = version;
    return PersistManifestLocked(error);
}

bool ArchiveBatchStager::MarkObjectDone(const std::string& object_id, std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = objects_.find(object_id);
    if (it == objects_.end()) {
        if (error) {
            *error = "staged object not found";
        }
        return false;
    }
    if (it->second.done) {
        return true;
    }
    it->second.done = true;
    (void)RemoveObjectFileNoThrow(it->second.data_file);
    return PersistManifestLocked(error);
}

bool ArchiveBatchStager::RemoveObject(const std::string& object_id, std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = objects_.find(object_id);
    if (it == objects_.end()) {
        return true;
    }
    total_bytes_ = total_bytes_ > it->second.size_bytes ? (total_bytes_ - it->second.size_bytes) : 0;
    (void)RemoveObjectFileNoThrow(it->second.data_file);
    objects_.erase(it);
    object_order_.erase(std::remove(object_order_.begin(), object_order_.end(), object_id), object_order_.end());
    return PersistManifestLocked(error);
}

bool ArchiveBatchStager::ResetIfDrained(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_ || !sealed_) {
        return true;
    }
    for (const auto& item : objects_) {
        if (!item.second.done) {
            return true;
        }
    }
    objects_.clear();
    object_order_.clear();
    total_bytes_ = 0;
    sealed_ = false;
    created_ts_ms_ = NowMilliseconds();
    batch_id_ = BuildBatchId(created_ts_ms_);
    return PersistManifestLocked(error);
}

bool ArchiveBatchStager::ContainsObject(const std::string& object_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    return objects_.find(object_id) != objects_.end();
}

uint64_t ArchiveBatchStager::CurrentBytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return total_bytes_;
}

uint64_t ArchiveBatchStager::DiscSizeBytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return options_.disc_size_bytes;
}

std::string ArchiveBatchStager::CurrentBatchId() const {
    std::lock_guard<std::mutex> lock(mu_);
    return batch_id_;
}

bool ArchiveBatchStager::LoadManifestLocked(std::string* error) {
    std::ifstream in(manifest_path_);
    if (!in.is_open()) {
        return true;
    }
    std::string line;
    size_t line_no = 0;
    uint64_t recalculated_total = 0;
    while (std::getline(in, line)) {
        ++line_no;
        if (line.empty()) {
            continue;
        }
        std::vector<std::string> parts = SplitTabs(line);
        if (parts.empty()) {
            continue;
        }
        if (parts[0] == "B") {
            if (parts.size() != 6) {
                if (error) {
                    *error = "invalid staging manifest header";
                }
                return false;
            }
            batch_id_ = parts[1];
            try {
                created_ts_ms_ = static_cast<uint64_t>(std::stoull(parts[2]));
                sealed_ = parts[3] == "1";
                total_bytes_ = static_cast<uint64_t>(std::stoull(parts[4]));
                (void)std::stoull(parts[5]);
            } catch (...) {
                if (error) {
                    *error = "invalid staging manifest header values";
                }
                return false;
            }
            continue;
        }
        if (parts[0] != "E" || parts.size() != 19) {
            if (error) {
                *error = "invalid staging manifest entry at line " + std::to_string(line_no);
            }
            return false;
        }
        StagedArchiveObject staged;
        staged.candidate.node_id = parts[2];
        staged.candidate.node_address = parts[3];
        staged.candidate.disk_id = parts[4];
        staged.candidate.object_id = parts[1];
        try {
            staged.candidate.last_access_ts_ms = static_cast<uint64_t>(std::stoull(parts[5]));
            staged.candidate.size_bytes = static_cast<uint64_t>(std::stoull(parts[6]));
            staged.candidate.checksum = static_cast<uint64_t>(std::stoull(parts[7]));
            staged.candidate.heat_score = std::stod(parts[8]);
            staged.candidate.archive_state = parts[9];
            staged.candidate.version = static_cast<uint64_t>(std::stoull(parts[10]));
            staged.candidate.score = std::stod(parts[11]);
            staged.candidate.report_ts_ms = static_cast<uint64_t>(std::stoull(parts[12]));
            staged.lease_id = parts[13];
            staged.op_id = parts[14];
            staged.object_key = parts[15];
            staged.version = static_cast<uint64_t>(std::stoull(parts[16]));
            staged.size_bytes = static_cast<uint64_t>(std::stoull(parts[17]));
            staged.data_file = parts[18];
            staged.done = false;
        } catch (...) {
            if (error) {
                *error = "invalid staging manifest values at line " + std::to_string(line_no);
            }
            return false;
        }
        if (staged.data_file.empty()) {
            staged.data_file = ObjectFilePathLocked(staged.candidate.ArchiveObjectId());
        }
        objects_[staged.candidate.ArchiveObjectId()] = staged;
        object_order_.push_back(staged.candidate.ArchiveObjectId());
        if (!staged.done) {
            recalculated_total += staged.size_bytes;
        }
    }
    if (total_bytes_ == 0) {
        total_bytes_ = recalculated_total;
    }
    return true;
}

bool ArchiveBatchStager::PersistManifestLocked(std::string* error) const {
    if (!EnsureDirsLocked(error)) {
        return false;
    }
    const std::string tmp = manifest_path_ + ".tmp";
    std::ofstream out(tmp, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        if (error) {
            *error = "failed to open staging manifest temp file";
        }
        return false;
    }
    out << "B\t" << batch_id_ << "\t" << created_ts_ms_ << "\t" << (sealed_ ? 1 : 0) << "\t" << total_bytes_ << "\t"
        << options_.disc_size_bytes << "\n";
    for (const auto& object_id : object_order_) {
        auto it = objects_.find(object_id);
        if (it == objects_.end()) {
            continue;
        }
        const StagedArchiveObject& staged = it->second;
        if (staged.done) {
            continue;
        }
        out << "E\t"
            << staged.candidate.ArchiveObjectId() << "\t"
            << staged.candidate.node_id << "\t"
            << staged.candidate.node_address << "\t"
            << staged.candidate.disk_id << "\t"
            << staged.candidate.last_access_ts_ms << "\t"
            << staged.candidate.size_bytes << "\t"
            << staged.candidate.checksum << "\t"
            << staged.candidate.heat_score << "\t"
            << staged.candidate.archive_state << "\t"
            << staged.candidate.version << "\t"
            << staged.candidate.score << "\t"
            << staged.candidate.report_ts_ms << "\t"
            << staged.lease_id << "\t"
            << staged.op_id << "\t"
            << staged.object_key << "\t"
            << staged.version << "\t"
            << staged.size_bytes << "\t"
            << staged.data_file << "\n";
    }
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write staging manifest temp file";
        }
        return false;
    }
    out.close();

    std::error_code ec;
    fs::remove(manifest_path_, ec);
    ec.clear();
    fs::rename(tmp, manifest_path_, ec);
    if (ec) {
        if (error) {
            *error = "failed to rotate staging manifest";
        }
        return false;
    }
    return true;
}

bool ArchiveBatchStager::EnsureDirsLocked(std::string* error) const {
    std::error_code ec;
    fs::create_directories(staging_dir_, ec);
    if (ec) {
        if (error) {
            *error = "failed to create staging dir";
        }
        return false;
    }
    ec.clear();
    fs::create_directories(object_dir_, ec);
    if (ec) {
        if (error) {
            *error = "failed to create staging object dir";
        }
        return false;
    }
    return true;
}

bool ArchiveBatchStager::IsReadyToSealLocked() const {
    if (total_bytes_ == options_.disc_size_bytes) {
        return true;
    }
    if (options_.strict_full_disc) {
        return false;
    }
    if (options_.max_batch_age_ms == 0) {
        return false;
    }
    const uint64_t now_ms = NowMilliseconds();
    return created_ts_ms_ > 0 && now_ms > created_ts_ms_ && now_ms - created_ts_ms_ >= options_.max_batch_age_ms;
}

std::string ArchiveBatchStager::ObjectFilePathLocked(const std::string& object_id) const {
    return (fs::path(object_dir_) / (object_id + ".bin")).string();
}

bool ArchiveBatchStager::RemoveObjectFileNoThrow(const std::string& path) const {
    if (path.empty()) {
        return true;
    }
    std::error_code ec;
    fs::remove(path, ec);
    return true;
}

} // namespace zb::mds
