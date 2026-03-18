#pragma once

#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::mds {

struct FileArchiveCandidateEntry {
    uint64_t inode_id{0};
    std::string node_id;
    std::string node_address;
    std::string disk_id;
    uint64_t file_size{0};
    uint32_t object_count{0};
    uint64_t last_access_ts_ms{0};
    std::string archive_state{"pending"};
    uint64_t version{0};
    uint64_t report_ts_ms{0};
};

class FileArchiveCandidateQueue {
public:
    struct PushResult {
        size_t accepted{0};
        size_t dropped{0};
        size_t evicted{0};
    };

    explicit FileArchiveCandidateQueue(size_t max_size = 0) : max_size_(max_size) {}

    PushResult PushBatch(const std::vector<FileArchiveCandidateEntry>& candidates) {
        std::lock_guard<std::mutex> lock(mu_);
        PushResult result;
        for (const auto& candidate : candidates) {
            bool evicted = false;
            if (PushLocked(candidate, &evicted)) {
                ++result.accepted;
                ++total_accepted_;
                if (evicted) {
                    ++result.evicted;
                    ++total_evicted_;
                }
            } else {
                ++result.dropped;
                ++total_dropped_;
            }
        }
        return result;
    }

    bool PopTop(FileArchiveCandidateEntry* out) {
        if (!out) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mu_);
        PruneBestLocked();
        while (!best_.empty()) {
            const HeapEntry top = best_.top();
            best_.pop();
            auto it = latest_.find(top.inode_id);
            if (it == latest_.end() || it->second.version != top.version) {
                continue;
            }
            *out = it->second.candidate;
            latest_.erase(it);
            return true;
        }
        return false;
    }

private:
    struct CandidateRecord {
        FileArchiveCandidateEntry candidate;
        uint64_t version{0};
    };

    struct HeapEntry {
        uint64_t last_access_ts_ms{0};
        uint64_t report_ts_ms{0};
        uint64_t inode_id{0};
        uint64_t version{0};
    };

    struct BestCompare {
        bool operator()(const HeapEntry& a, const HeapEntry& b) const {
            if (a.last_access_ts_ms != b.last_access_ts_ms) {
                return a.last_access_ts_ms > b.last_access_ts_ms;
            }
            if (a.report_ts_ms != b.report_ts_ms) {
                return a.report_ts_ms > b.report_ts_ms;
            }
            return a.inode_id > b.inode_id;
        }
    };

    struct WorstCompare {
        bool operator()(const HeapEntry& a, const HeapEntry& b) const {
            if (a.last_access_ts_ms != b.last_access_ts_ms) {
                return a.last_access_ts_ms < b.last_access_ts_ms;
            }
            if (a.report_ts_ms != b.report_ts_ms) {
                return a.report_ts_ms < b.report_ts_ms;
            }
            return a.inode_id < b.inode_id;
        }
    };

    bool IsActiveVersionLocked(const HeapEntry& entry) const {
        auto it = latest_.find(entry.inode_id);
        return it != latest_.end() && it->second.version == entry.version;
    }

    bool IsBetterThanLocked(const FileArchiveCandidateEntry& lhs, const HeapEntry& rhs) const {
        if (lhs.last_access_ts_ms != rhs.last_access_ts_ms) {
            return lhs.last_access_ts_ms < rhs.last_access_ts_ms;
        }
        if (lhs.report_ts_ms != rhs.report_ts_ms) {
            return lhs.report_ts_ms > rhs.report_ts_ms;
        }
        return lhs.inode_id < rhs.inode_id;
    }

    void PruneBestLocked() {
        while (!best_.empty() && !IsActiveVersionLocked(best_.top())) {
            best_.pop();
        }
    }

    void PruneWorstLocked() {
        while (!worst_.empty() && !IsActiveVersionLocked(worst_.top())) {
            worst_.pop();
        }
    }

    bool PushLocked(const FileArchiveCandidateEntry& candidate, bool* evicted) {
        if (evicted) {
            *evicted = false;
        }
        if (candidate.inode_id == 0 || candidate.disk_id.empty()) {
            return false;
        }
        if (!candidate.archive_state.empty() && candidate.archive_state != "pending") {
            return false;
        }
        const bool is_new = latest_.find(candidate.inode_id) == latest_.end();
        if (max_size_ > 0 && is_new && latest_.size() >= max_size_) {
            PruneWorstLocked();
            if (worst_.empty()) {
                return false;
            }
            const HeapEntry worst_entry = worst_.top();
            if (!IsBetterThanLocked(candidate, worst_entry)) {
                return false;
            }
            latest_.erase(worst_entry.inode_id);
            if (evicted) {
                *evicted = true;
            }
        }

        const uint64_t version = ++seq_;
        CandidateRecord& record = latest_[candidate.inode_id];
        record.candidate = candidate;
        record.version = version;
        const HeapEntry entry{candidate.last_access_ts_ms, candidate.report_ts_ms, candidate.inode_id, version};
        best_.push(entry);
        worst_.push(entry);
        return true;
    }

    size_t max_size_{0};
    std::mutex mu_;
    uint64_t seq_{0};
    uint64_t total_accepted_{0};
    uint64_t total_dropped_{0};
    uint64_t total_evicted_{0};
    std::unordered_map<uint64_t, CandidateRecord> latest_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, BestCompare> best_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, WorstCompare> worst_;
};

} // namespace zb::mds
