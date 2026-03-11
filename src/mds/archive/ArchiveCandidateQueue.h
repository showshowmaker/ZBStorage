#pragma once

#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::mds {

struct ArchiveCandidateEntry {
    std::string node_id;
    std::string node_address;
    std::string disk_id;
    std::string object_id;
    uint64_t last_access_ts_ms{0};
    uint64_t size_bytes{0};
    uint64_t checksum{0};
    double heat_score{0.0};
    std::string archive_state{"pending"};
    uint64_t version{0};
    double score{0.0};
    uint64_t report_ts_ms{0};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

class ArchiveCandidateQueue {
public:
    struct PushResult {
        size_t accepted{0};
        size_t dropped{0};
        size_t evicted{0};
    };

    struct QueueStats {
        uint64_t total_accepted{0};
        uint64_t total_dropped{0};
        uint64_t total_evicted{0};
        size_t size{0};
    };

    explicit ArchiveCandidateQueue(size_t max_size = 0) : max_size_(max_size) {}

    PushResult PushBatch(const std::vector<ArchiveCandidateEntry>& candidates) {
        std::lock_guard<std::mutex> lock(mu_);
        PushResult result;
        for (const auto& candidate : candidates) {
            bool evicted = false;
            if (PushLocked(candidate, &evicted)) {
                ++result.accepted;
                total_accepted_ += 1;
                if (evicted) {
                    ++result.evicted;
                    total_evicted_ += 1;
                }
            } else {
                ++result.dropped;
                total_dropped_ += 1;
            }
        }
        return result;
    }

    bool PopTop(ArchiveCandidateEntry* out) {
        if (!out) {
            return false;
        }
        std::lock_guard<std::mutex> lock(mu_);
        PruneTopLocked();
        while (!heap_.empty()) {
            HeapEntry top = heap_.top();
            heap_.pop();
            auto it = latest_.find(top.object_id);
            if (it == latest_.end()) {
                continue;
            }
            if (it->second.version != top.version) {
                continue;
            }
            *out = it->second.candidate;
            latest_.erase(it);
            return true;
        }
        return false;
    }

    size_t Size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return latest_.size();
    }

    QueueStats GetStats() const {
        std::lock_guard<std::mutex> lock(mu_);
        QueueStats out;
        out.total_accepted = total_accepted_;
        out.total_dropped = total_dropped_;
        out.total_evicted = total_evicted_;
        out.size = latest_.size();
        return out;
    }

private:
    struct CandidateRecord {
        ArchiveCandidateEntry candidate;
        uint64_t version{0};
    };

    struct HeapEntry {
        double score{0.0};
        uint64_t report_ts_ms{0};
        std::string object_id;
        uint64_t version{0};
    };

    struct HeapCompare {
        bool operator()(const HeapEntry& a, const HeapEntry& b) const {
            if (a.score != b.score) {
                return a.score < b.score;
            }
            return a.report_ts_ms < b.report_ts_ms;
        }
    };

    struct MinHeapCompare {
        bool operator()(const HeapEntry& a, const HeapEntry& b) const {
            if (a.score != b.score) {
                return a.score > b.score;
            }
            return a.report_ts_ms > b.report_ts_ms;
        }
    };

    bool IsActiveVersionLocked(const HeapEntry& entry) const {
        auto it = latest_.find(entry.object_id);
        if (it == latest_.end()) {
            return false;
        }
        return it->second.version == entry.version;
    }

    bool IsBetterThanLocked(const ArchiveCandidateEntry& lhs, const HeapEntry& rhs) const {
        if (lhs.score != rhs.score) {
            return lhs.score > rhs.score;
        }
        if (lhs.report_ts_ms != rhs.report_ts_ms) {
            return lhs.report_ts_ms > rhs.report_ts_ms;
        }
        return lhs.ArchiveObjectId() > rhs.object_id;
    }

    void PruneTopLocked() {
        while (!heap_.empty() && !IsActiveVersionLocked(heap_.top())) {
            heap_.pop();
        }
    }

    void PruneMinTopLocked() {
        while (!min_heap_.empty() && !IsActiveVersionLocked(min_heap_.top())) {
            min_heap_.pop();
        }
    }

    bool PushLocked(const ArchiveCandidateEntry& candidate, bool* evicted) {
        if (evicted) {
            *evicted = false;
        }
        const std::string object_id = candidate.ArchiveObjectId();
        if (object_id.empty() || candidate.disk_id.empty()) {
            return false;
        }
        if (!candidate.archive_state.empty() && candidate.archive_state != "pending") {
            return false;
        }
        const bool is_new_object = latest_.find(object_id) == latest_.end();
        if (max_size_ > 0 && is_new_object && latest_.size() >= max_size_) {
            PruneMinTopLocked();
            if (min_heap_.empty()) {
                return false;
            }
            const HeapEntry worst = min_heap_.top();
            if (!IsBetterThanLocked(candidate, worst)) {
                return false;
            }
            latest_.erase(worst.object_id);
            if (evicted) {
                *evicted = true;
            }
        }

        const uint64_t version = ++seq_;
        CandidateRecord& record = latest_[object_id];
        record.candidate = candidate;
        if (record.candidate.object_id.empty()) {
            record.candidate.object_id = object_id;
        }
        record.version = version;
        heap_.push({candidate.score, candidate.report_ts_ms, object_id, version});
        min_heap_.push({candidate.score, candidate.report_ts_ms, object_id, version});
        return true;
    }

    size_t max_size_{0};
    mutable std::mutex mu_;
    uint64_t seq_{0};
    uint64_t total_accepted_{0};
    uint64_t total_dropped_{0};
    uint64_t total_evicted_{0};
    std::unordered_map<std::string, CandidateRecord> latest_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapCompare> heap_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, MinHeapCompare> min_heap_;
};

} // namespace zb::mds
