#include "PGManager.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <unordered_set>
#include <utility>

namespace zb::mds {

namespace {

bool IsVirtualizedNodeType(NodeType type) {
    return type == NodeType::kVirtual || type == NodeType::kOptical;
}

uint64_t MixU64(uint64_t value) {
    value ^= value >> 33U;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33U;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33U;
    return value;
}

} // namespace

PGManager::PGManager() : PGManager(Options{}) {}

PGManager::PGManager(Options options) : options_(std::move(options)) {
    if (options_.pg_count == 0) {
        options_.pg_count = 1;
    }
    if (options_.replica == 0) {
        options_.replica = 1;
    }
    if (options_.history_limit == 0) {
        options_.history_limit = 1;
    }
}

uint32_t PGManager::ObjectToPg(std::string_view object_id, uint64_t epoch_hint) const {
    std::shared_ptr<PlacementView> view;
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        if (!GetViewLocked(epoch_hint, &view) && !GetViewLocked(0, &view)) {
            return 0;
        }
    }
    if (!view || view->pg_count == 0) {
        return 0;
    }
    return static_cast<uint32_t>(Hash64(object_id) % view->pg_count);
}

bool PGManager::ResolvePg(uint32_t pg_id, uint64_t epoch, PgReplicaSet* out, std::string* error) const {
    if (!out) {
        if (error) {
            *error = "ResolvePg output is null";
        }
        return false;
    }

    std::shared_ptr<PlacementView> view;
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        if (!GetViewLocked(epoch, &view)) {
            if (error) {
                *error = "placement view not found for epoch=" + std::to_string(epoch);
            }
            return false;
        }
    }
    if (!view || pg_id >= view->pg_to_replicas.size()) {
        if (error) {
            *error = "pg_id out of range: " + std::to_string(pg_id);
        }
        return false;
    }
    *out = view->pg_to_replicas[pg_id];
    if (out->members.empty()) {
        if (error) {
            *error = "replica set is empty for pg_id=" + std::to_string(pg_id);
        }
        return false;
    }
    return true;
}

uint64_t PGManager::CurrentEpoch() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return current_view_ ? current_view_->epoch : 0;
}

bool PGManager::ReplaceView(const PlacementView& view, std::string* error) {
    if (view.pg_count == 0) {
        if (error) {
            *error = "invalid placement view: pg_count=0";
        }
        return false;
    }
    if (view.pg_to_replicas.size() != view.pg_count) {
        if (error) {
            *error = "invalid placement view: pg_to_replicas size mismatch";
        }
        return false;
    }
    if (view.epoch == 0) {
        if (error) {
            *error = "invalid placement view: epoch=0";
        }
        return false;
    }

    auto next = std::make_shared<PlacementView>(view);
    {
        std::unique_lock<std::shared_mutex> lock(mu_);
        history_[next->epoch] = next;
        current_view_ = next;
        while (history_.size() > options_.history_limit) {
            auto victim = history_.end();
            for (auto it = history_.begin(); it != history_.end(); ++it) {
                if (current_view_ && it->first == current_view_->epoch) {
                    continue;
                }
                if (victim == history_.end() || it->first < victim->first) {
                    victim = it;
                }
            }
            if (victim == history_.end()) {
                break;
            }
            history_.erase(victim);
        }
    }
    return true;
}

bool PGManager::RebuildFromNodes(const std::vector<NodeInfo>& nodes, uint64_t new_epoch, std::string* error) {
    if (new_epoch == 0) {
        if (error) {
            *error = "new_epoch must be > 0";
        }
        return false;
    }

    std::vector<CandidateNode> candidates = BuildCandidates(nodes);
    PlacementView view;
    if (!BuildPlacementView(candidates, options_.pg_count, options_.replica, new_epoch, &view, error)) {
        return false;
    }
    return ReplaceView(view, error);
}

uint32_t PGManager::PgCount() const {
    return options_.pg_count;
}

uint32_t PGManager::ReplicaCount() const {
    return options_.replica;
}

uint64_t PGManager::Hash64(std::string_view data) {
    constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    uint64_t hash = kFnvOffset;
    for (unsigned char ch : data) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= kFnvPrime;
    }
    return MixU64(hash);
}

uint64_t PGManager::Hash64Pair(uint32_t pg_id, const std::string& node_id, uint64_t epoch) {
    std::string key;
    key.reserve(node_id.size() + 32);
    key.append(std::to_string(pg_id));
    key.push_back('/');
    key.append(node_id);
    key.push_back('/');
    key.append(std::to_string(epoch));
    return Hash64(key);
}

bool PGManager::IsNodeAllocatable(const NodeInfo& node) {
    if (!node.allocatable || !node.is_primary) {
        return false;
    }
    if (node.type == NodeType::kOptical) {
        return false;
    }
    for (const auto& disk : node.disks) {
        if (disk.is_healthy) {
            return true;
        }
    }
    return false;
}

std::string PGManager::BuildVirtualNodeId(const std::string& base_node_id, uint64_t virtual_index) {
    return base_node_id + "-v" + std::to_string(virtual_index);
}

std::vector<PGManager::CandidateNode> PGManager::BuildCandidates(const std::vector<NodeInfo>& nodes) {
    std::vector<CandidateNode> candidates;
    for (const auto& node : nodes) {
        if (!IsNodeAllocatable(node)) {
            continue;
        }

        std::vector<std::string> healthy_disks;
        healthy_disks.reserve(node.disks.size());
        for (const auto& disk : node.disks) {
            if (disk.is_healthy && !disk.disk_id.empty()) {
                healthy_disks.push_back(disk.disk_id);
            }
        }
        if (healthy_disks.empty()) {
            continue;
        }

        uint32_t logical_nodes = IsVirtualizedNodeType(node.type) ? std::max<uint32_t>(1, node.virtual_node_count) : 1;
        for (uint32_t idx = 0; idx < logical_nodes; ++idx) {
            CandidateNode candidate;
            candidate.node_id = IsVirtualizedNodeType(node.type) ? BuildVirtualNodeId(node.node_id, idx) : node.node_id;
            candidate.node_address = node.address;
            candidate.group_id = node.group_id.empty() ? node.node_id : node.group_id;
            candidate.disk_id = healthy_disks[idx % healthy_disks.size()];
            candidate.epoch = node.epoch;
            candidate.secondary_node_id = node.secondary_node_id;
            candidate.secondary_address = node.secondary_address;
            candidate.sync_ready = node.sync_ready;
            candidate.node_type = node.type;
            candidates.push_back(std::move(candidate));
        }
    }
    return candidates;
}

bool PGManager::BuildPlacementView(const std::vector<CandidateNode>& candidates,
                                   uint32_t pg_count,
                                   uint32_t replica,
                                   uint64_t epoch,
                                   PlacementView* out,
                                   std::string* error) {
    if (!out) {
        if (error) {
            *error = "placement view output is null";
        }
        return false;
    }
    if (pg_count == 0 || replica == 0) {
        if (error) {
            *error = "invalid pg_count or replica";
        }
        return false;
    }
    if (candidates.empty()) {
        if (error) {
            *error = "no allocatable candidates available for placement view";
        }
        return false;
    }

    out->epoch = epoch;
    out->pg_count = pg_count;
    out->replica = replica;
    out->pg_to_replicas.clear();
    out->pg_to_replicas.resize(pg_count);

    for (uint32_t pg = 0; pg < pg_count; ++pg) {
        std::vector<std::pair<uint64_t, size_t>> scores;
        scores.reserve(candidates.size());
        for (size_t i = 0; i < candidates.size(); ++i) {
            scores.emplace_back(Hash64Pair(pg, candidates[i].node_id, epoch), i);
        }
        std::sort(scores.begin(),
                  scores.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.first > rhs.first;
                  });

        PgReplicaSet set;
        set.pg_id = pg;
        set.view_epoch = epoch;

        std::unordered_set<std::string> seen_groups;
        for (const auto& scored : scores) {
            const CandidateNode& node = candidates[scored.second];
            if (!seen_groups.insert(node.group_id).second) {
                continue;
            }
            PgReplicaMember member;
            member.node_id = node.node_id;
            member.node_address = node.node_address;
            member.disk_id = node.disk_id;
            member.group_id = node.group_id;
            member.epoch = node.epoch == 0 ? epoch : node.epoch;
            member.primary_node_id = node.node_id;
            member.primary_address = node.node_address;
            member.secondary_node_id = node.secondary_node_id;
            member.secondary_address = node.secondary_address;
            member.sync_ready = node.sync_ready;
            member.node_type = node.node_type;
            set.members.push_back(std::move(member));

            if (set.members.size() >= replica) {
                break;
            }
        }

        if (set.members.size() < replica) {
            if (error) {
                *error = "insufficient unique groups for pg_id=" + std::to_string(pg) +
                         ", required=" + std::to_string(replica) +
                         ", actual=" + std::to_string(set.members.size());
            }
            return false;
        }
        out->pg_to_replicas[pg] = std::move(set);
    }

    return true;
}

bool PGManager::GetViewLocked(uint64_t epoch, std::shared_ptr<PlacementView>* out) const {
    if (!out) {
        return false;
    }
    if (epoch == 0) {
        if (!current_view_) {
            return false;
        }
        *out = current_view_;
        return true;
    }
    auto it = history_.find(epoch);
    if (it == history_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

} // namespace zb::mds
