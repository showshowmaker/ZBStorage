#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "../config/MdsConfig.h"

namespace zb::mds {

struct PgReplicaMember {
    std::string node_id;
    std::string node_address;
    std::string disk_id;
    std::string group_id;
    uint64_t epoch{1};
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
    bool sync_ready{false};
    NodeType node_type{NodeType::kReal};
};

struct PgReplicaSet {
    uint32_t pg_id{0};
    uint64_t view_epoch{0};
    std::vector<PgReplicaMember> members;
};

struct PlacementView {
    uint64_t epoch{0};
    uint32_t pg_count{0};
    uint32_t replica{0};
    std::vector<PgReplicaSet> pg_to_replicas;
};

class PGManager {
public:
    struct Options {
        uint32_t pg_count{1024};
        uint32_t replica{2};
        size_t history_limit{2};
    };

    PGManager();
    explicit PGManager(Options options);

    uint32_t ObjectToPg(std::string_view object_id, uint64_t epoch_hint = 0) const;
    bool ResolvePg(uint32_t pg_id, uint64_t epoch, PgReplicaSet* out, std::string* error) const;
    uint64_t CurrentEpoch() const;

    bool ReplaceView(const PlacementView& view, std::string* error);
    bool RebuildFromNodes(const std::vector<NodeInfo>& nodes, uint64_t new_epoch, std::string* error);

    uint32_t PgCount() const;
    uint32_t ReplicaCount() const;

private:
    struct CandidateNode {
        std::string node_id;
        std::string node_address;
        std::string disk_id;
        std::string group_id;
        uint64_t epoch{1};
        std::string secondary_node_id;
        std::string secondary_address;
        bool sync_ready{false};
        NodeType node_type{NodeType::kReal};
    };

    static uint64_t Hash64(std::string_view data);
    static uint64_t Hash64Pair(uint32_t pg_id, const std::string& node_id, uint64_t epoch);
    static bool IsNodeAllocatable(const NodeInfo& node);
    static std::string BuildVirtualNodeId(const std::string& base_node_id, uint64_t virtual_index);
    static std::vector<CandidateNode> BuildCandidates(const std::vector<NodeInfo>& nodes);
    static bool BuildPlacementView(const std::vector<CandidateNode>& candidates,
                                   uint32_t pg_count,
                                   uint32_t replica,
                                   uint64_t epoch,
                                   PlacementView* out,
                                   std::string* error);
    bool GetViewLocked(uint64_t epoch, std::shared_ptr<PlacementView>* out) const;

    Options options_;

    mutable std::shared_mutex mu_;
    std::shared_ptr<PlacementView> current_view_;
    std::unordered_map<uint64_t, std::shared_ptr<PlacementView>> history_;
};

} // namespace zb::mds
