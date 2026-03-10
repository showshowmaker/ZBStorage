#include <gflags/gflags.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "../../mds/allocator/PGManager.h"

DEFINE_uint32(pg_count, 128, "PG count");
DEFINE_uint32(replica, 2, "Replica count");
DEFINE_uint32(sample_objects, 1000, "How many synthetic objects to sample");
DEFINE_uint32(expect_moved_min, 1, "Minimum moved objects expected after migration");
DEFINE_uint32(expect_stable_min, 1, "Minimum stable objects expected after migration");

namespace {

zb::mds::NodeInfo MakeNode(const std::string& node_id, const std::string& addr, const std::string& group_id) {
    zb::mds::NodeInfo n;
    n.node_id = node_id;
    n.address = addr;
    n.group_id = group_id;
    n.type = zb::mds::NodeType::kReal;
    n.allocatable = true;
    n.is_primary = true;
    n.sync_ready = true;
    n.epoch = 1;
    zb::mds::DiskInfo d;
    d.disk_id = "disk0";
    d.is_healthy = true;
    n.disks.push_back(d);
    return n;
}

std::string Signature(const zb::mds::PgReplicaSet& set) {
    std::string s;
    for (const auto& m : set.members) {
        s += m.group_id;
        s.push_back('@');
        s += m.node_id;
        s.push_back('|');
    }
    return s;
}

bool ResolveSignature(const zb::mds::PGManager& manager, const std::string& object_id, uint64_t epoch, std::string* sig) {
    if (!sig) {
        return false;
    }
    const uint32_t pg = manager.ObjectToPg(object_id, epoch);
    zb::mds::PgReplicaSet set;
    std::string err;
    if (!manager.ResolvePg(pg, epoch, &set, &err)) {
        std::cerr << "ResolvePg failed object=" << object_id << " pg=" << pg << " epoch=" << epoch
                  << " err=" << err << std::endl;
        return false;
    }
    *sig = Signature(set);
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    const uint32_t sample_n = std::max<uint32_t>(1, FLAGS_sample_objects);

    zb::mds::PGManager::Options options;
    options.pg_count = std::max<uint32_t>(1, FLAGS_pg_count);
    options.replica = std::max<uint32_t>(1, FLAGS_replica);
    options.history_limit = 4;

    zb::mds::PGManager manager(options);

    std::vector<zb::mds::NodeInfo> epoch1_nodes;
    epoch1_nodes.push_back(MakeNode("node-a", "127.0.0.1:10001", "g-a"));
    epoch1_nodes.push_back(MakeNode("node-b", "127.0.0.1:10002", "g-b"));
    epoch1_nodes.push_back(MakeNode("node-c", "127.0.0.1:10003", "g-c"));

    std::vector<zb::mds::NodeInfo> epoch2_nodes = epoch1_nodes;
    epoch2_nodes.pop_back();
    epoch2_nodes.push_back(MakeNode("node-d", "127.0.0.1:10004", "g-d"));

    std::string err;
    if (!manager.RebuildFromNodes(epoch1_nodes, 1, &err)) {
        std::cerr << "Rebuild epoch1 failed: " << err << std::endl;
        return 1;
    }
    if (!manager.RebuildFromNodes(epoch2_nodes, 2, &err)) {
        std::cerr << "Rebuild epoch2 failed: " << err << std::endl;
        return 2;
    }

    uint32_t moved = 0;
    uint32_t stable = 0;
    uint32_t deterministic_ok = 0;

    for (uint32_t i = 0; i < sample_n; ++i) {
        const std::string object_id = "obj-" + std::to_string(i);

        std::string sig1_a;
        std::string sig1_b;
        std::string sig2;
        if (!ResolveSignature(manager, object_id, 1, &sig1_a) ||
            !ResolveSignature(manager, object_id, 1, &sig1_b) ||
            !ResolveSignature(manager, object_id, 2, &sig2)) {
            return 3;
        }
        if (sig1_a == sig1_b) {
            ++deterministic_ok;
        }
        if (sig1_a == sig2) {
            ++stable;
        } else {
            ++moved;
        }
    }

    if (deterministic_ok != sample_n) {
        std::cerr << "Determinism check failed: expected=" << sample_n
                  << " actual=" << deterministic_ok << std::endl;
        return 4;
    }
    if (moved < FLAGS_expect_moved_min) {
        std::cerr << "Migration movement too low: moved=" << moved
                  << " expect_min=" << FLAGS_expect_moved_min << std::endl;
        return 5;
    }
    if (stable < FLAGS_expect_stable_min) {
        std::cerr << "Migration stability too low: stable=" << stable
                  << " expect_min=" << FLAGS_expect_stable_min << std::endl;
        return 6;
    }

    std::cout << "PASS sample=" << sample_n
              << " moved=" << moved
              << " stable=" << stable
              << " deterministic=" << deterministic_ok
              << std::endl;
    return 0;
}
