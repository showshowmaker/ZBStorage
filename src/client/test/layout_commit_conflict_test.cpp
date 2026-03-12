#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "mds.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(path, "/e2e_file_meta_conflict_test", "Test file path");
DEFINE_uint64(new_size, 16, "Committed file size");
DEFINE_uint64(object_unit_size, 4 * 1024 * 1024, "Object unit size for file meta");
DEFINE_int32(timeout_ms, 3000, "RPC timeout(ms)");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

using zb::rpc::MdsService_Stub;

uint64_t NowMilliseconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

uint64_t NowSeconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
}

bool RpcOk(const brpc::Controller& cntl, const char* name) {
    if (cntl.Failed()) {
        std::cerr << name << " rpc failed: " << cntl.ErrorText() << std::endl;
        return false;
    }
    return true;
}

bool EnsureFile(MdsService_Stub* stub, const std::string& path, uint64_t* inode_id) {
    if (!stub) {
        return false;
    }
    zb::rpc::CreateRequest req;
    req.set_path(path);
    req.set_mode(0644);
    req.set_uid(0);
    req.set_gid(0);
    req.set_replica(1);
    req.set_object_unit_size(FLAGS_object_unit_size);

    zb::rpc::CreateReply resp;
    brpc::Controller cntl;
    stub->Create(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "Create")) {
        return false;
    }
    if (resp.status().code() == zb::rpc::MDS_OK) {
        if (inode_id) {
            *inode_id = resp.attr().inode_id();
        }
        return true;
    }
    if (resp.status().code() != zb::rpc::MDS_ALREADY_EXISTS) {
        std::cerr << "Create failed code=" << resp.status().code()
                  << " msg=" << resp.status().message() << std::endl;
        return false;
    }

    zb::rpc::LookupRequest lookup_req;
    lookup_req.set_path(path);
    zb::rpc::LookupReply lookup_resp;
    brpc::Controller lookup_cntl;
    stub->Lookup(&lookup_cntl, &lookup_req, &lookup_resp, nullptr);
    if (!RpcOk(lookup_cntl, "Lookup")) {
        return false;
    }
    if (lookup_resp.status().code() != zb::rpc::MDS_OK) {
        std::cerr << "Lookup failed code=" << lookup_resp.status().code()
                  << " msg=" << lookup_resp.status().message() << std::endl;
        return false;
    }
    if (inode_id) {
        *inode_id = lookup_resp.attr().inode_id();
    }
    return true;
}

bool GetFileAnchor(MdsService_Stub* stub, uint64_t inode_id, zb::rpc::ReplicaLocation* anchor) {
    if (!stub || !anchor || inode_id == 0) {
        return false;
    }
    zb::rpc::GetFileAnchorRequest req;
    req.set_inode_id(inode_id);
    zb::rpc::GetFileAnchorReply resp;
    brpc::Controller cntl;
    stub->GetFileAnchor(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "GetFileAnchor")) {
        return false;
    }
    if (resp.status().code() != zb::rpc::MDS_OK) {
        std::cerr << "GetFileAnchor failed code=" << resp.status().code()
                  << " msg=" << resp.status().message() << std::endl;
        return false;
    }
    const auto& set = resp.anchor();
    if (!set.disk_anchor().node_id().empty() && !set.disk_anchor().disk_id().empty()) {
        anchor->set_node_id(set.disk_anchor().node_id());
        anchor->set_disk_id(set.disk_anchor().disk_id());
        anchor->set_object_id(set.disk_anchor().object_id());
        anchor->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        anchor->set_replica_state(set.disk_anchor().replica_state());
    } else if (!set.optical_anchor().node_id().empty() && !set.optical_anchor().disk_id().empty()) {
        anchor->set_node_id(set.optical_anchor().node_id());
        anchor->set_disk_id(set.optical_anchor().disk_id());
        anchor->set_object_id(set.optical_anchor().object_id());
        anchor->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        anchor->set_replica_state(set.optical_anchor().replica_state());
    } else {
        return false;
    }
    return !anchor->node_id().empty();
}

bool ResolveNodeAddress(const std::string& scheduler_endpoint,
                        const std::string& node_id,
                        std::string* address) {
    if (node_id.empty() || !address) {
        return false;
    }
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(scheduler_endpoint.c_str(), &options) != 0) {
        std::cerr << "Init scheduler channel failed: " << scheduler_endpoint << std::endl;
        return false;
    }
    zb::rpc::SchedulerService_Stub stub(&channel);
    zb::rpc::GetClusterViewRequest req;
    req.set_min_generation(0);
    zb::rpc::GetClusterViewReply resp;
    brpc::Controller cntl;
    stub.GetClusterView(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "GetClusterView")) {
        return false;
    }
    if (resp.status().code() != zb::rpc::SCHED_OK) {
        std::cerr << "GetClusterView failed code=" << resp.status().code()
                  << " msg=" << resp.status().message() << std::endl;
        return false;
    }
    for (const auto& node : resp.nodes()) {
        if (node.node_id() == node_id && !node.address().empty()) {
            *address = node.address();
            return true;
        }
    }
    const size_t pos = node_id.rfind("-v");
    if (pos != std::string::npos && pos + 2 < node_id.size()) {
        const std::string base_id = node_id.substr(0, pos);
        for (const auto& node : resp.nodes()) {
            if (node.node_id() == base_id && !node.address().empty()) {
                *address = node.address();
                return true;
            }
        }
    }
    std::cerr << "node not found in scheduler view: " << node_id << std::endl;
    return false;
}

bool CommitFileWrite(const std::string& node_address,
                     uint64_t inode_id,
                     const std::string& txid,
                     uint64_t file_size,
                     uint64_t object_unit_size,
                     uint64_t expected_version,
                     bool allow_create,
                     zb::rpc::CommitFileWriteReply* out_reply) {
    if (node_address.empty() || inode_id == 0 || object_unit_size == 0 || txid.empty() || !out_reply) {
        return false;
    }
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(node_address.c_str(), &options) != 0) {
        std::cerr << "Init data node channel failed: " << node_address << std::endl;
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(&channel);
    zb::rpc::CommitFileWriteRequest req;
    req.set_inode_id(inode_id);
    req.set_txid(txid);
    req.set_file_size(file_size);
    req.set_object_unit_size(object_unit_size);
    req.set_expected_version(expected_version);
    req.set_allow_create(allow_create);
    req.set_mtime_sec(NowSeconds());

    brpc::Controller cntl;
    cntl.set_timeout_ms(FLAGS_timeout_ms);
    stub.CommitFileWrite(&cntl, &req, out_reply, nullptr);
    if (!RpcOk(cntl, "CommitFileWrite")) {
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_mds.c_str(), &options) != 0) {
        std::cerr << "Init channel failed: " << FLAGS_mds << std::endl;
        return 1;
    }
    MdsService_Stub stub(&channel);

    uint64_t inode_id = 0;
    if (!EnsureFile(&stub, FLAGS_path, &inode_id) || inode_id == 0) {
        return 2;
    }

    zb::rpc::ReplicaLocation anchor;
    if (!GetFileAnchor(&stub, inode_id, &anchor)) {
        return 3;
    }
    std::string anchor_address;
    if (!ResolveNodeAddress(FLAGS_scheduler, anchor.node_id(), &anchor_address)) {
        return 10;
    }

    zb::rpc::CommitFileWriteReply first;
    if (!CommitFileWrite(anchor_address,
                         inode_id,
                         "tx-first-" + std::to_string(NowMilliseconds()),
                         FLAGS_new_size,
                         FLAGS_object_unit_size,
                         0,
                         true,
                         &first)) {
        return 4;
    }
    if (first.status().code() != zb::rpc::STATUS_OK) {
        std::cerr << "first CommitFileWrite failed code=" << first.status().code()
                  << " msg=" << first.status().message() << std::endl;
        return 5;
    }
    const uint64_t stale_version = first.meta().version();

    zb::rpc::CommitFileWriteReply second;
    if (!CommitFileWrite(anchor_address,
                         inode_id,
                         "tx-second-" + std::to_string(NowMilliseconds()),
                         FLAGS_new_size + 1,
                         FLAGS_object_unit_size,
                         stale_version,
                         true,
                         &second)) {
        return 6;
    }
    if (second.status().code() != zb::rpc::STATUS_OK) {
        std::cerr << "second CommitFileWrite failed code=" << second.status().code()
                  << " msg=" << second.status().message() << std::endl;
        return 7;
    }

    zb::rpc::CommitFileWriteReply stale;
    if (!CommitFileWrite(anchor_address,
                         inode_id,
                         "tx-stale-" + std::to_string(NowMilliseconds()),
                         FLAGS_new_size + 2,
                         FLAGS_object_unit_size,
                         stale_version,
                         true,
                         &stale)) {
        return 8;
    }
    if (stale.status().code() != zb::rpc::STATUS_IO_ERROR ||
        stale.status().message() != "VERSION_MISMATCH") {
        std::cerr << "expect VERSION_MISMATCH, got code=" << stale.status().code()
                  << " msg=" << stale.status().message() << std::endl;
        return 9;
    }

    std::cout << "PASS inode=" << inode_id
              << " stale_version=" << stale_version
              << " conflict_status=" << stale.status().message() << std::endl;
    return 0;
}
