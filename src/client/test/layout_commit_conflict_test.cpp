#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <iostream>
#include <string>

#include "mds.pb.h"

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(path, "/e2e_layout_conflict_test", "Test file path");
DEFINE_uint64(new_size, 16, "Committed file size");
DEFINE_int32(timeout_ms, 3000, "RPC timeout(ms)");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

using zb::rpc::MdsService_Stub;

uint64_t NowMilliseconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

bool RpcOk(const brpc::Controller& cntl, const char* name) {
    if (cntl.Failed()) {
        std::cerr << name << " rpc failed: " << cntl.ErrorText() << std::endl;
        return false;
    }
    return true;
}

bool EnsureFile(MdsService_Stub* stub, const std::string& path) {
    zb::rpc::CreateRequest req;
    req.set_path(path);
    req.set_mode(0644);
    req.set_uid(0);
    req.set_gid(0);
    req.set_replica(1);
    req.set_chunk_size(4 * 1024 * 1024);

    zb::rpc::CreateReply resp;
    brpc::Controller cntl;
    stub->Create(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "Create")) {
        return false;
    }
    if (resp.status().code() == zb::rpc::MDS_OK || resp.status().code() == zb::rpc::MDS_ALREADY_EXISTS) {
        return true;
    }
    std::cerr << "Create failed code=" << resp.status().code()
              << " msg=" << resp.status().message() << std::endl;
    return false;
}

bool LookupInode(MdsService_Stub* stub, const std::string& path, uint64_t* inode_id) {
    if (!inode_id) {
        return false;
    }
    zb::rpc::LookupRequest req;
    req.set_path(path);
    zb::rpc::LookupReply resp;
    brpc::Controller cntl;
    stub->Lookup(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "Lookup")) {
        return false;
    }
    if (resp.status().code() != zb::rpc::MDS_OK) {
        std::cerr << "Lookup failed code=" << resp.status().code()
                  << " msg=" << resp.status().message() << std::endl;
        return false;
    }
    *inode_id = resp.attr().inode_id();
    return *inode_id != 0;
}

bool GetLayoutRoot(MdsService_Stub* stub, uint64_t inode_id, zb::rpc::LayoutRoot* out) {
    if (!out || inode_id == 0) {
        return false;
    }
    zb::rpc::GetLayoutRootRequest req;
    req.set_inode_id(inode_id);
    zb::rpc::GetLayoutRootReply resp;
    brpc::Controller cntl;
    stub->GetLayoutRoot(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "GetLayoutRoot")) {
        return false;
    }
    if (resp.status().code() != zb::rpc::MDS_OK) {
        std::cerr << "GetLayoutRoot failed code=" << resp.status().code()
                  << " msg=" << resp.status().message() << std::endl;
        return false;
    }
    *out = resp.root();
    return true;
}

bool CommitRoot(MdsService_Stub* stub,
                uint64_t inode_id,
                uint64_t expected_version,
                uint64_t new_size,
                uint64_t epoch,
                const std::string& layout_root_id,
                zb::rpc::MdsStatus* out_status) {
    zb::rpc::CommitLayoutRootRequest req;
    req.set_inode_id(inode_id);
    req.set_expected_layout_version(expected_version);
    req.set_update_inode_size(true);
    req.set_new_size(new_size);

    zb::rpc::LayoutRoot* root = req.mutable_root();
    root->set_inode_id(inode_id);
    root->set_layout_root_id(layout_root_id);
    root->set_layout_version(expected_version + 1);
    root->set_file_size(new_size);
    root->set_epoch(epoch);
    root->set_update_ts(NowMilliseconds());

    zb::rpc::CommitLayoutRootReply resp;
    brpc::Controller cntl;
    stub->CommitLayoutRoot(&cntl, &req, &resp, nullptr);
    if (!RpcOk(cntl, "CommitLayoutRoot")) {
        if (out_status) {
            out_status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            out_status->set_message(cntl.ErrorText());
        }
        return false;
    }
    if (out_status) {
        *out_status = resp.status();
    }
    return resp.status().code() == zb::rpc::MDS_OK;
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

    if (!EnsureFile(&stub, FLAGS_path)) {
        return 2;
    }

    uint64_t inode_id = 0;
    if (!LookupInode(&stub, FLAGS_path, &inode_id)) {
        return 3;
    }

    zb::rpc::LayoutRoot base_root;
    if (!GetLayoutRoot(&stub, inode_id, &base_root)) {
        return 4;
    }
    const uint64_t expected_version = base_root.layout_version();
    const uint64_t epoch = base_root.epoch();
    std::string root_id = base_root.layout_root_id();
    if (root_id.empty()) {
        root_id = "lr-" + std::to_string(inode_id) + "-" + std::to_string(NowMilliseconds());
    }

    zb::rpc::MdsStatus commit_status;
    if (!CommitRoot(&stub,
                    inode_id,
                    expected_version,
                    FLAGS_new_size,
                    epoch,
                    root_id,
                    &commit_status)) {
        std::cerr << "first commit failed code=" << commit_status.code()
                  << " msg=" << commit_status.message() << std::endl;
        return 5;
    }

    zb::rpc::MdsStatus stale_status;
    (void)CommitRoot(&stub,
                     inode_id,
                     expected_version,
                     FLAGS_new_size + 1,
                     epoch,
                     root_id,
                     &stale_status);
    if (stale_status.code() != zb::rpc::MDS_STALE_EPOCH) {
        std::cerr << "expect stale-epoch, got code=" << stale_status.code()
                  << " msg=" << stale_status.message() << std::endl;
        return 6;
    }

    std::cout << "PASS inode=" << inode_id
              << " expected_version=" << expected_version
              << " stale_code=" << stale_status.code()
              << std::endl;
    return 0;
}
