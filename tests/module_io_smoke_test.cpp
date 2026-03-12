#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <string>
#include <vector>

#include "mds.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(real, "127.0.0.1:19080", "Real node endpoint");
DEFINE_string(virtual_node, "127.0.0.1:29080", "Virtual node endpoint");
DEFINE_string(optical, "127.0.0.1:39080", "Optical node endpoint");
DEFINE_string(real_disk, "disk0", "Real node disk id");
DEFINE_string(virtual_disk, "disk0", "Virtual node disk id");
DEFINE_string(optical_disc, "odisk0", "Optical node disc id");
DEFINE_bool(expect_optical, true, "Whether optical node test is required");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

struct CaseSummary {
    int total{0};
    int pass{0};
    int fail{0};
    std::vector<std::string> failed_cases;

    void Add(const std::string& name, bool ok, const std::string& detail) {
        ++total;
        if (ok) {
            ++pass;
            std::cout << "[PASS] " << name;
            if (!detail.empty()) {
                std::cout << " | " << detail;
            }
            std::cout << std::endl;
            return;
        }
        ++fail;
        failed_cases.push_back(name);
        std::cerr << "[FAIL] " << name;
        if (!detail.empty()) {
            std::cerr << " | " << detail;
        }
        std::cerr << std::endl;
    }
};

std::string UniqueSuffix() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    return std::to_string(ms);
}

bool InitChannel(const std::string& endpoint, brpc::Channel* channel, std::string* error) {
    if (!channel) {
        if (error) {
            *error = "channel is null";
        }
        return false;
    }
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;
    if (channel->Init(endpoint.c_str(), &options) != 0) {
        if (error) {
            *error = "failed to init channel: " + endpoint;
        }
        return false;
    }
    return true;
}

bool TestScheduler(const std::string& endpoint, std::string* detail) {
    brpc::Channel channel;
    std::string error;
    if (!InitChannel(endpoint, &channel, &error)) {
        if (detail) {
            *detail = error;
        }
        return false;
    }

    zb::rpc::SchedulerService_Stub stub(&channel);
    zb::rpc::GetClusterViewRequest req;
    req.set_min_generation(0);
    zb::rpc::GetClusterViewReply resp;
    brpc::Controller cntl;
    stub.GetClusterView(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (detail) {
            *detail = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::SCHED_OK) {
        if (detail) {
            *detail = "scheduler status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
        }
        return false;
    }
    if (detail) {
        *detail = "generation=" + std::to_string(resp.generation()) +
                  " nodes=" + std::to_string(resp.nodes_size());
    }
    return true;
}

bool TestMdsNamespace(const std::string& endpoint, const std::string& uniq, std::string* detail) {
    brpc::Channel channel;
    std::string error;
    if (!InitChannel(endpoint, &channel, &error)) {
        if (detail) {
            *detail = error;
        }
        return false;
    }

    zb::rpc::MdsService_Stub stub(&channel);
    const std::string dir = "/smoke_dir_" + uniq;
    const std::string file = dir + "/smoke_file.txt";
    const std::string renamed = dir + "/smoke_file_renamed.txt";

    {
        zb::rpc::MkdirRequest req;
        req.set_path(dir);
        req.set_mode(0755);
        req.set_uid(0);
        req.set_gid(0);
        zb::rpc::MkdirReply resp;
        brpc::Controller cntl;
        stub.Mkdir(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Mkdir rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "Mkdir status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    uint64_t inode_id = 0;
    {
        zb::rpc::CreateRequest req;
        req.set_path(file);
        req.set_mode(0644);
        req.set_uid(0);
        req.set_gid(0);
        req.set_replica(1);
        req.set_object_unit_size(4 * 1024 * 1024ULL);
        zb::rpc::CreateReply resp;
        brpc::Controller cntl;
        stub.Create(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Create rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "Create status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
        inode_id = resp.attr().inode_id();
    }

    {
        zb::rpc::LookupRequest req;
        req.set_path(file);
        zb::rpc::LookupReply resp;
        brpc::Controller cntl;
        stub.Lookup(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Lookup rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK || resp.attr().inode_id() != inode_id) {
            if (detail) {
                *detail = "Lookup mismatch";
            }
            return false;
        }
    }

    {
        zb::rpc::GetFileAnchorRequest req;
        req.set_inode_id(inode_id);
        zb::rpc::GetFileAnchorReply resp;
        brpc::Controller cntl;
        stub.GetFileAnchor(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "GetFileAnchor rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "GetFileAnchor status=" + std::to_string(resp.status().code()) +
                          " msg=" + resp.status().message();
            }
            return false;
        }
        const bool has_disk_anchor =
            !resp.anchor().disk_anchor().node_id().empty() &&
            !resp.anchor().disk_anchor().disk_id().empty();
        const bool has_optical_anchor =
            !resp.anchor().optical_anchor().node_id().empty() &&
            !resp.anchor().optical_anchor().disk_id().empty();
        if (!has_disk_anchor && !has_optical_anchor) {
            if (detail) {
                *detail = "GetFileAnchor returns empty anchor set";
            }
            return false;
        }
    }

    {
        zb::rpc::RenameRequest req;
        req.set_old_path(file);
        req.set_new_path(renamed);
        zb::rpc::RenameReply resp;
        brpc::Controller cntl;
        stub.Rename(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Rename rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "Rename status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    {
        zb::rpc::UnlinkRequest req;
        req.set_path(renamed);
        zb::rpc::UnlinkReply resp;
        brpc::Controller cntl;
        stub.Unlink(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Unlink rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "Unlink status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    {
        zb::rpc::RmdirRequest req;
        req.set_path(dir);
        zb::rpc::RmdirReply resp;
        brpc::Controller cntl;
        stub.Rmdir(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "Rmdir rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::MDS_OK) {
            if (detail) {
                *detail = "Rmdir status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    if (detail) {
        *detail = "namespace create/lookup/rename/unlink/rmdir OK";
    }
    return true;
}

bool TestObjectRw(const std::string& endpoint,
                  const std::string& disk_id,
                  const std::string& object_id,
                  const std::string& payload,
                  std::string* detail) {
    brpc::Channel channel;
    std::string error;
    if (!InitChannel(endpoint, &channel, &error)) {
        if (detail) {
            *detail = error;
        }
        return false;
    }
    zb::rpc::RealNodeService_Stub stub(&channel);

    {
        zb::rpc::WriteObjectRequest req;
        req.set_disk_id(disk_id);
        req.set_object_id(object_id);
        req.set_offset(0);
        req.set_data(payload);
        zb::rpc::WriteObjectReply resp;
        brpc::Controller cntl;
        stub.WriteObject(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "WriteObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "WriteObject status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    {
        zb::rpc::ReadObjectRequest req;
        req.set_disk_id(disk_id);
        req.set_object_id(object_id);
        req.set_offset(0);
        req.set_size(static_cast<uint64_t>(payload.size()));
        zb::rpc::ReadObjectReply resp;
        brpc::Controller cntl;
        stub.ReadObject(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "ReadObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "ReadObject status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
        if (resp.data() != payload) {
            if (detail) {
                *detail = "ReadObject payload mismatch";
            }
            return false;
        }
    }

    {
        zb::rpc::DeleteObjectRequest req;
        req.set_disk_id(disk_id);
        req.set_object_id(object_id);
        zb::rpc::DeleteObjectReply resp;
        brpc::Controller cntl;
        stub.DeleteObject(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "DeleteObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "DeleteObject status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
    }

    if (detail) {
        *detail = "disk=" + disk_id + " object rw OK";
    }
    return true;
}

bool TestOptical(const std::string& endpoint,
                 const std::string& disc_id,
                 const std::string& uniq,
                 std::string* detail) {
    brpc::Channel channel;
    std::string error;
    if (!InitChannel(endpoint, &channel, &error)) {
        if (detail) {
            *detail = error;
        }
        return false;
    }
    zb::rpc::RealNodeService_Stub stub(&channel);

    const std::string object_id = "optical_object_" + uniq;
    const std::string payload = "optical_payload_" + uniq;
    const uint64_t inode_id = static_cast<uint64_t>(std::stoull(uniq));
    const std::string file_id = "inode-" + std::to_string(inode_id);

    {
        zb::rpc::WriteObjectRequest req;
        req.set_disk_id(disc_id);
        req.set_object_id(object_id);
        req.set_offset(0);
        req.set_data(payload);
        zb::rpc::WriteObjectReply resp;
        brpc::Controller cntl;
        stub.WriteObject(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "optical plain WriteObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_INVALID_ARGUMENT) {
            if (detail) {
                *detail = "optical plain write should be rejected";
            }
            return false;
        }
    }

    zb::rpc::WriteObjectReply write_resp;
    {
        zb::rpc::WriteObjectRequest req;
        req.set_disk_id(disc_id);
        req.set_object_id(object_id);
        req.set_offset(0);
        req.set_data(payload);
        req.set_archive_op_id("archive_op_" + uniq);
        req.set_inode_id(inode_id);
        req.set_file_id(file_id);
        req.set_file_path("/archive/" + file_id);
        req.set_file_size(static_cast<uint64_t>(payload.size()));
        req.set_file_offset(0);
        req.set_file_mode(0644);
        req.set_file_uid(0);
        req.set_file_gid(0);
        req.set_file_mtime(static_cast<uint64_t>(std::time(nullptr)));
        req.set_file_object_index(0);
        brpc::Controller cntl;
        stub.WriteObject(&cntl, &req, &write_resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "optical archive WriteObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (write_resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "optical archive write status=" + std::to_string(write_resp.status().code()) +
                          " msg=" + write_resp.status().message();
            }
            return false;
        }
    }

    {
        zb::rpc::ReadObjectRequest req;
        req.set_disk_id(disc_id);
        req.set_object_id(object_id);
        req.set_offset(0);
        req.set_size(static_cast<uint64_t>(payload.size()));
        req.set_image_id(write_resp.image_id());
        req.set_image_offset(write_resp.image_offset());
        req.set_image_length(write_resp.image_length());
        zb::rpc::ReadObjectReply resp;
        brpc::Controller cntl;
        stub.ReadObject(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "optical ReadObject rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "optical ReadObject status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
        if (resp.data() != payload) {
            if (detail) {
                *detail = "optical ReadObject payload mismatch";
            }
            return false;
        }
    }

    {
        zb::rpc::ReadArchivedFileRequest req;
        req.set_disc_id(disc_id);
        req.set_inode_id(inode_id);
        req.set_file_id(file_id);
        req.set_offset(0);
        req.set_size(static_cast<uint64_t>(payload.size()));
        zb::rpc::ReadArchivedFileReply resp;
        brpc::Controller cntl;
        stub.ReadArchivedFile(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (detail) {
                *detail = "ReadArchivedFile rpc failed: " + cntl.ErrorText();
            }
            return false;
        }
        if (resp.status().code() != zb::rpc::STATUS_OK) {
            if (detail) {
                *detail = "ReadArchivedFile status=" + std::to_string(resp.status().code()) + " msg=" + resp.status().message();
            }
            return false;
        }
        if (resp.data() != payload) {
            if (detail) {
                *detail = "ReadArchivedFile payload mismatch";
            }
            return false;
        }
    }

    if (detail) {
        *detail = "disc=" + disc_id + " archive/read path OK";
    }
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    CaseSummary summary;
    const std::string uniq = UniqueSuffix();

    std::string detail;
    summary.Add("scheduler.get_cluster_view", TestScheduler(FLAGS_scheduler, &detail), detail);

    detail.clear();
    summary.Add("mds.namespace_ops", TestMdsNamespace(FLAGS_mds, uniq, &detail), detail);

    detail.clear();
    summary.Add("real_node.object_rw",
                TestObjectRw(FLAGS_real, FLAGS_real_disk, "real_smoke_object_" + uniq, "real_payload_" + uniq, &detail),
                detail);

    detail.clear();
    summary.Add("virtual_node.object_rw",
                TestObjectRw(FLAGS_virtual_node,
                             FLAGS_virtual_disk,
                             "virtual_smoke_object_" + uniq,
                             "virtual_payload_" + uniq,
                             &detail),
                detail);

    if (FLAGS_expect_optical) {
        detail.clear();
        summary.Add("optical_node.archive_io", TestOptical(FLAGS_optical, FLAGS_optical_disc, uniq, &detail), detail);
    } else {
        summary.Add("optical_node.archive_io", true, "skipped (expect_optical=false)");
    }

    std::cout << "\n=== Module IO Smoke Summary ===\n";
    std::cout << "total=" << summary.total
              << " pass=" << summary.pass
              << " fail=" << summary.fail << std::endl;
    if (summary.fail > 0) {
        std::cout << "failed_cases=";
        for (size_t i = 0; i < summary.failed_cases.size(); ++i) {
            if (i > 0) {
                std::cout << ",";
            }
            std::cout << summary.failed_cases[i];
        }
        std::cout << std::endl;
        return 1;
    }
    return 0;
}
