#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <chrono>
#include <iostream>
#include <string>

#include "google/protobuf/empty.pb.h"
#include "real_node.pb.h"

DEFINE_string(server, "127.0.0.1:29080", "Virtual node endpoint");
DEFINE_string(disk, "disk-01", "Disk id to test");
DEFINE_string(chunk_id, "virtual-test-chunk-001", "Chunk id for read/write test");
DEFINE_uint64(write_size, 1024, "Bytes to write");
DEFINE_uint64(read_size, 1024, "Bytes to read");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

bool IsAllX(const std::string& data) {
    for (char ch : data) {
        if (ch != 'x') {
            return false;
        }
    }
    return true;
}

bool CheckStatusOk(const zb::rpc::Status& status, const char* op_name) {
    if (status.code() != zb::rpc::STATUS_OK) {
        std::cerr << op_name << " failed, code=" << status.code()
                  << ", msg=" << status.message() << std::endl;
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
    if (channel.Init(FLAGS_server.c_str(), &options) != 0) {
        std::cerr << "Failed to init channel to " << FLAGS_server << std::endl;
        return 1;
    }

    zb::rpc::RealNodeService_Stub stub(&channel);

    google::protobuf::Empty empty;
    zb::rpc::DiskReportReply disk_reply;
    brpc::Controller disk_cntl;
    stub.GetDiskReport(&disk_cntl, &empty, &disk_reply, nullptr);
    if (disk_cntl.Failed()) {
        std::cerr << "GetDiskReport RPC failed: " << disk_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (!CheckStatusOk(disk_reply.status(), "GetDiskReport")) {
        return 1;
    }

    bool disk_found = false;
    for (const auto& item : disk_reply.reports()) {
        if (item.id() == FLAGS_disk) {
            disk_found = true;
            break;
        }
    }
    if (!disk_found) {
        std::cerr << "Disk " << FLAGS_disk << " not found in disk report" << std::endl;
        return 1;
    }

    std::string write_data(static_cast<size_t>(FLAGS_write_size), 'a');
    zb::rpc::WriteChunkRequest write_req;
    write_req.set_disk_id(FLAGS_disk);
    write_req.set_chunk_id(FLAGS_chunk_id);
    write_req.set_offset(0);
    write_req.set_data(write_data);

    zb::rpc::WriteChunkReply write_reply;
    brpc::Controller write_cntl;
    auto write_begin = std::chrono::steady_clock::now();
    stub.WriteChunk(&write_cntl, &write_req, &write_reply, nullptr);
    auto write_end = std::chrono::steady_clock::now();
    if (write_cntl.Failed()) {
        std::cerr << "WriteChunk RPC failed: " << write_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (!CheckStatusOk(write_reply.status(), "WriteChunk")) {
        return 1;
    }
    if (write_reply.bytes() != FLAGS_write_size) {
        std::cerr << "WriteChunk bytes mismatch, expect=" << FLAGS_write_size
                  << ", got=" << write_reply.bytes() << std::endl;
        return 1;
    }

    zb::rpc::ReadChunkRequest read_req;
    read_req.set_disk_id(FLAGS_disk);
    read_req.set_chunk_id(FLAGS_chunk_id);
    read_req.set_offset(0);
    read_req.set_size(FLAGS_read_size);

    zb::rpc::ReadChunkReply read_reply;
    brpc::Controller read_cntl;
    auto read_begin = std::chrono::steady_clock::now();
    stub.ReadChunk(&read_cntl, &read_req, &read_reply, nullptr);
    auto read_end = std::chrono::steady_clock::now();
    if (read_cntl.Failed()) {
        std::cerr << "ReadChunk RPC failed: " << read_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (!CheckStatusOk(read_reply.status(), "ReadChunk")) {
        return 1;
    }
    if (read_reply.bytes() != FLAGS_read_size || read_reply.data().size() != FLAGS_read_size) {
        std::cerr << "ReadChunk length mismatch, expect=" << FLAGS_read_size
                  << ", got_bytes=" << read_reply.bytes()
                  << ", got_data_size=" << read_reply.data().size() << std::endl;
        return 1;
    }
    if (!IsAllX(read_reply.data())) {
        std::cerr << "ReadChunk content mismatch: expected all 'x'" << std::endl;
        return 1;
    }

    zb::rpc::WriteChunkRequest bad_req;
    bad_req.set_disk_id("bad-disk");
    bad_req.set_chunk_id(FLAGS_chunk_id);
    bad_req.set_offset(0);
    bad_req.set_data("abc");
    zb::rpc::WriteChunkReply bad_reply;
    brpc::Controller bad_cntl;
    stub.WriteChunk(&bad_cntl, &bad_req, &bad_reply, nullptr);
    if (bad_cntl.Failed()) {
        std::cerr << "WriteChunk(bad-disk) RPC failed: " << bad_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (bad_reply.status().code() != zb::rpc::STATUS_NOT_FOUND) {
        std::cerr << "WriteChunk(bad-disk) expect STATUS_NOT_FOUND, got="
                  << bad_reply.status().code() << std::endl;
        return 1;
    }

    auto write_ms = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_begin).count();
    auto read_ms = std::chrono::duration_cast<std::chrono::milliseconds>(read_end - read_begin).count();

    std::cout << "OK virtual node test passed"
              << " server=" << FLAGS_server
              << " disk=" << FLAGS_disk
              << " write_ms=" << write_ms
              << " read_ms=" << read_ms
              << " read_bytes=" << read_reply.bytes()
              << std::endl;
    return 0;
}
