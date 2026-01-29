#include <brpc/channel.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "real_node.pb.h"

DEFINE_string(server, "127.0.0.1:8000", "Real node server address");
DEFINE_string(disk_id, "disk-01", "Target disk id");
DEFINE_string(chunk_id, "550e8400-e29b-41d4-a716-446655440000", "Chunk id");
DEFINE_string(write_data, "hello", "Payload for write");
DEFINE_uint64(offset, 0, "Write/read offset");
DEFINE_uint64(read_size, 5, "Read size");
DEFINE_string(mode, "both", "Mode: write/read/both");

namespace {

bool StatusOk(const zb::rpc::Status& status) {
    return status.code() == zb::rpc::STATUS_OK;
}

void PrintStatus(const zb::rpc::Status& status) {
    std::cout << "status=" << status.code() << " message=" << status.message() << std::endl;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    options.max_retry = 0;

    if (channel.Init(FLAGS_server.c_str(), &options) != 0) {
        std::cerr << "Failed to init channel to " << FLAGS_server << std::endl;
        return 1;
    }

    zb::rpc::RealNodeService_Stub stub(&channel);
    brpc::Controller controller;

    if (FLAGS_mode == "write" || FLAGS_mode == "both") {
        zb::rpc::WriteChunkRequest request;
        request.set_disk_id(FLAGS_disk_id);
        request.set_chunk_id(FLAGS_chunk_id);
        request.set_offset(FLAGS_offset);
        request.set_data(FLAGS_write_data);

        zb::rpc::WriteChunkReply response;
        stub.WriteChunk(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            std::cerr << "WriteChunk RPC failed: " << controller.ErrorText() << std::endl;
            return 1;
        }
        std::cout << "WriteChunk bytes=" << response.bytes() << std::endl;
        PrintStatus(response.status());
        if (!StatusOk(response.status())) {
            return 1;
        }
    }

    if (FLAGS_mode == "read" || FLAGS_mode == "both") {
        zb::rpc::ReadChunkRequest request;
        request.set_disk_id(FLAGS_disk_id);
        request.set_chunk_id(FLAGS_chunk_id);
        request.set_offset(FLAGS_offset);
        request.set_size(FLAGS_read_size);

        controller.Reset();
        zb::rpc::ReadChunkReply response;
        stub.ReadChunk(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            std::cerr << "ReadChunk RPC failed: " << controller.ErrorText() << std::endl;
            return 1;
        }
        std::cout << "ReadChunk bytes=" << response.bytes() << " data=" << response.data() << std::endl;
        PrintStatus(response.status());
        if (!StatusOk(response.status())) {
            return 1;
        }
    }

    return 0;
}
