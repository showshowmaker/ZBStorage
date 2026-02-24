#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "scheduler.pb.h"

DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(node_id, "vpool", "Node id to control");
DEFINE_bool(do_reboot, false, "Also test reboot operation");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

bool CheckSchedOk(const zb::rpc::SchedulerStatus& status, const char* op) {
    if (status.code() != zb::rpc::SCHED_OK) {
        std::cerr << op << " failed: code=" << status.code()
                  << " msg=" << status.message() << std::endl;
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
    if (channel.Init(FLAGS_scheduler.c_str(), &options) != 0) {
        std::cerr << "Failed to init scheduler channel: " << FLAGS_scheduler << std::endl;
        return 1;
    }

    zb::rpc::SchedulerService_Stub stub(&channel);

    zb::rpc::StopNodeRequest stop_req;
    stop_req.set_node_id(FLAGS_node_id);
    stop_req.set_force(false);
    stop_req.set_reason("scheduler_control_test");
    zb::rpc::NodeOperationReply stop_resp;
    brpc::Controller stop_cntl;
    stub.StopNode(&stop_cntl, &stop_req, &stop_resp, nullptr);
    if (stop_cntl.Failed()) {
        std::cerr << "StopNode RPC failed: " << stop_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (!CheckSchedOk(stop_resp.status(), "StopNode")) {
        return 1;
    }

    zb::rpc::StartNodeRequest start_req;
    start_req.set_node_id(FLAGS_node_id);
    start_req.set_reason("scheduler_control_test");
    zb::rpc::NodeOperationReply start_resp;
    brpc::Controller start_cntl;
    stub.StartNode(&start_cntl, &start_req, &start_resp, nullptr);
    if (start_cntl.Failed()) {
        std::cerr << "StartNode RPC failed: " << start_cntl.ErrorText() << std::endl;
        return 1;
    }
    if (!CheckSchedOk(start_resp.status(), "StartNode")) {
        return 1;
    }

    if (FLAGS_do_reboot) {
        zb::rpc::RebootNodeRequest reboot_req;
        reboot_req.set_node_id(FLAGS_node_id);
        reboot_req.set_reason("scheduler_control_test");
        zb::rpc::NodeOperationReply reboot_resp;
        brpc::Controller reboot_cntl;
        stub.RebootNode(&reboot_cntl, &reboot_req, &reboot_resp, nullptr);
        if (reboot_cntl.Failed()) {
            std::cerr << "RebootNode RPC failed: " << reboot_cntl.ErrorText() << std::endl;
            return 1;
        }
        if (!CheckSchedOk(reboot_resp.status(), "RebootNode")) {
            return 1;
        }
    }

    std::cout << "OK scheduler control test passed node_id=" << FLAGS_node_id << std::endl;
    return 0;
}
