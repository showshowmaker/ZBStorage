#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "../config/SchedulerConfig.h"
#include "../health/FailureDetector.h"
#include "../lifecycle/LifecycleManager.h"
#include "../lifecycle/NodeActuator.h"
#include "../model/ClusterState.h"
#include "../service/SchedulerServiceImpl.h"

DEFINE_string(config, "", "Path to Scheduler config file");
DEFINE_int32(port, 9100, "Port for Scheduler brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string error;
    zb::scheduler::SchedulerConfig cfg = zb::scheduler::SchedulerConfig::LoadFromFile(FLAGS_config, &error);
    if (!error.empty()) {
        std::cerr << "Failed to load config: " << error << std::endl;
        return 1;
    }

    zb::scheduler::FailureDetector detector(cfg.suspect_timeout_ms, cfg.dead_timeout_ms);
    zb::scheduler::ClusterState state(detector);
    zb::scheduler::ShellNodeActuator actuator(cfg.start_cmd_template,
                                              cfg.stop_cmd_template,
                                              cfg.reboot_cmd_template);
    zb::scheduler::LifecycleManager lifecycle(&state, &actuator);
    zb::scheduler::SchedulerServiceImpl service(&state, &lifecycle);

    std::atomic<bool> stop{false};
    std::thread tick_thread([&]() {
        uint64_t interval_ms = cfg.tick_interval_ms > 0 ? cfg.tick_interval_ms : 1000;
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            state.TickHealth();
        }
    });

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add Scheduler service" << std::endl;
        stop.store(true);
        tick_thread.join();
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;
    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start Scheduler server on port " << FLAGS_port << std::endl;
        stop.store(true);
        tick_thread.join();
        return 1;
    }

    server.RunUntilAskedToQuit();
    stop.store(true);
    tick_thread.join();
    return 0;
}
