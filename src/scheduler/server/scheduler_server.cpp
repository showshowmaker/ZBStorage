#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "../config/SchedulerConfig.h"
#include "../health/FailureDetector.h"
#include "../lifecycle/LifecycleManager.h"
#include "../lifecycle/NodeActuator.h"
#include "../model/ClusterState.h"
#include "../service/SchedulerServiceImpl.h"

DEFINE_string(config, "", "Path to Scheduler config file");
DEFINE_int32(port, 9100, "Port for Scheduler brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

namespace {

void PersistClusterViewSnapshot(const std::string& path, zb::scheduler::ClusterState* state) {
    if (path.empty() || !state) {
        return;
    }

    uint64_t generation = 0;
    std::vector<zb::scheduler::NodeState> nodes;
    state->Snapshot(0, &generation, &nodes);

    std::error_code ec;
    const std::filesystem::path snapshot_path(path);
    if (!snapshot_path.parent_path().empty()) {
        std::filesystem::create_directories(snapshot_path.parent_path(), ec);
        if (ec) {
            std::cerr << "Failed to create scheduler snapshot directory: " << ec.message() << std::endl;
            return;
        }
    }

    const std::filesystem::path tmp_path = snapshot_path.string() + ".tmp";
    std::ofstream out(tmp_path, std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open scheduler snapshot file: " << tmp_path.string() << std::endl;
        return;
    }

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    out << "scheduler_cluster_view_v1\n";
    out << "snapshot_ts_ms=" << now_ms << '\n';
    out << "generation=" << generation << '\n';
    out << "node_count=" << nodes.size() << '\n';
    for (const auto& node : nodes) {
        out << "[node]\n";
        out << "node_id=" << node.node_id << '\n';
        out << "node_type=" << static_cast<int>(node.node_type) << '\n';
        out << "address=" << node.address << '\n';
        out << "weight=" << node.weight << '\n';
        out << "virtual_node_count=" << node.virtual_node_count << '\n';
        out << "group_id=" << node.group_id << '\n';
        out << "role=" << static_cast<int>(node.role) << '\n';
        out << "epoch=" << node.epoch << '\n';
        out << "applied_lsn=" << node.applied_lsn << '\n';
        out << "peer_node_id=" << node.peer_node_id << '\n';
        out << "peer_address=" << node.peer_address << '\n';
        out << "sync_ready=" << (node.sync_ready ? 1 : 0) << '\n';
        out << "health_state=" << static_cast<int>(node.health_state) << '\n';
        out << "admin_state=" << static_cast<int>(node.admin_state) << '\n';
        out << "power_state=" << static_cast<int>(node.power_state) << '\n';
        out << "desired_admin_state=" << static_cast<int>(node.desired_admin_state) << '\n';
        out << "desired_power_state=" << static_cast<int>(node.desired_power_state) << '\n';
        out << "last_heartbeat_ms=" << node.last_heartbeat_ms << '\n';
        out << "disk_count=" << node.disks.size() << '\n';
        for (const auto& item : node.disks) {
            const auto& disk = item.second;
            out << "[disk]\n";
            out << "disk_id=" << disk.disk_id << '\n';
            out << "capacity_bytes=" << disk.capacity_bytes << '\n';
            out << "free_bytes=" << disk.free_bytes << '\n';
            out << "is_healthy=" << (disk.is_healthy ? 1 : 0) << '\n';
            out << "last_update_ms=" << disk.last_update_ms << '\n';
        }
    }
    out.close();
    if (!out) {
        std::cerr << "Failed to flush scheduler snapshot file: " << tmp_path.string() << std::endl;
        return;
    }

    std::filesystem::rename(tmp_path, snapshot_path, ec);
    if (ec) {
        std::filesystem::remove(snapshot_path, ec);
        ec.clear();
        std::filesystem::rename(tmp_path, snapshot_path, ec);
        if (ec) {
            std::cerr << "Failed to finalize scheduler snapshot file: " << ec.message() << std::endl;
        }
    }
}

} // namespace

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
    std::thread snapshot_thread;
    if (!cfg.cluster_view_snapshot_path.empty() && cfg.cluster_view_snapshot_interval_ms > 0) {
        snapshot_thread = std::thread([&]() {
            const uint64_t interval_ms = cfg.cluster_view_snapshot_interval_ms;
            while (!stop.load()) {
                PersistClusterViewSnapshot(cfg.cluster_view_snapshot_path, &state);
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    }

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add Scheduler service" << std::endl;
        stop.store(true);
        tick_thread.join();
        if (snapshot_thread.joinable()) {
            snapshot_thread.join();
        }
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;
    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start Scheduler server on port " << FLAGS_port << std::endl;
        stop.store(true);
        tick_thread.join();
        if (snapshot_thread.joinable()) {
            snapshot_thread.join();
        }
        return 1;
    }

    server.RunUntilAskedToQuit();
    stop.store(true);
    tick_thread.join();
    if (snapshot_thread.joinable()) {
        snapshot_thread.join();
    }
    return 0;
}
