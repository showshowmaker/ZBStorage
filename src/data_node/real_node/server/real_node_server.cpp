#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>

#include "../config/NodeConfig.h"
#include "../io/DiskManager.h"
#include "../io/IOExecutor.h"
#include "../io/LocalPathResolver.h"
#include "../service/BrpcStorageService.h"
#include "../service/StorageServiceImpl.h"

DEFINE_string(config, "", "Path to real node config file");
DEFINE_int32(port, 8000, "Port for real node brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string config_error;
    zb::real_node::NodeConfig cfg = zb::real_node::NodeConfig::LoadFromFile(FLAGS_config, &config_error);
    if (!config_error.empty()) {
        std::cerr << "Failed to load config: " << config_error << std::endl;
        return 1;
    }
    zb::real_node::DiskManager disk_manager;
    zb::msg::Status init_status;

    if (!cfg.disks_env.empty()) {
        init_status = disk_manager.InitFromConfig(cfg.disks_env);
    } else if (!cfg.data_root.empty()) {
        init_status = disk_manager.InitFromDataRoot(cfg.data_root);
    } else {
        std::cerr << "Missing disk config in file: set ZB_DISKS or DATA_ROOT" << std::endl;
        return 1;
    }

    if (!init_status.ok()) {
        std::cerr << "DiskManager init failed: " << init_status.message << std::endl;
        return 1;
    }

    zb::real_node::LocalPathResolver path_resolver;
    zb::real_node::IOExecutor io_executor;
    zb::real_node::StorageServiceImpl storage_service(&disk_manager, &path_resolver, &io_executor);
    zb::real_node::BrpcStorageService brpc_service(&storage_service);

    brpc::Server server;
    if (server.AddService(&brpc_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add brpc service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start brpc server on port " << FLAGS_port << std::endl;
        return 1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
