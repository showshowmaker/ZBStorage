#include <brpc/server.h>
#include <gflags/gflags.h>

#include <iostream>

#include "../allocator/ChunkAllocator.h"
#include "../allocator/NodeStateCache.h"
#include "../config/MdsConfig.h"
#include "../service/MdsServiceImpl.h"
#include "../storage/RocksMetaStore.h"

DEFINE_string(config, "", "Path to MDS config file");
DEFINE_int32(port, 9000, "Port for MDS brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string error;
    zb::mds::MdsConfig cfg = zb::mds::MdsConfig::LoadFromFile(FLAGS_config, &error);
    if (!error.empty()) {
        std::cerr << "Failed to load config: " << error << std::endl;
        return 1;
    }

    zb::mds::RocksMetaStore store;
    if (!store.Open(cfg.db_path, &error)) {
        std::cerr << "Failed to open RocksDB: " << error << std::endl;
        return 1;
    }

    zb::mds::NodeStateCache cache(cfg.nodes);
    zb::mds::ChunkAllocator allocator(&cache);
    zb::mds::MdsServiceImpl service(&store, &allocator, cfg.chunk_size);

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add MDS service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start MDS server on port " << FLAGS_port << std::endl;
        return 1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
