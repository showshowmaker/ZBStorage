#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>

#include "../allocator/ObjectAllocator.h"
#include "../allocator/NodeStateCache.h"
#include "../allocator/PGManager.h"
#include "../archive/ArchiveBatchStager.h"
#include "../archive/ArchiveCandidateQueue.h"
#include "../archive/ArchiveLeaseManager.h"
#include "../archive/OpticalArchiveManager.h"
#include "../config/MdsConfig.h"
#include "../gc/GcManager.h"
#include "../service/MdsServiceImpl.h"
#include "../storage/RocksMetaStore.h"
#include "scheduler.pb.h"

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
    zb::mds::PGManager::Options pg_options;
    pg_options.pg_count = cfg.pg_count;
    pg_options.replica = cfg.replica > 0 ? cfg.replica : 1;
    zb::mds::PGManager pg_manager(pg_options);
    if (!cfg.nodes.empty()) {
        const uint64_t initial_pg_epoch = cfg.pg_view_epoch > 0 ? cfg.pg_view_epoch : 1;
        if (!pg_manager.RebuildFromNodes(cfg.nodes, initial_pg_epoch, &error)) {
            std::cerr << "Failed to initialize PG placement view: " << error << std::endl;
            return 1;
        }
    }
    zb::mds::ObjectAllocator allocator(&cache, &pg_manager);
    zb::mds::ArchiveCandidateQueue candidate_queue(cfg.archive_candidate_queue_size);
    zb::mds::ArchiveLeaseManager::Options lease_options;
    lease_options.default_lease_ms = cfg.archive_lease_default_ms;
    lease_options.min_lease_ms = cfg.archive_lease_min_ms;
    lease_options.max_lease_ms = cfg.archive_lease_max_ms;
    zb::mds::ArchiveLeaseManager lease_manager(&store, lease_options);
    zb::mds::ArchiveBatchStager batch_stager;
    zb::mds::MdsServiceImpl service(&store, &allocator, cfg.object_unit_size, &candidate_queue, &lease_manager);
    zb::mds::MdsServiceImpl::LayoutObjectOptions layout_options;
    layout_options.replica_count = cfg.layout_object_replica_count;
    layout_options.scrub_on_load = cfg.layout_object_scrub_on_load;
    service.SetLayoutObjectOptions(layout_options);
    service.SetSimplifiedAnchorMetadataMode(cfg.enable_simplified_anchor_metadata);
    std::unique_ptr<zb::mds::OpticalArchiveManager> archive_manager;
    if (cfg.enable_optical_archive) {
        zb::mds::ArchiveBatchStager::Options stager_options;
        stager_options.disc_size_bytes = cfg.archive_disc_size_bytes;
        stager_options.strict_full_disc = cfg.archive_strict_full_disc;
        stager_options.max_batch_age_ms = cfg.archive_batch_max_age_ms;
        std::string staging_dir = cfg.archive_staging_dir;
        if (staging_dir.empty()) {
            staging_dir = cfg.db_path + "/archive_staging";
        }
        if (!batch_stager.Init(staging_dir, stager_options, &error)) {
            std::cerr << "Failed to init archive batch stager: " << error << std::endl;
            return 1;
        }

        zb::mds::OpticalArchiveManager::Options options;
        options.archive_trigger_bytes = cfg.archive_trigger_bytes;
        options.archive_target_bytes = cfg.archive_target_bytes;
        options.cold_file_ttl_sec = cfg.cold_file_ttl_sec;
        options.max_objects_per_round = cfg.archive_max_objects_per_round;
        options.default_object_unit_size = cfg.object_unit_size;
        archive_manager =
            std::make_unique<zb::mds::OpticalArchiveManager>(&store,
                                                              &cache,
                                                              &candidate_queue,
                                                              &batch_stager,
                                                              &lease_manager,
                                                              options);
    }

    std::atomic<bool> stop_sync{false};
    std::thread sync_thread;
    std::thread archive_thread;
    std::thread layout_gc_thread;
    std::unique_ptr<zb::mds::GcManager> gc_manager;
    if (!cfg.scheduler_address.empty()) {
        sync_thread = std::thread([&]() {
            uint64_t min_generation = 0;
            uint32_t refresh_ms = cfg.scheduler_refresh_ms > 0 ? cfg.scheduler_refresh_ms : 2000;
            std::unique_ptr<brpc::Channel> channel;
            while (!stop_sync.load()) {
                if (!channel) {
                    auto new_channel = std::make_unique<brpc::Channel>();
                    brpc::ChannelOptions options;
                    options.protocol = "baidu_std";
                    options.timeout_ms = 2000;
                    options.max_retry = 0;
                    if (new_channel->Init(cfg.scheduler_address.c_str(), &options) != 0) {
                        std::cerr << "Failed to connect Scheduler at " << cfg.scheduler_address << std::endl;
                        std::this_thread::sleep_for(std::chrono::milliseconds(refresh_ms));
                        continue;
                    }
                    channel = std::move(new_channel);
                }
                zb::rpc::SchedulerService_Stub stub(channel.get());
                zb::rpc::GetClusterViewRequest request;
                request.set_min_generation(min_generation);
                zb::rpc::GetClusterViewReply response;
                brpc::Controller cntl;
                stub.GetClusterView(&cntl, &request, &response, nullptr);
                if (cntl.Failed()) {
                    channel.reset();
                } else if (response.status().code() == zb::rpc::SCHED_OK) {
                    std::vector<zb::mds::NodeInfo> nodes;
                    std::unordered_map<std::string, const zb::rpc::NodeView*> primary_by_group;
                    std::unordered_map<std::string, const zb::rpc::NodeView*> secondary_by_group;
                    std::vector<std::string> groups;
                    groups.reserve(static_cast<size_t>(response.nodes_size()));

                    for (const auto& n : response.nodes()) {
                        std::string group_id = n.group_id().empty() ? n.node_id() : n.group_id();
                        if (primary_by_group.find(group_id) == primary_by_group.end() &&
                            secondary_by_group.find(group_id) == secondary_by_group.end()) {
                            groups.push_back(group_id);
                        }
                        if (n.role() == zb::rpc::NODE_ROLE_PRIMARY) {
                            primary_by_group[group_id] = &n;
                        } else if (n.role() == zb::rpc::NODE_ROLE_SECONDARY) {
                            secondary_by_group[group_id] = &n;
                        } else if (primary_by_group.find(group_id) == primary_by_group.end()) {
                            primary_by_group[group_id] = &n;
                        }
                    }

                    nodes.reserve(groups.size());
                    for (const auto& group_id : groups) {
                        const zb::rpc::NodeView* primary = nullptr;
                        const zb::rpc::NodeView* secondary = nullptr;
                        auto pit = primary_by_group.find(group_id);
                        if (pit != primary_by_group.end()) {
                            primary = pit->second;
                        }
                        auto sit = secondary_by_group.find(group_id);
                        if (sit != secondary_by_group.end()) {
                            secondary = sit->second;
                        }
                        if (!primary && secondary) {
                            primary = secondary;
                            secondary = nullptr;
                        }
                        if (!primary) {
                            continue;
                        }

                        zb::mds::NodeInfo info;
                        info.node_id = primary->node_id();
                        info.address = primary->address();
                        info.group_id = group_id;
                        if (primary->node_type() == zb::rpc::NODE_VIRTUAL_POOL) {
                            info.type = zb::mds::NodeType::kVirtual;
                        } else if (primary->node_type() == zb::rpc::NODE_OPTICAL) {
                            info.type = zb::mds::NodeType::kOptical;
                        } else {
                            info.type = zb::mds::NodeType::kReal;
                        }
                        info.weight = primary->weight() > 0 ? primary->weight() : 1;
                        info.virtual_node_count = primary->virtual_node_count() > 0
                                                      ? primary->virtual_node_count()
                                                      : 1;
                        info.epoch = primary->epoch() > 0 ? primary->epoch() : 1;
                        info.is_primary = true;
                        info.sync_ready = primary->sync_ready();
                        info.secondary_node_id = secondary ? secondary->node_id() : "";
                        info.secondary_address = secondary ? secondary->address() : "";
                        info.allocatable = (primary->health_state() == zb::rpc::NODE_HEALTH_HEALTHY) &&
                                           (primary->admin_state() == zb::rpc::NODE_ADMIN_ENABLED) &&
                                           (primary->power_state() == zb::rpc::NODE_POWER_ON);
                        for (const auto& d : primary->disks()) {
                            if (!d.is_healthy()) {
                                continue;
                            }
                            zb::mds::DiskInfo disk;
                            disk.disk_id = d.disk_id();
                            disk.capacity_bytes = d.capacity_bytes();
                            disk.free_bytes = d.free_bytes();
                            disk.is_healthy = d.is_healthy();
                            info.disks.push_back(std::move(disk));
                        }
                        if (info.disks.empty()) {
                            info.allocatable = false;
                        }
                        nodes.push_back(std::move(info));
                    }
                    uint64_t pg_epoch = response.generation();
                    if (pg_epoch == 0) {
                        pg_epoch = pg_manager.CurrentEpoch() + 1;
                        if (pg_epoch == 0) {
                            pg_epoch = 1;
                        }
                    }
                    std::string pg_error;
                    if (!pg_manager.RebuildFromNodes(nodes, pg_epoch, &pg_error)) {
                        std::cerr << "Failed to rebuild PG placement view: " << pg_error << std::endl;
                    }
                    cache.ReplaceNodes(std::move(nodes));
                    min_generation = response.generation();
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(refresh_ms));
            }
        });
    }
    if (archive_manager) {
        archive_thread = std::thread([&]() {
            uint32_t interval_ms = cfg.archive_scan_interval_ms > 0 ? cfg.archive_scan_interval_ms : 5000;
            while (!stop_sync.load()) {
                std::string archive_error;
                archive_manager->RunOnce(&archive_error);
                if (!archive_error.empty()) {
                    std::cerr << "archive round warning: " << archive_error << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    }
    if (cfg.enable_layout_gc) {
        zb::mds::GcManager::Options gc_options;
        gc_options.orphan_grace_ms = cfg.layout_gc_orphan_grace_ms;
        gc_options.max_delete_per_round = cfg.layout_gc_max_delete_per_round;
        gc_options.dry_run = cfg.layout_gc_dry_run;
        gc_options.paused = cfg.layout_gc_paused;
        gc_manager = std::make_unique<zb::mds::GcManager>(&store, gc_options);
        layout_gc_thread = std::thread([&]() {
            const uint32_t interval_ms = cfg.layout_gc_interval_ms > 0 ? cfg.layout_gc_interval_ms : 30000;
            while (!stop_sync.load()) {
                std::string gc_error;
                gc_manager->RunOnce(&gc_error);
                if (!gc_error.empty()) {
                    std::cerr << "layout gc round warning: " << gc_error << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    }

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add MDS service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start MDS server on port " << FLAGS_port << std::endl;
        stop_sync.store(true);
        if (sync_thread.joinable()) {
            sync_thread.join();
        }
        if (archive_thread.joinable()) {
            archive_thread.join();
        }
        if (layout_gc_thread.joinable()) {
            layout_gc_thread.join();
        }
        return 1;
    }

    server.RunUntilAskedToQuit();
    stop_sync.store(true);
    if (sync_thread.joinable()) {
        sync_thread.join();
    }
    if (archive_thread.joinable()) {
        archive_thread.join();
    }
    if (layout_gc_thread.joinable()) {
        layout_gc_thread.join();
    }
    return 0;
}
