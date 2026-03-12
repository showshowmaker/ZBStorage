#include "disk_meta_generator.h"

#include <chrono>
#include <fstream>
#include <string>
#include <vector>

#include "io_utils.h"
#include "placement.h"
#include "workload_enumerator.h"

namespace zb::meta_gen {

namespace {

uint64_t NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

uint64_t BuildPreloadSeed(uint64_t inode_id, uint64_t file_size) {
    return (inode_id * 11400714819323198485ULL) ^ (file_size + 0x9e3779b97f4a7c15ULL);
}

struct NodeSink {
    std::string node_id;
    std::string file_path;
    std::ofstream out;
    uint64_t records{0};
};

bool InitNodeSinks(const std::string& root,
                   const std::string& node_prefix,
                   uint64_t node_count,
                   std::vector<NodeSink>* sinks,
                   std::string* error) {
    if (!sinks) {
        return false;
    }
    sinks->clear();
    sinks->reserve(static_cast<size_t>(node_count));
    for (uint64_t i = 0; i < node_count; ++i) {
        NodeSink sink;
        sink.node_id = node_prefix + std::to_string(i + 1);
        const std::string node_dir = JoinPath(root, sink.node_id);
        if (!EnsureDirRecursive(node_dir, error)) {
            return false;
        }
        sink.file_path = JoinPath(node_dir, "file_meta.tsv");
        sink.out.open(sink.file_path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!sink.out.is_open()) {
            if (error) {
                *error = "failed to open file meta sink: " + sink.file_path;
            }
            return false;
        }
        sinks->push_back(std::move(sink));
    }
    return true;
}

} // namespace

bool DiskMetaGenerator::Generate(const ClusterScaleConfig& cluster,
                                 const DirectoryLayoutConfig& dir_cfg,
                                 const FileSizeSamplerConfig& file_cfg,
                                 const DiskMetaGenConfig& gen_cfg,
                                 DiskMetaGenStats* out,
                                 std::string* error) {
    if (!out) {
        if (error) {
            *error = "output stats is null";
        }
        return false;
    }
    if (gen_cfg.output_dir.empty()) {
        if (error) {
            *error = "output_dir is empty";
        }
        return false;
    }

    const std::string root = NormalizePath(gen_cfg.output_dir);
    if (!EnsureDirRecursive(root, error)) {
        return false;
    }
    const std::string real_root = JoinPath(root, "real_nodes");
    const std::string virtual_root = JoinPath(root, "virtual_nodes");
    if (!EnsureDirRecursive(real_root, error) || !EnsureDirRecursive(virtual_root, error)) {
        return false;
    }

    std::vector<NodeSink> real_sinks;
    std::vector<NodeSink> virtual_sinks;
    if (!InitNodeSinks(real_root, "real-", cluster.real_node_count, &real_sinks, error)) {
        return false;
    }
    if (!InitNodeSinks(virtual_root, "virtual-", cluster.virtual_node_count, &virtual_sinks, error)) {
        return false;
    }

    const long double real_tb =
        static_cast<long double>(cluster.real_node_count) *
        static_cast<long double>(cluster.real_disks_per_node) *
        static_cast<long double>(cluster.real_disk_size_tb);
    const long double virtual_tb =
        static_cast<long double>(cluster.virtual_node_count) *
        static_cast<long double>(cluster.virtual_disks_per_node) *
        static_cast<long double>(cluster.virtual_disk_size_tb);
    const long double budget_bytes =
        (real_tb + virtual_tb) * cluster.disk_replica_ratio * 1000.0L * 1000.0L * 1000.0L * 1000.0L;
    long double remaining = budget_bytes;

    const uint64_t now_sec = gen_cfg.now_seconds > 0 ? gen_cfg.now_seconds : NowSeconds();
    const uint64_t now_ms = now_sec * 1000ULL;

    DiskMetaGenStats stats;
    stats.disk_budget_bytes = budget_bytes;

    bool ok = WorkloadEnumerator::EnumerateFiles(
        cluster,
        dir_cfg,
        file_cfg,
        [&](const FileWorkItem& item) -> bool {
            ++stats.total_files;
            if (remaining < static_cast<long double>(item.file_size)) {
                return true;
            }
            const DiskPlacement p = PickDiskPlacement(cluster, item.inode_id);
            if (!p.valid) {
                return true;
            }
            remaining -= static_cast<long double>(item.file_size);
            ++stats.files_with_disk_meta;

            std::ofstream* sink = nullptr;
            if (p.is_real_node) {
                if (p.node_index >= real_sinks.size()) {
                    if (error) {
                        *error = "real node index out of range while writing file meta";
                    }
                    return false;
                }
                sink = &real_sinks[static_cast<size_t>(p.node_index)].out;
                ++real_sinks[static_cast<size_t>(p.node_index)].records;
                ++stats.real_node_file_meta_records;
            } else {
                if (p.node_index >= virtual_sinks.size()) {
                    if (error) {
                        *error = "virtual node index out of range while writing file meta";
                    }
                    return false;
                }
                sink = &virtual_sinks[static_cast<size_t>(p.node_index)].out;
                ++virtual_sinks[static_cast<size_t>(p.node_index)].records;
                ++stats.virtual_node_file_meta_records;
            }
            if (!sink) {
                return false;
            }
            (*sink) << item.inode_id << '\t'
                    << item.file_size << '\t'
                    << gen_cfg.object_unit_size << '\t'
                    << 1 << '\t'
                    << now_sec << '\t'
                    << now_ms << '\t'
                    << gen_cfg.txid_prefix << "-" << item.inode_id;
            if (!p.is_real_node) {
                (*sink) << '\t'
                        << p.disk_id << '\t'
                        << BuildPreloadSeed(item.inode_id, item.file_size);
            }
            (*sink) << '\n';
            return sink->good();
        },
        error);
    if (!ok) {
        return false;
    }

    for (auto& sink : real_sinks) {
        sink.out.flush();
        if (!sink.out.good()) {
            if (error) {
                *error = "failed to flush " + sink.file_path;
            }
            return false;
        }
    }
    for (auto& sink : virtual_sinks) {
        sink.out.flush();
        if (!sink.out.good()) {
            if (error) {
                *error = "failed to flush " + sink.file_path;
            }
            return false;
        }
    }

    stats.disk_used_bytes = budget_bytes - remaining;
    *out = std::move(stats);
    return true;
}

} // namespace zb::meta_gen
