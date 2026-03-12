#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "config.h"
#include "mds_sst_generator.h"

namespace zb::meta_gen {

namespace {

bool StartsWith(const std::string& s, const std::string& prefix) {
    return s.rfind(prefix, 0) == 0;
}

bool ParseU64(const std::string& value, uint64_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDouble(const std::string& value, double* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = std::stod(value);
        return true;
    } catch (...) {
        return false;
    }
}

std::string Fmt2(long double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << v;
    return oss.str();
}

std::string Fmt0(long double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(0) << v;
    return oss.str();
}

void PrintUsage(std::ostream& os) {
    os << "Usage: mds_sst_gen_tool --output_dir=<path> [options]\n"
          "Options:\n"
          "  --output_dir=<path>                  Required: output directory for sst and manifest\n"
          "  --sst_prefix=<str>                   SST file prefix (default: mds_meta)\n"
          "  --max_kv_per_sst=<u64>               Max kv pairs per shard (default: 1000000)\n"
          "  --total_files=<u64>                  Total files (default: 100000000000)\n"
          "  --namespace_count=<u64>              Namespace count (default: 1000)\n"
          "  --branch_factor=<u64>                Branch factor (default: 64)\n"
          "  --max_depth=<u64>                    Max depth (default: 4)\n"
          "  --max_files_per_leaf=<u64>           Max files per leaf (default: 2048)\n"
          "  --disk_replica_ratio=<double>        Disk occupancy ratio [0,1] (default: 0.5)\n"
          "  --optical_node_count=<u64>           Optical nodes (default: 10000)\n"
          "  --discs_per_optical_node=<u64>       Optical discs per node (default: 10000)\n"
          "  --virtual_node_count=<u64>           Virtual nodes (default: 100)\n"
          "  --virtual_disks_per_node=<u64>       Virtual disks per node (default: 16)\n"
          "  --real_node_count=<u64>              Real nodes (default: 1)\n"
          "  --real_disks_per_node=<u64>          Real disks per node (default: 24)\n"
          "  --progress_interval_files=<u64>      Progress log interval by file count (default: 1000000)\n"
          "  --progress_interval_sec=<u64>        Progress log interval by seconds (default: 30)\n"
          "  --seed=<u64>                         File size sampler seed\n"
          "  --now_seconds=<u64>                  Override inode timestamp\n"
          "  --help                               Show this help\n";
}

} // namespace

int RunMdsSstGenTool(int argc, char** argv) {
    ClusterScaleConfig cluster = DefaultClusterScaleConfig();
    DirectoryLayoutConfig dir_cfg = DefaultDirectoryLayoutConfig();
    FileSizeSamplerConfig file_cfg = DefaultFileSizeSamplerConfig();
    MdsSstGenConfig gen_cfg;

    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i] ? argv[i] : "");
        if (arg == "--help" || arg == "-h") {
            PrintUsage(std::cout);
            return 0;
        }
        if (!StartsWith(arg, "--")) {
            std::cerr << "invalid argument: " << arg << "\n";
            return 1;
        }
        const size_t eq = arg.find('=');
        if (eq == std::string::npos) {
            std::cerr << "argument must use --key=value: " << arg << "\n";
            return 1;
        }
        const std::string key = arg.substr(2, eq - 2);
        const std::string value = arg.substr(eq + 1);

        bool ok = true;
        if (key == "output_dir") {
            gen_cfg.output_dir = value;
        } else if (key == "sst_prefix") {
            gen_cfg.sst_prefix = value;
        } else if (key == "max_kv_per_sst") {
            ok = ParseU64(value, &gen_cfg.max_kv_per_sst);
        } else if (key == "total_files") {
            ok = ParseU64(value, &cluster.total_files);
        } else if (key == "namespace_count") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            cluster.namespace_count = static_cast<uint32_t>(tmp);
        } else if (key == "branch_factor") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            dir_cfg.branch_factor = static_cast<uint32_t>(tmp);
        } else if (key == "max_depth") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            dir_cfg.max_depth = static_cast<uint32_t>(tmp);
        } else if (key == "max_files_per_leaf") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            dir_cfg.max_files_per_leaf = static_cast<uint32_t>(tmp);
        } else if (key == "disk_replica_ratio") {
            ok = ParseDouble(value, &cluster.disk_replica_ratio);
        } else if (key == "optical_node_count") {
            ok = ParseU64(value, &cluster.optical_node_count);
        } else if (key == "discs_per_optical_node") {
            ok = ParseU64(value, &cluster.discs_per_optical_node);
        } else if (key == "virtual_node_count") {
            ok = ParseU64(value, &cluster.virtual_node_count);
        } else if (key == "virtual_disks_per_node") {
            ok = ParseU64(value, &cluster.virtual_disks_per_node);
        } else if (key == "real_node_count") {
            ok = ParseU64(value, &cluster.real_node_count);
        } else if (key == "real_disks_per_node") {
            ok = ParseU64(value, &cluster.real_disks_per_node);
        } else if (key == "progress_interval_files") {
            ok = ParseU64(value, &gen_cfg.progress_interval_files);
        } else if (key == "progress_interval_sec") {
            ok = ParseU64(value, &gen_cfg.progress_interval_sec);
        } else if (key == "seed") {
            ok = ParseU64(value, &file_cfg.seed);
        } else if (key == "now_seconds") {
            ok = ParseU64(value, &gen_cfg.now_seconds);
        } else {
            std::cerr << "unknown option: --" << key << "\n";
            return 1;
        }
        if (!ok) {
            std::cerr << "invalid value for --" << key << ": " << value << "\n";
            return 1;
        }
    }

    if (gen_cfg.output_dir.empty()) {
        std::cerr << "--output_dir is required\n";
        return 1;
    }
    if (cluster.disk_replica_ratio < 0.0 || cluster.disk_replica_ratio > 1.0) {
        std::cerr << "disk_replica_ratio must be in [0,1]\n";
        return 1;
    }

    MdsSstGenStats stats;
    std::string error;
    if (!MdsSstGenerator::Generate(cluster, dir_cfg, file_cfg, gen_cfg, &stats, &error)) {
        std::cerr << "mds sst generation failed: " << error << "\n";
        return 2;
    }

    const std::string manifest_path = gen_cfg.output_dir + "/manifest.txt";
    std::ofstream ofs(manifest_path, std::ios::out | std::ios::trunc);
    if (!ofs.is_open()) {
        std::cerr << "failed to write manifest: " << manifest_path << "\n";
        return 3;
    }

    const long double gib = 1024.0L * 1024.0L * 1024.0L;
    const long double tib = gib * 1024.0L;
    ofs << "mds_sst_gen_manifest\n";
    ofs << "inode_count=" << stats.inode_count << "\n";
    ofs << "dentry_count=" << stats.dentry_count << "\n";
    ofs << "anchor_count=" << stats.anchor_count << "\n";
    ofs << "kv_count=" << stats.kv_count << "\n";
    ofs << "sst_count=" << stats.sst_count << "\n";
    ofs << "next_inode=" << stats.next_inode << "\n";
    ofs << "total_files=" << cluster.total_files << "\n";
    ofs << "files_with_disk_anchor=" << stats.files_with_disk_anchor << "\n";
    ofs << "files_with_optical_anchor=" << stats.files_with_optical_anchor << "\n";
    ofs << "sampled_total_bytes=" << Fmt0(stats.sampled_total_bytes) << "\n";
    if (cluster.total_files > 0) {
        ofs << "avg_file_size_bytes=" << Fmt0(stats.sampled_total_bytes /
                                              static_cast<long double>(cluster.total_files)) << "\n";
    } else {
        ofs << "avg_file_size_bytes=0\n";
    }
    ofs << "sampled_total_gib=" << Fmt2(stats.sampled_total_bytes / gib) << "\n";
    ofs << "disk_budget_tib=" << Fmt2(stats.disk_budget_bytes / tib) << "\n";
    ofs << "disk_used_tib=" << Fmt2(stats.disk_used_bytes / tib) << "\n";
    ofs << "sst_files_begin\n";
    for (const auto& f : stats.sst_files) {
        ofs << f << "\n";
    }
    ofs << "sst_files_end\n";
    ofs.close();

    std::cout << "MDS SST generation done.\n";
    std::cout << "manifest: " << manifest_path << "\n";
    std::cout << "sst_count=" << stats.sst_count << ", kv_count=" << stats.kv_count << "\n";
    return 0;
}

} // namespace zb::meta_gen

int main(int argc, char** argv) {
    return zb::meta_gen::RunMdsSstGenTool(argc, argv);
}
