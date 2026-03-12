#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

#include "config.h"
#include "disk_meta_generator.h"

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

void PrintUsage(std::ostream& os) {
    os << "Usage: disk_meta_gen_tool --output_dir=<path> [options]\n"
          "Options:\n"
          "  --output_dir=<path>                  Required: output root\n"
          "  --total_files=<u64>                  Total files\n"
          "  --namespace_count=<u64>              Namespace count\n"
          "  --disk_replica_ratio=<double>        Disk occupancy ratio [0,1]\n"
          "  --virtual_node_count=<u64>           Virtual node count\n"
          "  --virtual_disks_per_node=<u64>       Virtual disks per node\n"
          "  --real_node_count=<u64>              Real node count\n"
          "  --real_disks_per_node=<u64>          Real disks per node\n"
          "  --branch_factor=<u64>                Dir branch factor\n"
          "  --max_depth=<u64>                    Dir max depth\n"
          "  --max_files_per_leaf=<u64>           Max files per leaf\n"
          "  --progress_interval_files=<u64>      Progress log interval by file count (default: 1000000)\n"
          "  --progress_interval_sec=<u64>        Progress log interval by seconds (default: 30)\n"
          "  --seed=<u64>                         Size sampler seed\n"
          "  --object_unit_size=<u64>             Object unit bytes\n"
          "  --now_seconds=<u64>                  Timestamp override\n"
          "  --txid_prefix=<str>                  Txid prefix\n";
}

} // namespace

int RunDiskMetaGenTool(int argc, char** argv) {
    ClusterScaleConfig cluster = DefaultClusterScaleConfig();
    DirectoryLayoutConfig dir_cfg = DefaultDirectoryLayoutConfig();
    FileSizeSamplerConfig file_cfg = DefaultFileSizeSamplerConfig();
    DiskMetaGenConfig gen_cfg;

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
        } else if (key == "total_files") {
            ok = ParseU64(value, &cluster.total_files);
        } else if (key == "namespace_count") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            cluster.namespace_count = static_cast<uint32_t>(tmp);
        } else if (key == "disk_replica_ratio") {
            ok = ParseDouble(value, &cluster.disk_replica_ratio);
        } else if (key == "virtual_node_count") {
            ok = ParseU64(value, &cluster.virtual_node_count);
        } else if (key == "virtual_disks_per_node") {
            ok = ParseU64(value, &cluster.virtual_disks_per_node);
        } else if (key == "real_node_count") {
            ok = ParseU64(value, &cluster.real_node_count);
        } else if (key == "real_disks_per_node") {
            ok = ParseU64(value, &cluster.real_disks_per_node);
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
        } else if (key == "progress_interval_files") {
            ok = ParseU64(value, &gen_cfg.progress_interval_files);
        } else if (key == "progress_interval_sec") {
            ok = ParseU64(value, &gen_cfg.progress_interval_sec);
        } else if (key == "seed") {
            ok = ParseU64(value, &file_cfg.seed);
        } else if (key == "object_unit_size") {
            ok = ParseU64(value, &gen_cfg.object_unit_size);
        } else if (key == "now_seconds") {
            ok = ParseU64(value, &gen_cfg.now_seconds);
        } else if (key == "txid_prefix") {
            gen_cfg.txid_prefix = value;
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

    DiskMetaGenStats stats;
    std::string error;
    if (!DiskMetaGenerator::Generate(cluster, dir_cfg, file_cfg, gen_cfg, &stats, &error)) {
        std::cerr << "disk metadata generation failed: " << error << "\n";
        return 2;
    }

    std::ofstream ofs(gen_cfg.output_dir + "/disk_meta_manifest.txt",
                      std::ios::out | std::ios::trunc);
    if (ofs.is_open()) {
        ofs << "total_files=" << stats.total_files << "\n";
        ofs << "files_with_disk_meta=" << stats.files_with_disk_meta << "\n";
        ofs << "real_node_file_meta_records=" << stats.real_node_file_meta_records << "\n";
        ofs << "virtual_node_file_meta_records=" << stats.virtual_node_file_meta_records << "\n";
        ofs << "disk_budget_bytes=" << static_cast<unsigned long long>(stats.disk_budget_bytes) << "\n";
        ofs << "disk_used_bytes=" << static_cast<unsigned long long>(stats.disk_used_bytes) << "\n";
    }

    std::cout << "disk metadata generation done\n";
    std::cout << "files_with_disk_meta=" << stats.files_with_disk_meta << "\n";
    return 0;
}

} // namespace zb::meta_gen

int main(int argc, char** argv) {
    return zb::meta_gen::RunDiskMetaGenTool(argc, argv);
}
