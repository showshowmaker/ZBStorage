#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>

#include "config.h"
#include "optical_meta_generator.h"

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

void PrintUsage(std::ostream& os) {
    os << "Usage: optical_meta_gen_tool --output_dir=<path> [options]\n"
          "Options:\n"
          "  --output_dir=<path>                  Required: output root\n"
          "  --total_files=<u64>                  Total files\n"
          "  --namespace_count=<u64>              Namespace count\n"
          "  --optical_node_count=<u64>           Optical node count\n"
          "  --discs_per_optical_node=<u64>       Discs per optical node\n"
          "  --target_optical_eb=<u64>            Optical target capacity EB\n"
          "  --min_100gb_discs=<u64>              Lower bound for 100GB discs\n"
          "  --min_1tb_discs=<u64>                Lower bound for 1TB discs\n"
          "  --min_10tb_discs=<u64>               Lower bound for 10TB discs\n"
          "  --branch_factor=<u64>                Dir branch factor\n"
          "  --max_depth=<u64>                    Dir max depth\n"
          "  --max_files_per_leaf=<u64>           Max files per leaf\n"
          "  --progress_interval_files=<u64>      Progress log interval by file count (default: 1000000)\n"
          "  --progress_interval_sec=<u64>        Progress log interval by seconds (default: 30)\n"
          "  --seed=<u64>                         Size sampler seed\n"
          "  --now_seconds=<u64>                  Timestamp override\n";
}

} // namespace

int RunOpticalMetaGenTool(int argc, char** argv) {
    ClusterScaleConfig cluster = DefaultClusterScaleConfig();
    DirectoryLayoutConfig dir_cfg = DefaultDirectoryLayoutConfig();
    FileSizeSamplerConfig file_cfg = DefaultFileSizeSamplerConfig();
    OpticalMetaGenConfig gen_cfg;

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
        } else if (key == "optical_node_count") {
            ok = ParseU64(value, &cluster.optical_node_count);
        } else if (key == "discs_per_optical_node") {
            ok = ParseU64(value, &cluster.discs_per_optical_node);
        } else if (key == "target_optical_eb") {
            ok = ParseU64(value, &gen_cfg.target_optical_eb);
        } else if (key == "min_100gb_discs") {
            ok = ParseU64(value, &gen_cfg.min_100gb_discs);
        } else if (key == "min_1tb_discs") {
            ok = ParseU64(value, &gen_cfg.min_1tb_discs);
        } else if (key == "min_10tb_discs") {
            ok = ParseU64(value, &gen_cfg.min_10tb_discs);
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

    OpticalMetaGenStats stats;
    std::string error;
    if (!OpticalMetaGenerator::Generate(cluster, dir_cfg, file_cfg, gen_cfg, &stats, &error)) {
        std::cerr << "optical metadata generation failed: " << error << "\n";
        return 2;
    }

    std::ofstream ofs(gen_cfg.output_dir + "/optical_meta_manifest.txt",
                      std::ios::out | std::ios::trunc);
    if (ofs.is_open()) {
        ofs << "total_files=" << stats.total_files << "\n";
        ofs << "manifest_records=" << stats.manifest_records << "\n";
        ofs << "total_discs=" << stats.total_discs << "\n";
        ofs << "used_discs=" << stats.used_discs << "\n";
        ofs << "count_100gb=" << stats.count_100gb << "\n";
        ofs << "count_1tb=" << stats.count_1tb << "\n";
        ofs << "count_10tb=" << stats.count_10tb << "\n";
    }

    std::cout << "optical metadata generation done\n";
    std::cout << "manifest_records=" << stats.manifest_records << "\n";
    return 0;
}

} // namespace zb::meta_gen

int main(int argc, char** argv) {
    return zb::meta_gen::RunOpticalMetaGenTool(argc, argv);
}
