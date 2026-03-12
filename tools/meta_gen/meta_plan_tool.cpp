#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "capacity_planner.h"
#include "config.h"
#include "file_size_sampler.h"
#include "namespace_layout_planner.h"

namespace zb::meta_gen {

namespace {

constexpr long double kBytesPerTb = 1000.0L * 1000.0L * 1000.0L * 1000.0L;
constexpr long double kBytesPerEb = kBytesPerTb * 1000.0L * 1000.0L;

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

std::string Fmt4(long double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(4) << v;
    return oss.str();
}

void PrintUsage(std::ostream& os) {
    os << "Usage: meta_plan_tool [options]\n"
          "Options:\n"
          "  --output=<path>                      Output markdown path (default: stdout)\n"
          "  --total_files=<u64>                  Total files (default: 100000000000)\n"
          "  --namespace_count=<u64>              Namespace count (default: 1000)\n"
          "  --disk_replica_ratio=<double>        Disk occupancy ratio [0,1] (default: 0.5)\n"
          "  --optical_node_count=<u64>           Optical node count (default: 10000)\n"
          "  --discs_per_optical_node=<u64>       Discs per optical node (default: 10000)\n"
          "  --virtual_node_count=<u64>           Virtual node count (default: 100)\n"
          "  --virtual_disks_per_node=<u64>       Virtual disks per node (default: 16)\n"
          "  --real_node_count=<u64>              Real node count (default: 1)\n"
          "  --real_disks_per_node=<u64>          Real disks per node (default: 24)\n"
          "  --target_optical_eb=<u64>            Optical target capacity in EB (default: 1000)\n"
          "  --min_100gb_discs=<u64>              Lower bound for 100GB discs (default: 0)\n"
          "  --min_1tb_discs=<u64>                Lower bound for 1TB discs (default: 0)\n"
          "  --min_10tb_discs=<u64>               Lower bound for 10TB discs (default: 0)\n"
          "  --branch_factor=<u64>                Dir branch factor (default: 64)\n"
          "  --max_depth=<u64>                    Max dir depth (default: 4)\n"
          "  --max_files_per_leaf=<u64>           Max files per leaf dir (default: 2048)\n"
          "  --sample_count=<u64>                 Sample count for avg size estimate (default: 200000)\n"
          "  --seed=<u64>                         File size sampler seed\n"
          "  --help                               Show this help\n";
}

} // namespace

int RunMetaPlanTool(int argc, char** argv) {
    ClusterScaleConfig cluster = DefaultClusterScaleConfig();
    OpticalCapacityPlanConfig optical_cfg = DefaultOpticalCapacityPlanConfig();
    DirectoryLayoutConfig dir_cfg = DefaultDirectoryLayoutConfig();
    FileSizeSamplerConfig fs_cfg = DefaultFileSizeSamplerConfig();

    std::string output_path;
    uint64_t sample_count = cluster.file_size_estimate_samples;

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
            std::cerr << "argument must use --key=value format: " << arg << "\n";
            return 1;
        }
        const std::string key = arg.substr(2, eq - 2);
        const std::string value = arg.substr(eq + 1);

        bool ok = true;
        if (key == "output") {
            output_path = value;
        } else if (key == "total_files") {
            ok = ParseU64(value, &cluster.total_files);
        } else if (key == "namespace_count") {
            uint64_t tmp = 0;
            ok = ParseU64(value, &tmp);
            cluster.namespace_count = static_cast<uint32_t>(tmp);
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
        } else if (key == "target_optical_eb") {
            ok = ParseU64(value, &optical_cfg.target_eb);
        } else if (key == "min_100gb_discs") {
            ok = ParseU64(value, &optical_cfg.min_100gb_discs);
        } else if (key == "min_1tb_discs") {
            ok = ParseU64(value, &optical_cfg.min_1tb_discs);
        } else if (key == "min_10tb_discs") {
            ok = ParseU64(value, &optical_cfg.min_10tb_discs);
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
        } else if (key == "sample_count") {
            ok = ParseU64(value, &sample_count);
        } else if (key == "seed") {
            ok = ParseU64(value, &fs_cfg.seed);
        } else {
            std::cerr << "unknown option: --" << key << "\n";
            return 1;
        }

        if (!ok) {
            std::cerr << "invalid value for --" << key << ": " << value << "\n";
            return 1;
        }
    }

    if (cluster.disk_replica_ratio < 0.0 || cluster.disk_replica_ratio > 1.0) {
        std::cerr << "disk_replica_ratio must be within [0,1]\n";
        return 1;
    }
    if (sample_count == 0) {
        std::cerr << "sample_count must be > 0\n";
        return 1;
    }

    OpticalCapacityPlan optical_plan;
    std::string err;
    if (!CapacityPlanner::BuildOpticalCapacityPlan(cluster, optical_cfg, &optical_plan, &err)) {
        std::cerr << "optical capacity planning failed: " << err << "\n";
        return 2;
    }

    NamespaceLayoutPlan ns_plan;
    if (!NamespaceLayoutPlanner::BuildPlan(cluster, dir_cfg, &ns_plan, &err)) {
        std::cerr << "namespace layout planning failed: " << err << "\n";
        return 3;
    }

    FileSizeSampler sampler(fs_cfg);
    const long double avg_bytes = sampler.EstimateAverage(0, sample_count);
    const long double avg_gb = avg_bytes / (1000.0L * 1000.0L * 1000.0L);
    const long double avg_mib = avg_bytes / (1024.0L * 1024.0L);

    const long double logical_total_bytes =
        static_cast<long double>(cluster.total_files) * avg_bytes;
    const long double logical_total_eb = logical_total_bytes / kBytesPerEb;

    const long double real_disk_tb = static_cast<long double>(cluster.real_node_count) *
                                     static_cast<long double>(cluster.real_disks_per_node) *
                                     static_cast<long double>(cluster.real_disk_size_tb);
    const long double virtual_disk_tb =
        static_cast<long double>(cluster.virtual_node_count) *
        static_cast<long double>(cluster.virtual_disks_per_node) *
        static_cast<long double>(cluster.virtual_disk_size_tb);
    const long double total_disk_tb = real_disk_tb + virtual_disk_tb;
    const long double disk_target_tb = total_disk_tb * cluster.disk_replica_ratio;
    const long double disk_target_bytes = disk_target_tb * kBytesPerTb;
    const uint64_t approx_files_with_disk_replica =
        (avg_bytes <= 0.0L)
            ? 0ULL
            : static_cast<uint64_t>(disk_target_bytes / avg_bytes);
    const long double disk_file_ratio = (cluster.total_files == 0)
                                            ? 0.0L
                                            : static_cast<long double>(approx_files_with_disk_replica) /
                                                  static_cast<long double>(cluster.total_files);

    std::ostringstream report;
    report << "# 元数据压测规划结果\n\n";
    report << "## 输入规模\n\n";
    report << "- 文件总数: " << cluster.total_files << "\n";
    report << "- 命名空间数: " << cluster.namespace_count << "\n";
    report << "- 光盘节点: " << cluster.optical_node_count << "，每节点光盘数: "
           << cluster.discs_per_optical_node << "\n";
    report << "- 虚拟节点: " << cluster.virtual_node_count << "，每节点磁盘数: "
           << cluster.virtual_disks_per_node << "，单盘: " << cluster.virtual_disk_size_tb << "TB\n";
    report << "- 真实节点: " << cluster.real_node_count << "，每节点磁盘数: "
           << cluster.real_disks_per_node << "，单盘: " << cluster.real_disk_size_tb << "TB\n";
    report << "- 磁盘副本占用比例: " << Fmt4(cluster.disk_replica_ratio) << "\n\n";

    report << "## 光盘容量规划（100GB/1TB/10TB）\n\n";
    report << "- 光盘总数: " << optical_plan.total_disc_count << "\n";
    report << "- 100GB 光盘数: " << optical_plan.count_100gb << "\n";
    report << "- 1TB 光盘数: " << optical_plan.count_1tb << "\n";
    report << "- 10TB 光盘数: " << optical_plan.count_10tb << "\n";
    report << "- 目标容量: " << optical_plan.target_eb << " EB\n";
    report << "- 规划容量: " << Fmt2(optical_plan.total_eb) << " EB ("
           << Fmt2(optical_plan.total_tb) << " TB)\n";
    report << "- 容量差值: " << Fmt2(optical_plan.capacity_gap_tb) << " TB\n";
    report << "- 说明: " << optical_plan.note << "\n\n";

    report << "## 目录结构规划\n\n";
    report << "- 分支因子: " << ns_plan.branch_factor << "\n";
    report << "- 树深: " << ns_plan.depth << "\n";
    report << "- 每命名空间文件数: " << ns_plan.files_per_namespace << "\n";
    report << "- 叶子目录数(每命名空间): " << ns_plan.leaf_dir_count << "\n";
    report << "- 每叶子目录文件基数: " << ns_plan.files_per_leaf_base << "\n";
    report << "- 余数叶子目录额外文件数: " << ns_plan.files_per_leaf_remainder << "\n";
    report << "- 目录总数(每命名空间): " << ns_plan.total_dir_count << "\n\n";

    report << "## 文件大小采样估计\n\n";
    report << "- 样本数: " << sample_count << "\n";
    report << "- 平均文件大小: " << Fmt2(avg_gb) << " GB (" << Fmt2(avg_mib) << " MiB)\n";
    report << "- 总逻辑数据量估计: " << Fmt2(logical_total_eb) << " EB\n\n";

    report << "## 磁盘副本预算\n\n";
    report << "- 真实节点磁盘总容量: " << Fmt2(real_disk_tb) << " TB\n";
    report << "- 虚拟节点磁盘总容量: " << Fmt2(virtual_disk_tb) << " TB\n";
    report << "- 磁盘总容量: " << Fmt2(total_disk_tb) << " TB\n";
    report << "- 磁盘副本目标容量: " << Fmt2(disk_target_tb) << " TB\n";
    report << "- 可落盘文件数估计: " << approx_files_with_disk_replica << "\n";
    report << "- 文件层面落盘比例估计: " << Fmt4(disk_file_ratio) << "\n\n";

    report << "## 结论\n\n";
    report << "- 全量光盘副本容量是否足够: "
           << ((optical_plan.total_eb >= logical_total_eb) ? "是" : "否") << "\n";
    report << "- 磁盘副本策略建议: 按容量预算筛选热点文件落盘，其余仅保留光盘副本。\n";

    if (!output_path.empty()) {
        std::ofstream ofs(output_path, std::ios::out | std::ios::trunc);
        if (!ofs.is_open()) {
            std::cerr << "failed to open output file: " << output_path << "\n";
            return 4;
        }
        ofs << report.str();
    } else {
        std::cout << report.str();
    }
    return 0;
}

} // namespace zb::meta_gen

int main(int argc, char** argv) {
    return zb::meta_gen::RunMetaPlanTool(argc, argv);
}
