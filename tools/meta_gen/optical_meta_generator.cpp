#include "optical_meta_generator.h"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "capacity_planner.h"
#include "io_utils.h"
#include "placement.h"
#include "workload_enumerator.h"

namespace zb::meta_gen {

namespace {

uint64_t NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

void MaybeLogProgress(const char* stage,
                      uint64_t done_files,
                      uint64_t total_files,
                      uint64_t manifest_records,
                      uint64_t progress_interval_files,
                      uint64_t progress_interval_sec,
                      uint64_t* next_progress_files,
                      std::chrono::steady_clock::time_point start_tp,
                      std::chrono::steady_clock::time_point* last_progress_tp,
                      bool force = false) {
    if (!stage || !next_progress_files || !last_progress_tp) {
        return;
    }
    if (done_files == 0) {
        return;
    }
    const auto now_tp = std::chrono::steady_clock::now();
    bool should_log = force;
    if (!should_log && progress_interval_files > 0 && done_files >= *next_progress_files) {
        should_log = true;
    }
    if (!should_log && progress_interval_sec > 0) {
        const auto elapsed_since_last =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now_tp - *last_progress_tp).count());
        if (elapsed_since_last >= progress_interval_sec) {
            should_log = true;
        }
    }
    if (!should_log) {
        return;
    }
    const uint64_t elapsed_sec =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now_tp - start_tp).count());
    const uint64_t safe_elapsed_sec = elapsed_sec == 0 ? 1 : elapsed_sec;
    const long double pct = total_files == 0
                                ? 0.0L
                                : (100.0L * static_cast<long double>(done_files) /
                                   static_cast<long double>(total_files));
    const uint64_t files_per_sec = done_files / safe_elapsed_sec;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3) << pct;
    std::cout << "[progress][" << stage << "] files=" << done_files << "/" << total_files
              << " (" << oss.str() << "%)"
              << " manifest_records=" << manifest_records
              << " elapsed_sec=" << elapsed_sec
              << " throughput_files_per_sec=" << files_per_sec
              << std::endl;

    *last_progress_tp = now_tp;
    if (progress_interval_files > 0) {
        while (*next_progress_files <= done_files) {
            *next_progress_files += progress_interval_files;
        }
    }
}

inline void MarkBit(std::vector<uint64_t>* bits, uint64_t index, bool* was_set) {
    if (!bits || index / 64ULL >= bits->size()) {
        if (was_set) {
            *was_set = false;
        }
        return;
    }
    const uint64_t word_idx = index / 64ULL;
    const uint64_t bit = 1ULL << (index % 64ULL);
    const bool already = (((*bits)[word_idx] & bit) != 0ULL);
    (*bits)[word_idx] |= bit;
    if (was_set) {
        *was_set = already;
    }
}

inline bool TestBit(const std::vector<uint64_t>& bits, uint64_t index) {
    if (index / 64ULL >= bits.size()) {
        return false;
    }
    return ((bits[index / 64ULL] >> (index % 64ULL)) & 1ULL) != 0ULL;
}

uint64_t PermuteIndex(uint64_t i, uint64_t n) {
    if (n == 0) {
        return 0;
    }
#if defined(__SIZEOF_INT128__)
    constexpr uint64_t kA = 2862933555777941757ULL; // odd, not divisible by 5
    constexpr uint64_t kB = 3037000493ULL;
    __uint128_t v = static_cast<__uint128_t>(kA) * static_cast<__uint128_t>(i) + kB;
    return static_cast<uint64_t>(v % n);
#else
    constexpr uint64_t kA = 1140071487ULL;
    constexpr uint64_t kB = 2654435761ULL;
    return static_cast<uint64_t>((kA * (i % n) + kB) % n);
#endif
}

uint64_t DiscCapacityBytes(uint64_t global_disc_idx,
                           uint64_t total_discs,
                           const OpticalCapacityPlan& plan) {
    const uint64_t p = PermuteIndex(global_disc_idx, total_discs);
    if (p < plan.count_10tb) {
        return 10ULL * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
    }
    if (p < plan.count_10tb + plan.count_1tb) {
        return 1ULL * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
    }
    return 100ULL * 1000ULL * 1000ULL * 1000ULL;
}

} // namespace

bool OpticalMetaGenerator::Generate(const ClusterScaleConfig& cluster,
                                    const DirectoryLayoutConfig& dir_cfg,
                                    const FileSizeSamplerConfig& file_cfg,
                                    const OpticalMetaGenConfig& gen_cfg,
                                    OpticalMetaGenStats* out,
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
    if (cluster.optical_node_count == 0 || cluster.discs_per_optical_node == 0) {
        if (error) {
            *error = "optical topology is empty";
        }
        return false;
    }

    const std::string root = NormalizePath(gen_cfg.output_dir);
    if (!EnsureDirRecursive(root, error)) {
        return false;
    }
    const std::string manifest_path = JoinPath(root, "optical_manifest.tsv");
    const std::string catalog_path = JoinPath(root, "optical_disc_catalog.tsv");

    OpticalCapacityPlanConfig cap_cfg;
    cap_cfg.target_eb = gen_cfg.target_optical_eb;
    cap_cfg.min_100gb_discs = gen_cfg.min_100gb_discs;
    cap_cfg.min_1tb_discs = gen_cfg.min_1tb_discs;
    cap_cfg.min_10tb_discs = gen_cfg.min_10tb_discs;

    OpticalCapacityPlan cap_plan;
    std::string cap_error;
    if (!CapacityPlanner::BuildOpticalCapacityPlan(cluster, cap_cfg, &cap_plan, &cap_error)) {
        cap_plan = OpticalCapacityPlan{};
        cap_plan.total_disc_count = cluster.optical_node_count * cluster.discs_per_optical_node;
        cap_plan.count_10tb = cap_plan.total_disc_count;
        cap_plan.count_1tb = 0;
        cap_plan.count_100gb = 0;
        cap_plan.note = "fallback: strict exact solver failed, use all 10TB discs";
    }

    const uint64_t total_discs = cluster.optical_node_count * cluster.discs_per_optical_node;
    std::vector<uint64_t> used_bits(static_cast<size_t>((total_discs + 63ULL) / 64ULL), 0ULL);
    uint64_t used_disc_count = 0;

    const uint64_t now_sec = gen_cfg.now_seconds > 0 ? gen_cfg.now_seconds : NowSeconds();
    std::ofstream manifest(manifest_path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!manifest.is_open()) {
        if (error) {
            *error = "failed to open optical manifest: " + manifest_path;
        }
        return false;
    }

    OpticalMetaGenStats stats;
    stats.total_discs = total_discs;
    stats.count_100gb = cap_plan.count_100gb;
    stats.count_1tb = cap_plan.count_1tb;
    stats.count_10tb = cap_plan.count_10tb;
    const uint64_t progress_interval_files = gen_cfg.progress_interval_files;
    const uint64_t progress_interval_sec = gen_cfg.progress_interval_sec;
    uint64_t next_progress_files = progress_interval_files > 0 ? progress_interval_files : UINT64_MAX;
    const auto progress_start_tp = std::chrono::steady_clock::now();
    auto progress_last_tp = progress_start_tp;

    bool ok = WorkloadEnumerator::EnumerateFiles(
        cluster,
        dir_cfg,
        file_cfg,
        [&](const FileWorkItem& item) -> bool {
            ++stats.total_files;
            MaybeLogProgress("optical_meta_generation",
                             stats.total_files,
                             cluster.total_files,
                             stats.manifest_records,
                             progress_interval_files,
                             progress_interval_sec,
                             &next_progress_files,
                             progress_start_tp,
                             &progress_last_tp,
                             false);
            const OpticalPlacement p = PickOpticalPlacement(cluster, item.inode_id);
            if (!p.valid) {
                return true;
            }
            const uint64_t global_disc_idx = p.node_index * cluster.discs_per_optical_node + p.disc_index;
            bool was_set = false;
            MarkBit(&used_bits, global_disc_idx, &was_set);
            if (!was_set) {
                ++used_disc_count;
            }

            const std::string object_id = BuildStableObjectId(item.inode_id, 0);
            const std::string image_id = "img-" + p.disc_id;
            manifest << "W|"
                     << object_id << "|"
                     << image_id << "|"
                     << 0 << "|"
                     << item.file_size << "|"
                     << p.disc_id << "|"
                     << item.inode_id << "|"
                     << item.file_id << "|"
                     << item.file_path << "|"
                     << 0 << "|"
                     << item.file_size << "|"
                     << now_sec << "|"
                     << 420 << "|"
                     << 0 << "|"
                     << 0 << "|"
                     << 0
                     << "\n";
            ++stats.manifest_records;
            return manifest.good();
        },
        error);
    if (!ok) {
        return false;
    }

    manifest.flush();
    if (!manifest.good()) {
        if (error) {
            *error = "failed to flush optical manifest";
        }
        return false;
    }

    std::ofstream catalog(catalog_path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!catalog.is_open()) {
        if (error) {
            *error = "failed to open optical disc catalog: " + catalog_path;
        }
        return false;
    }
    catalog << "node_id\tdisc_id\tcapacity_bytes\tstate\n";
    for (uint64_t node_idx = 0; node_idx < cluster.optical_node_count; ++node_idx) {
        const std::string node_id = "optical-" + std::to_string(node_idx + 1);
        for (uint64_t disc_idx = 0; disc_idx < cluster.discs_per_optical_node; ++disc_idx) {
            const uint64_t global_disc_idx = node_idx * cluster.discs_per_optical_node + disc_idx;
            const std::string disc_id = "odisk" + std::to_string(disc_idx);
            const uint64_t cap = DiscCapacityBytes(global_disc_idx, total_discs, cap_plan);
            const bool used = TestBit(used_bits, global_disc_idx);
            catalog << node_id << '\t'
                    << disc_id << '\t'
                    << cap << '\t'
                    << (used ? "used" : "empty")
                    << '\n';
        }
    }
    catalog.flush();
    if (!catalog.good()) {
        if (error) {
            *error = "failed to flush optical disc catalog";
        }
        return false;
    }

    stats.used_discs = used_disc_count;
    MaybeLogProgress("optical_meta_generation",
                     stats.total_files,
                     cluster.total_files,
                     stats.manifest_records,
                     progress_interval_files,
                     progress_interval_sec,
                     &next_progress_files,
                     progress_start_tp,
                     &progress_last_tp,
                     true);
    *out = std::move(stats);
    return true;
}

} // namespace zb::meta_gen
