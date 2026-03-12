#include "workload_enumerator.h"

#include <sstream>
#include <string>

#include "file_size_sampler.h"
#include "namespace_layout_planner.h"

namespace zb::meta_gen {

namespace {

std::string FormatNamespaceName(uint32_t ns) {
    std::ostringstream oss;
    oss << "ns_" << ns;
    return oss.str();
}

std::string FormatDirName(uint32_t level, uint32_t slot) {
    std::ostringstream oss;
    oss << "d" << level << "_" << slot;
    return oss.str();
}

std::string FormatFileName(uint64_t leaf_index, uint64_t idx) {
    std::ostringstream oss;
    oss << "f_" << leaf_index << "_" << idx;
    return oss.str();
}

std::string BuildFileId(uint64_t inode_id) {
    return "inode-" + std::to_string(inode_id);
}

struct Runtime {
    const NamespaceLayoutPlan* ns_plan{nullptr};
    FileSizeSampler* sampler{nullptr};
    WorkloadEnumerator::FileCallback callback;
    std::string* error{nullptr};

    uint64_t next_inode{2};
    uint64_t global_file_ordinal{0};
};

bool EnumerateRec(Runtime* rt,
                  uint32_t namespace_id,
                  const std::string& parent_path,
                  uint32_t level,
                  uint64_t* leaf_index,
                  uint64_t* ns_file_ordinal) {
    if (!rt || !leaf_index || !ns_file_ordinal) {
        return false;
    }
    if (level == rt->ns_plan->depth) {
        const uint64_t cur_leaf = (*leaf_index)++;
        uint64_t file_count = rt->ns_plan->files_per_leaf_base;
        if (cur_leaf < rt->ns_plan->files_per_leaf_remainder) {
            ++file_count;
        }
        for (uint64_t i = 0; i < file_count; ++i) {
            FileWorkItem item;
            item.namespace_id = namespace_id;
            item.inode_id = rt->next_inode++;
            item.file_ordinal_in_namespace = (*ns_file_ordinal)++;
            item.global_file_ordinal = rt->global_file_ordinal++;
            item.file_size = rt->sampler->Sample(namespace_id, item.file_ordinal_in_namespace);
            item.file_id = BuildFileId(item.inode_id);
            item.file_path = parent_path + "/" + FormatFileName(cur_leaf, i);
            if (!rt->callback(item)) {
                if (rt->error && rt->error->empty()) {
                    *rt->error = "file callback aborted enumeration";
                }
                return false;
            }
        }
        return true;
    }

    for (uint32_t slot = 0; slot < rt->ns_plan->branch_factor; ++slot) {
        rt->next_inode++; // directory inode
        const std::string child_path = parent_path + "/" + FormatDirName(level, slot);
        if (!EnumerateRec(rt, namespace_id, child_path, level + 1, leaf_index, ns_file_ordinal)) {
            return false;
        }
    }
    return true;
}

} // namespace

bool WorkloadEnumerator::EnumerateFiles(const ClusterScaleConfig& cluster,
                                        const DirectoryLayoutConfig& dir_cfg,
                                        const FileSizeSamplerConfig& file_cfg,
                                        const FileCallback& on_file,
                                        std::string* error) {
    if (!on_file) {
        if (error) {
            *error = "file callback is empty";
        }
        return false;
    }

    NamespaceLayoutPlan ns_plan;
    if (!NamespaceLayoutPlanner::BuildPlan(cluster, dir_cfg, &ns_plan, error)) {
        return false;
    }

    FileSizeSampler sampler(file_cfg);
    Runtime rt;
    rt.ns_plan = &ns_plan;
    rt.sampler = &sampler;
    rt.callback = on_file;
    rt.error = error;
    rt.next_inode = 2;
    rt.global_file_ordinal = 0;

    for (uint32_t ns = 0; ns < cluster.namespace_count; ++ns) {
        rt.next_inode++; // namespace directory inode
        uint64_t leaf_index = 0;
        uint64_t ns_file_ordinal = 0;
        const std::string ns_root = "/" + FormatNamespaceName(ns);
        if (!EnumerateRec(&rt, ns, ns_root, 0, &leaf_index, &ns_file_ordinal)) {
            return false;
        }
    }

    return true;
}

} // namespace zb::meta_gen
