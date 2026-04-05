#include "mds_sst_generator.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <cerrno>

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_writer.h>

#include "file_size_sampler.h"
#include "mds/storage/UnifiedInodeRecord.h"
#include "mds.pb.h"
#include "namespace_layout_planner.h"

namespace zb::meta_gen {

namespace {

struct KvPair {
    std::string key;
    std::string value;
};

class SstShardWriter {
public:
    SstShardWriter(std::string output_dir, std::string prefix, uint64_t max_kv_per_sst)
        : output_dir_(std::move(output_dir)),
          prefix_(std::move(prefix)),
          max_kv_per_sst_(std::max<uint64_t>(1ULL, max_kv_per_sst)) {}

    bool Add(std::string key, std::string value, std::string* error) {
        KvPair kv;
        kv.key = std::move(key);
        kv.value = std::move(value);
        buffer_.push_back(std::move(kv));
        if (buffer_.size() >= max_kv_per_sst_) {
            return Flush(error);
        }
        return true;
    }

    bool Finish(std::string* error) {
        if (!buffer_.empty() && !Flush(error)) {
            return false;
        }
        return true;
    }

    const std::vector<std::string>& files() const {
        return files_;
    }

    uint64_t shard_count() const {
        return shard_index_;
    }

private:
    bool Flush(std::string* error) {
        std::sort(buffer_.begin(), buffer_.end(), [](const KvPair& a, const KvPair& b) {
            return a.key < b.key;
        });

        std::ostringstream oss;
        oss << prefix_ << "_" << shard_index_ << ".sst";
        const std::string file_path = output_dir_ + "/" + oss.str();

        rocksdb::Options options;
        options.comparator = rocksdb::BytewiseComparator();
        options.create_if_missing = true;
        rocksdb::EnvOptions env_options;
        rocksdb::SstFileWriter writer(env_options, options);
        rocksdb::Status st = writer.Open(file_path);
        if (!st.ok()) {
            if (error) {
                *error = "failed to open sst: " + file_path + ", " + st.ToString();
            }
            return false;
        }

        for (size_t i = 0; i < buffer_.size();) {
            size_t j = i;
            while (j + 1 < buffer_.size() && buffer_[j + 1].key == buffer_[i].key) {
                ++j;
            }
            st = writer.Put(buffer_[j].key, buffer_[j].value);
            if (!st.ok()) {
                if (error) {
                    *error = "failed to put key into sst: " + st.ToString();
                }
                return false;
            }
            i = j + 1;
        }

        st = writer.Finish();
        if (!st.ok()) {
            if (error) {
                *error = "failed to finish sst: " + st.ToString();
            }
            return false;
        }

        files_.push_back(file_path);
        ++shard_index_;
        buffer_.clear();
        return true;
    }

    std::string output_dir_;
    std::string prefix_;
    uint64_t max_kv_per_sst_;
    uint64_t shard_index_{0};
    std::vector<KvPair> buffer_;
    std::vector<std::string> files_;
};

std::string InodeKey(uint64_t inode_id) {
    return "I/" + std::to_string(inode_id);
}

std::string DentryKey(uint64_t parent_inode, const std::string& name) {
    return "D/" + std::to_string(parent_inode) + "/" + name;
}

std::string NextInodeKey() {
    return "X/next_inode";
}

std::string NextHandleKey() {
    return "X/next_handle";
}

std::string EncodeU64(uint64_t value) {
    std::string out(sizeof(uint64_t), '\0');
    std::memcpy(out.data(), &value, sizeof(uint64_t));
    return out;
}

uint64_t ComputeBlocks512(uint64_t size_bytes) {
    return size_bytes == 0 ? 0 : ((size_bytes + 511ULL) / 512ULL);
}

void FillPosixInodeDefaults(zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    if (attr->type() == zb::rpc::INODE_DIR && attr->size() == 0) {
        attr->set_size(4096);
    }
    if (attr->blksize() == 0) {
        attr->set_blksize(4096);
    }
    attr->set_blocks_512(ComputeBlocks512(attr->size()));
    attr->set_rdev_major(0);
    attr->set_rdev_minor(0);
}

bool EncodeUnifiedInode(const zb::rpc::InodeAttr& attr,
                        uint64_t parent_inode_id,
                        zb::mds::UnifiedStorageTier storage_tier,
                        uint64_t disk_node_id,
                        uint32_t disk_id,
                        uint64_t optical_node_id,
                        uint64_t optical_disk_id,
                        uint32_t optical_image_id,
                        std::string* out,
                        std::string* error) {
    zb::mds::UnifiedInodeRecord record;
    if (!zb::mds::AttrToUnifiedInodeRecord(attr, parent_inode_id, storage_tier, &record, error)) {
        return false;
    }
    record.disk_node_id = disk_node_id;
    record.disk_id = disk_id;
    record.optical_node_id = optical_node_id;
    record.optical_disk_id = optical_disk_id;
    record.optical_image_id = optical_image_id;
    return zb::mds::EncodeUnifiedInodeRecord(record, out, error);
}

uint64_t NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

std::string FormatNodeId(bool is_real_node, uint64_t index) {
    std::ostringstream oss;
    if (is_real_node) {
        oss << "real-" << (index + 1);
    } else {
        oss << "virtual-" << (index + 1);
    }
    return oss.str();
}

uint64_t SelectDiskNodeNumericId(bool is_real_node, uint64_t index) {
    if (is_real_node) {
        return index + 1U;
    }
    return index;
}

std::string FormatDiskId(uint64_t disk_index) {
    return "disk" + std::to_string(disk_index);
}

std::string FormatOpticalNodeId(uint64_t index) {
    return "optical-" + std::to_string(index + 1);
}

std::string FormatDiscId(uint64_t disc_index) {
    return "odisk" + std::to_string(disc_index);
}

std::string FormatDirName(uint32_t level, uint32_t slot) {
    std::ostringstream oss;
    oss << "d" << level << "_" << slot;
    return oss.str();
}

std::string FormatNamespaceName(uint32_t ns) {
    std::ostringstream oss;
    oss << "ns_" << ns;
    return oss.str();
}

std::string FormatFileName(uint64_t leaf_index, uint64_t idx) {
    std::ostringstream oss;
    oss << "f_" << leaf_index << "_" << idx;
    return oss.str();
}

std::string BuildObjectId(uint64_t inode_id) {
    return "obj-" + std::to_string(inode_id) + "-0";
}

bool MakeDirSingle(const std::string& path) {
    if (path.empty()) {
        return true;
    }
#ifdef _WIN32
    const int rc = _mkdir(path.c_str());
#else
    const int rc = mkdir(path.c_str(), 0755);
#endif
    if (rc == 0 || errno == EEXIST) {
        return true;
    }
    return false;
}

std::string NormalizePath(std::string path) {
    for (char& c : path) {
        if (c == '\\') {
            c = '/';
        }
    }
    while (path.size() > 1 && path.back() == '/') {
        path.pop_back();
    }
    return path;
}

bool EnsureDirRecursive(const std::string& raw_path, std::string* error) {
    const std::string path = NormalizePath(raw_path);
    if (path.empty()) {
        if (error) {
            *error = "empty directory path";
        }
        return false;
    }

    std::string current;
    size_t i = 0;
    if (path.size() >= 2 && path[1] == ':') { // Windows drive
        current = path.substr(0, 2);
        i = 2;
    } else if (path[0] == '/') {
        current = "/";
        i = 1;
    }

    while (i < path.size()) {
        while (i < path.size() && path[i] == '/') {
            ++i;
        }
        size_t j = i;
        while (j < path.size() && path[j] != '/') {
            ++j;
        }
        if (j == i) {
            break;
        }
        const std::string part = path.substr(i, j - i);
        if (!current.empty() && current.back() != '/') {
            current.push_back('/');
        }
        current += part;
        if (!MakeDirSingle(current)) {
            if (error) {
                *error = "failed to create directory: " + current + ", errno=" + std::to_string(errno);
            }
            return false;
        }
        i = j;
    }
    return true;
}

struct GeneratorRuntime {
    const ClusterScaleConfig* cluster{nullptr};
    const NamespaceLayoutPlan* ns_plan{nullptr};
    const MdsSstGenConfig* cfg{nullptr};
    FileSizeSampler* sampler{nullptr};
    SstShardWriter* writer{nullptr};
    MdsSstGenStats* stats{nullptr};
    std::string* error{nullptr};

    uint64_t next_inode{2};
    long double disk_budget_bytes{0.0L};
    long double disk_remaining_bytes{0.0L};
    uint64_t files_emitted{0};
    uint64_t next_progress_files{0};
    uint64_t progress_interval_files{1000000};
    uint64_t progress_interval_sec{30};
    std::chrono::steady_clock::time_point start_tp{};
    std::chrono::steady_clock::time_point last_progress_tp{};
};

bool ShouldLogProgress(const GeneratorRuntime* rt,
                       const std::chrono::steady_clock::time_point& now_tp) {
    if (!rt) {
        return false;
    }
    if (rt->files_emitted == 0) {
        return false;
    }
    if (rt->progress_interval_files > 0 && rt->files_emitted >= rt->next_progress_files) {
        return true;
    }
    if (rt->progress_interval_sec > 0) {
        const auto elapsed_sec =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now_tp - rt->last_progress_tp).count());
        if (elapsed_sec >= rt->progress_interval_sec) {
            return true;
        }
    }
    return false;
}

void MaybeLogProgress(GeneratorRuntime* rt, bool force = false) {
    if (!rt || !rt->stats || !rt->cluster || !rt->writer) {
        return;
    }
    const auto now_tp = std::chrono::steady_clock::now();
    if (!force && !ShouldLogProgress(rt, now_tp)) {
        return;
    }
    const uint64_t elapsed_sec =
        static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now_tp - rt->start_tp).count());
    const uint64_t safe_elapsed_sec = elapsed_sec == 0 ? 1 : elapsed_sec;
    const uint64_t total_files = rt->cluster->total_files;
    const long double pct = total_files == 0
                                ? 0.0L
                                : (100.0L * static_cast<long double>(rt->files_emitted) /
                                   static_cast<long double>(total_files));
    const uint64_t files_per_sec = rt->files_emitted / safe_elapsed_sec;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3) << pct;
    std::cout << "[progress][mds_sst_generation] files=" << rt->files_emitted
              << "/" << total_files
              << " (" << oss.str() << "%)"
              << " kv=" << rt->stats->kv_count
              << " sst=" << rt->writer->shard_count()
              << " elapsed_sec=" << elapsed_sec
              << " throughput_files_per_sec=" << files_per_sec
              << std::endl;
    rt->last_progress_tp = now_tp;
    if (rt->progress_interval_files > 0) {
        while (rt->next_progress_files <= rt->files_emitted) {
            rt->next_progress_files += rt->progress_interval_files;
        }
    }
}

bool EmitKv(GeneratorRuntime* rt, std::string key, std::string value) {
    if (!rt || !rt->writer || !rt->stats) {
        return false;
    }
    if (!rt->writer->Add(std::move(key), std::move(value), rt->error)) {
        return false;
    }
    ++rt->stats->kv_count;
    return true;
}

uint64_t AllocInode(GeneratorRuntime* rt) {
    return rt->next_inode++;
}

bool EmitDirInode(GeneratorRuntime* rt,
                  uint64_t inode_id,
                  uint64_t parent_inode_id,
                  const std::string& file_name,
                  uint64_t now_sec) {
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_DIR);
    attr.set_mode(0755);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(0);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(2);
    attr.set_object_unit_size(rt->cfg->object_unit_size);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    attr.set_file_name(file_name);
    FillPosixInodeDefaults(&attr);
    std::string payload;
    if (!EncodeUnifiedInode(attr,
                            parent_inode_id,
                            zb::mds::UnifiedStorageTier::kNone,
                            0,
                            0,
                            0,
                            0,
                            0,
                            &payload,
                            rt->error)) {
        if (rt->error) {
            *rt->error = rt->error->empty() ? "failed to encode dir inode" : *rt->error;
        }
        return false;
    }
    if (!EmitKv(rt, InodeKey(inode_id), std::move(payload))) {
        return false;
    }
    ++rt->stats->inode_count;
    return true;
}

void SelectDiskReplica(uint64_t inode_id,
                       const ClusterScaleConfig& cluster,
                       std::string* node_id,
                       std::string* disk_id) {
    if (!node_id || !disk_id) {
        return;
    }
    const uint64_t real_disk_total = cluster.real_node_count * cluster.real_disks_per_node;
    const uint64_t virtual_disk_total = cluster.virtual_node_count * cluster.virtual_disks_per_node;
    const uint64_t total_disk = real_disk_total + virtual_disk_total;
    if (total_disk == 0) {
        node_id->clear();
        disk_id->clear();
        return;
    }
    uint64_t slot = inode_id % total_disk;
    if (slot < real_disk_total && cluster.real_node_count > 0 && cluster.real_disks_per_node > 0) {
        const uint64_t node_idx = slot / cluster.real_disks_per_node;
        const uint64_t disk_idx = slot % cluster.real_disks_per_node;
        *node_id = FormatNodeId(true, node_idx);
        *disk_id = FormatDiskId(disk_idx);
        return;
    }
    slot -= real_disk_total;
    if (cluster.virtual_node_count == 0 || cluster.virtual_disks_per_node == 0) {
        node_id->clear();
        disk_id->clear();
        return;
    }
    const uint64_t node_idx = slot / cluster.virtual_disks_per_node;
    const uint64_t disk_idx = slot % cluster.virtual_disks_per_node;
    *node_id = FormatNodeId(false, node_idx);
    *disk_id = FormatDiskId(disk_idx);
}

void SelectOpticalReplica(uint64_t inode_id,
                          const ClusterScaleConfig& cluster,
                          std::string* node_id,
                          std::string* disc_id) {
    if (!node_id || !disc_id) {
        return;
    }
    if (cluster.optical_node_count == 0 || cluster.discs_per_optical_node == 0) {
        node_id->clear();
        disc_id->clear();
        return;
    }
    const uint64_t node_idx = inode_id % cluster.optical_node_count;
    const uint64_t disc_idx = (inode_id / std::max<uint64_t>(1ULL, cluster.optical_node_count)) %
                              cluster.discs_per_optical_node;
    *node_id = FormatOpticalNodeId(node_idx);
    *disc_id = FormatDiscId(disc_idx);
}

bool EmitFileMeta(GeneratorRuntime* rt,
                  uint64_t parent_inode,
                  const std::string& file_name,
                  uint64_t inode_id,
                  uint64_t file_size,
                  uint64_t now_sec) {
    if (!EmitKv(rt, DentryKey(parent_inode, file_name), EncodeU64(inode_id))) {
        return false;
    }
    ++rt->stats->dentry_count;

    const bool has_disk_anchor = (rt->disk_remaining_bytes >= static_cast<long double>(file_size));
    ++rt->stats->files_with_optical_anchor;
    if (has_disk_anchor) {
        rt->disk_remaining_bytes -= static_cast<long double>(file_size);
        ++rt->stats->files_with_disk_anchor;
    }

    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_FILE);
    attr.set_mode(0644);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(file_size);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(1);
    attr.set_object_unit_size(rt->cfg->object_unit_size);
    attr.set_replica(has_disk_anchor ? 2 : 1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_ARCHIVED);
    attr.set_file_name(file_name);
    FillPosixInodeDefaults(&attr);
    std::string inode_payload;
    uint64_t disk_node_numeric_id = 0;
    uint32_t disk_numeric_id = 0;
    uint64_t optical_node_numeric_id = 0;
    uint64_t optical_disk_numeric_id = 0;
    uint32_t optical_image_numeric_id = 0;
    if (has_disk_anchor) {
        const uint64_t real_disk_total = rt->cluster->real_node_count * rt->cluster->real_disks_per_node;
        const uint64_t virtual_disk_total = rt->cluster->virtual_node_count * rt->cluster->virtual_disks_per_node;
        const uint64_t total_disk = real_disk_total + virtual_disk_total;
        if (total_disk != 0) {
            uint64_t slot = inode_id % total_disk;
            if (slot < real_disk_total && rt->cluster->real_node_count > 0 && rt->cluster->real_disks_per_node > 0) {
                const uint64_t node_idx = slot / rt->cluster->real_disks_per_node;
                const uint64_t disk_idx = slot % rt->cluster->real_disks_per_node;
                disk_node_numeric_id = SelectDiskNodeNumericId(true, node_idx);
                disk_numeric_id = static_cast<uint32_t>(disk_idx);
            } else {
                slot -= real_disk_total;
                if (rt->cluster->virtual_node_count > 0 && rt->cluster->virtual_disks_per_node > 0) {
                    const uint64_t node_idx = slot / rt->cluster->virtual_disks_per_node;
                    const uint64_t disk_idx = slot % rt->cluster->virtual_disks_per_node;
                    disk_node_numeric_id = SelectDiskNodeNumericId(false, node_idx);
                    disk_numeric_id = static_cast<uint32_t>(disk_idx);
                }
            }
        }
    } else if (rt->cluster->optical_node_count > 0 && rt->cluster->discs_per_optical_node > 0) {
        optical_node_numeric_id = inode_id % rt->cluster->optical_node_count;
        optical_disk_numeric_id =
            (inode_id / std::max<uint64_t>(1ULL, rt->cluster->optical_node_count)) % rt->cluster->discs_per_optical_node;
        optical_image_numeric_id = 0;
    }
    if (!EncodeUnifiedInode(attr,
                            parent_inode,
                            has_disk_anchor ? zb::mds::UnifiedStorageTier::kDisk : zb::mds::UnifiedStorageTier::kOptical,
                            disk_node_numeric_id,
                            disk_numeric_id,
                            optical_node_numeric_id,
                            optical_disk_numeric_id,
                            optical_image_numeric_id,
                            &inode_payload,
                            rt->error)) {
        if (rt->error) {
            *rt->error = rt->error->empty() ? "failed to encode file inode" : *rt->error;
        }
        return false;
    }
    if (!EmitKv(rt, InodeKey(inode_id), std::move(inode_payload))) {
        return false;
    }
    ++rt->stats->inode_count;

    ++rt->stats->anchor_count;

    if (has_disk_anchor) {
        ++rt->stats->anchor_count;
    }

    rt->stats->sampled_total_bytes += static_cast<long double>(file_size);
    ++rt->files_emitted;
    MaybeLogProgress(rt, false);
    return true;
}

bool GenerateTreeRec(GeneratorRuntime* rt,
                     uint32_t namespace_id,
                     uint64_t parent_inode,
                     uint32_t level,
                     uint64_t* leaf_index,
                     uint64_t* file_ordinal,
                     uint64_t now_sec) {
    if (!rt || !leaf_index || !file_ordinal) {
        return false;
    }

    if (level == rt->ns_plan->depth) {
        const uint64_t cur_leaf = (*leaf_index)++;
        uint64_t file_count = rt->ns_plan->files_per_leaf_base;
        if (cur_leaf < rt->ns_plan->files_per_leaf_remainder) {
            ++file_count;
        }
        for (uint64_t i = 0; i < file_count; ++i) {
            const uint64_t inode_id = AllocInode(rt);
            const uint64_t sample_ord = (*file_ordinal)++;
            const uint64_t file_size = rt->sampler->Sample(namespace_id, sample_ord);
            if (!EmitFileMeta(rt,
                              parent_inode,
                              FormatFileName(cur_leaf, i),
                              inode_id,
                              file_size,
                              now_sec)) {
                return false;
            }
        }
        return true;
    }

    for (uint32_t slot = 0; slot < rt->ns_plan->branch_factor; ++slot) {
        const uint64_t child_inode = AllocInode(rt);
        const std::string dir_name = FormatDirName(level, slot);
        if (!EmitKv(rt, DentryKey(parent_inode, dir_name), EncodeU64(child_inode))) {
            return false;
        }
        ++rt->stats->dentry_count;
        if (!EmitDirInode(rt, child_inode, parent_inode, dir_name, now_sec)) {
            return false;
        }
        if (!GenerateTreeRec(rt, namespace_id, child_inode, level + 1, leaf_index, file_ordinal, now_sec)) {
            return false;
        }
    }
    return true;
}

} // namespace

bool MdsSstGenerator::Generate(const ClusterScaleConfig& cluster,
                               const DirectoryLayoutConfig& dir_cfg,
                               const FileSizeSamplerConfig& file_cfg,
                               const MdsSstGenConfig& gen_cfg,
                               MdsSstGenStats* out,
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
    if (cluster.namespace_count == 0) {
        if (error) {
            *error = "namespace_count is zero";
        }
        return false;
    }

    NamespaceLayoutPlan ns_plan;
    if (!NamespaceLayoutPlanner::BuildPlan(cluster, dir_cfg, &ns_plan, error)) {
        return false;
    }

    if (!EnsureDirRecursive(gen_cfg.output_dir, error)) {
        return false;
    }

    FileSizeSampler sampler(file_cfg);
    SstShardWriter writer(gen_cfg.output_dir, gen_cfg.sst_prefix, gen_cfg.max_kv_per_sst);
    MdsSstGenStats stats;

    const uint64_t now_sec = (gen_cfg.now_seconds == 0 ? NowSeconds() : gen_cfg.now_seconds);
    uint64_t next_inode = 2;
    const long double real_tb =
        static_cast<long double>(cluster.real_node_count) *
        static_cast<long double>(cluster.real_disks_per_node) *
        static_cast<long double>(cluster.real_disk_size_tb);
    const long double virtual_tb =
        static_cast<long double>(cluster.virtual_node_count) *
        static_cast<long double>(cluster.virtual_disks_per_node) *
        static_cast<long double>(cluster.virtual_disk_size_tb);
    const long double disk_budget_bytes =
        (real_tb + virtual_tb) * cluster.disk_replica_ratio * 1000.0L * 1000.0L * 1000.0L * 1000.0L;

    GeneratorRuntime rt;
    rt.cluster = &cluster;
    rt.ns_plan = &ns_plan;
    rt.cfg = &gen_cfg;
    rt.sampler = &sampler;
    rt.writer = &writer;
    rt.stats = &stats;
    rt.error = error;
    rt.next_inode = next_inode;
    rt.disk_budget_bytes = disk_budget_bytes;
    rt.disk_remaining_bytes = disk_budget_bytes;
    rt.progress_interval_files = gen_cfg.progress_interval_files;
    rt.progress_interval_sec = gen_cfg.progress_interval_sec;
    rt.next_progress_files = rt.progress_interval_files > 0 ? rt.progress_interval_files : UINT64_MAX;
    rt.start_tp = std::chrono::steady_clock::now();
    rt.last_progress_tp = rt.start_tp;

    if (!EmitDirInode(&rt, 1, 0, "/", now_sec)) {
        return false;
    }

    for (uint32_t ns = 0; ns < cluster.namespace_count; ++ns) {
        const uint64_t ns_inode = AllocInode(&rt);
        const std::string ns_name = FormatNamespaceName(ns);
        if (!EmitKv(&rt, DentryKey(1, ns_name), EncodeU64(ns_inode))) {
            return false;
        }
        ++stats.dentry_count;
        if (!EmitDirInode(&rt, ns_inode, 1, ns_name, now_sec)) {
            return false;
        }

        uint64_t leaf_index = 0;
        uint64_t file_ordinal = 0;
        if (!GenerateTreeRec(&rt, ns, ns_inode, 0, &leaf_index, &file_ordinal, now_sec)) {
            return false;
        }
    }

    if (!EmitKv(&rt, NextInodeKey(), EncodeU64(rt.next_inode))) {
        return false;
    }
    if (!EmitKv(&rt, NextHandleKey(), EncodeU64(1))) {
        return false;
    }

    if (!writer.Finish(error)) {
        return false;
    }

    stats.sst_files = writer.files();
    stats.sst_count = writer.shard_count();
    stats.next_inode = rt.next_inode;
    stats.disk_budget_bytes = rt.disk_budget_bytes;
    stats.disk_used_bytes = rt.disk_budget_bytes - rt.disk_remaining_bytes;
    MaybeLogProgress(&rt, true);
    *out = std::move(stats);
    return true;
}

} // namespace zb::meta_gen
