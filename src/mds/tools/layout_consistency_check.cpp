#include <gflags/gflags.h>
#include <rocksdb/iterator.h>

#include <iostream>
#include <memory>
#include <string>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "../storage/RocksMetaStore.h"

DEFINE_string(db_path, "", "Path to MDS RocksDB");
DEFINE_uint64(start_inode, 0, "Start inode id (inclusive), 0 means auto");
DEFINE_uint64(end_inode, 0, "End inode id (inclusive), 0 means no limit");
DEFINE_uint64(limit, 0, "Max checked inode count, 0 means unlimited");
DEFINE_uint32(progress_every, 2000, "Print progress every N scanned inodes");
DEFINE_bool(strict_require_layout_root, false, "Require every file inode to have LR/<inode>");
DEFINE_bool(require_reverse_index, false, "Require reverse object index to exist and match");

namespace zb::mds {

namespace {

struct Stats {
    uint64_t scanned{0};
    uint64_t checked{0};
    uint64_t skipped_non_file{0};
    uint64_t missing_layout_root{0};
    uint64_t invalid_layout_root{0};
    uint64_t missing_layout_object{0};
    uint64_t invalid_layout_object{0};
    uint64_t invalid_extent{0};
    uint64_t missing_reverse_index{0};
    uint64_t missing_object_meta{0};
    uint64_t invalid_object_meta{0};
    uint64_t object_mismatch{0};
};

bool ParseInodeKey(const std::string& key, uint64_t* inode_id) {
    if (!inode_id || key.size() < 3 || key[0] != 'I' || key[1] != '/') {
        return false;
    }
    try {
        *inode_id = static_cast<uint64_t>(std::stoull(key.substr(2)));
        return true;
    } catch (...) {
        return false;
    }
}

std::string InodeSeekKey(uint64_t inode_id) {
    return "I/" + std::to_string(inode_id);
}

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

bool HasReplicaObjectId(const zb::rpc::ObjectMeta& meta, const std::string& object_id) {
    for (const auto& replica : meta.replicas()) {
        if (ReplicaObjectId(replica) == object_id) {
            return true;
        }
    }
    return false;
}

} // namespace

} // namespace zb::mds

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_db_path.empty()) {
        std::cerr << "Missing --db_path" << std::endl;
        return 1;
    }

    std::string error;
    zb::mds::RocksMetaStore store;
    if (!store.Open(FLAGS_db_path, &error)) {
        std::cerr << "Failed to open db: " << error << std::endl;
        return 1;
    }

    zb::mds::Stats stats;
    std::unique_ptr<rocksdb::Iterator> it(store.db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(zb::mds::InodeSeekKey(FLAGS_start_inode)); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind("I/", 0) != 0) {
            break;
        }
        uint64_t inode_id = 0;
        if (!zb::mds::ParseInodeKey(key, &inode_id)) {
            continue;
        }
        if (FLAGS_end_inode > 0 && inode_id > FLAGS_end_inode) {
            break;
        }
        ++stats.scanned;

        zb::rpc::InodeAttr inode;
        if (!zb::mds::MetaCodec::DecodeInodeAttr(it->value().ToString(), &inode)) {
            continue;
        }
        if (inode.type() != zb::rpc::INODE_FILE) {
            ++stats.skipped_non_file;
            continue;
        }

        ++stats.checked;

        do {
            std::string root_data;
            std::string get_error;
            if (!store.Get(zb::mds::LayoutRootKey(inode_id), &root_data, &get_error)) {
                if (!get_error.empty() || FLAGS_strict_require_layout_root) {
                    ++stats.missing_layout_root;
                }
                break;
            }

            zb::mds::LayoutRootRecord root;
            if (!zb::mds::MetaCodec::DecodeLayoutRoot(root_data, &root)) {
                ++stats.invalid_layout_root;
                break;
            }

            if (root.layout_root_id.empty()) {
                ++stats.invalid_layout_root;
                break;
            }

            std::string node_data;
            get_error.clear();
            if (!store.Get(zb::mds::LayoutObjectKey(root.layout_root_id), &node_data, &get_error)) {
                ++stats.missing_layout_object;
                break;
            }

            zb::mds::LayoutNodeRecord node;
            if (!zb::mds::MetaCodec::DecodeLayoutNode(node_data, &node)) {
                ++stats.invalid_layout_object;
                break;
            }

            for (const auto& extent : node.extents) {
                if (extent.object_id.empty()) {
                    ++stats.invalid_extent;
                    continue;
                }
                const uint64_t object_unit_size = inode.object_unit_size() > 0 ? inode.object_unit_size() : 1;
                uint32_t object_index = extent.object_index;
                if (object_index == 0 && extent.logical_offset > 0) {
                    object_index = static_cast<uint32_t>(extent.logical_offset / object_unit_size);
                }
                const std::string object_key = zb::mds::ObjectKey(inode_id, object_index);
                std::string object_data;
                std::string local_error;
                if (!store.Get(object_key, &object_data, &local_error)) {
                    ++stats.missing_object_meta;
                    continue;
                }

                zb::rpc::ObjectMeta meta;
                if (!zb::mds::MetaCodec::DecodeObjectMeta(object_data, &meta)) {
                    ++stats.invalid_object_meta;
                    continue;
                }
                if (!zb::mds::HasReplicaObjectId(meta, extent.object_id)) {
                    ++stats.object_mismatch;
                }
                if (FLAGS_require_reverse_index) {
                    std::string reverse_object_key;
                    local_error.clear();
                    if (!store.Get(zb::mds::ReverseObjectKey(extent.object_id), &reverse_object_key, &local_error)) {
                        ++stats.missing_reverse_index;
                        continue;
                    }
                    if (reverse_object_key != object_key) {
                        ++stats.object_mismatch;
                    }
                }
            }
        } while (false);

        if (FLAGS_progress_every > 0 && stats.scanned % FLAGS_progress_every == 0) {
            std::cout << "progress scanned=" << stats.scanned
                      << " checked=" << stats.checked
                      << " missing_lr=" << stats.missing_layout_root
                      << " missing_lo=" << stats.missing_layout_object
                      << " missing_rc=" << stats.missing_reverse_index << std::endl;
        }
        if (FLAGS_limit > 0 && stats.checked >= FLAGS_limit) {
            break;
        }
    }

    const uint64_t failures = stats.missing_layout_root +
                              stats.invalid_layout_root +
                              stats.missing_layout_object +
                              stats.invalid_layout_object +
                              stats.invalid_extent +
                              stats.missing_reverse_index +
                              stats.missing_object_meta +
                              stats.invalid_object_meta +
                              stats.object_mismatch;

    std::cout << "done scanned=" << stats.scanned
              << " checked=" << stats.checked
              << " skipped_non_file=" << stats.skipped_non_file
              << " missing_layout_root=" << stats.missing_layout_root
              << " invalid_layout_root=" << stats.invalid_layout_root
              << " missing_layout_object=" << stats.missing_layout_object
              << " invalid_layout_object=" << stats.invalid_layout_object
              << " invalid_extent=" << stats.invalid_extent
              << " missing_reverse_index=" << stats.missing_reverse_index
              << " missing_object_meta=" << stats.missing_object_meta
              << " invalid_object_meta=" << stats.invalid_object_meta
              << " object_mismatch=" << stats.object_mismatch
              << " failures=" << failures
              << std::endl;
    return failures == 0 ? 0 : 2;
}
