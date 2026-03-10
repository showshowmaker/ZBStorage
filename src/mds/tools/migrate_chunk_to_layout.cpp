#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <string>
#include <algorithm>
#include <chrono>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "../storage/RocksMetaStore.h"

DEFINE_string(db_path, "", "Path to MDS RocksDB");
DEFINE_uint64(start_inode, 0, "Start inode id (inclusive), 0 means auto");
DEFINE_uint64(end_inode, 0, "End inode id (inclusive), 0 means no limit");
DEFINE_bool(dry_run, false, "Only scan and print stats, do not write");
DEFINE_uint64(limit, 0, "Max migrated inode count, 0 means unlimited");
DEFINE_bool(resume_from_checkpoint, true, "Resume from stored checkpoint key");
DEFINE_string(checkpoint_key, "MG/chunk_to_layout/last_inode", "Checkpoint key");
DEFINE_uint32(progress_every, 1000, "Print progress every N scanned inodes");

namespace zb::mds {

namespace {

uint64_t NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

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

    uint64_t scan_start_inode = FLAGS_start_inode;
    if (FLAGS_resume_from_checkpoint) {
        std::string ckpt_data;
        std::string ckpt_error;
        if (store.Get(FLAGS_checkpoint_key, &ckpt_data, &ckpt_error)) {
            uint64_t last_inode = 0;
            if (zb::mds::MetaCodec::DecodeUInt64(ckpt_data, &last_inode)) {
                scan_start_inode = std::max<uint64_t>(scan_start_inode, last_inode + 1);
            }
        }
    }

    uint64_t scanned = 0;
    uint64_t migrated = 0;
    uint64_t skipped_non_file = 0;
    uint64_t skipped_existing_root = 0;
    uint64_t skipped_range = 0;
    uint64_t parse_fail = 0;

    std::unique_ptr<rocksdb::Iterator> it(store.db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(zb::mds::InodeSeekKey(scan_start_inode)); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind("I/", 0) != 0) {
            break;
        }

        uint64_t inode_id = 0;
        if (!zb::mds::ParseInodeKey(key, &inode_id)) {
            ++parse_fail;
            continue;
        }
        if (FLAGS_end_inode > 0 && inode_id > FLAGS_end_inode) {
            break;
        }
        if (inode_id < scan_start_inode) {
            ++skipped_range;
            continue;
        }

        ++scanned;

        zb::rpc::InodeAttr inode;
        if (!zb::mds::MetaCodec::DecodeInodeAttr(it->value().ToString(), &inode)) {
            ++parse_fail;
            continue;
        }
        if (inode.type() != zb::rpc::INODE_FILE) {
            ++skipped_non_file;
            continue;
        }

        std::string exists_error;
        if (store.Exists(zb::mds::LayoutRootKey(inode_id), &exists_error)) {
            ++skipped_existing_root;
        } else if (!exists_error.empty()) {
            std::cerr << "Exists check failed for inode=" << inode_id << ": " << exists_error << std::endl;
            return 2;
        } else {
            zb::mds::LayoutRootRecord root;
            root.inode_id = inode_id;
            root.layout_root_id = "legacy:" + std::to_string(inode_id);
            root.layout_version = std::max<uint64_t>(1, inode.version());
            root.file_size = inode.size();
            root.epoch = 0;
            root.update_ts = zb::mds::NowMilliseconds();

            if (!FLAGS_dry_run) {
                rocksdb::WriteBatch batch;
                batch.Put(zb::mds::LayoutRootKey(inode_id), zb::mds::MetaCodec::EncodeLayoutRoot(root));
                batch.Put(FLAGS_checkpoint_key, zb::mds::MetaCodec::EncodeUInt64(inode_id));
                std::string write_error;
                if (!store.WriteBatch(&batch, &write_error)) {
                    std::cerr << "Write failed for inode=" << inode_id << ": " << write_error << std::endl;
                    return 3;
                }
            }
            ++migrated;
        }

        if (FLAGS_progress_every > 0 && scanned % FLAGS_progress_every == 0) {
            std::cout << "progress scanned=" << scanned << " migrated=" << migrated << std::endl;
        }
        if (FLAGS_limit > 0 && migrated >= FLAGS_limit) {
            break;
        }
    }

    std::cout << "done scanned=" << scanned
              << " migrated=" << migrated
              << " skipped_non_file=" << skipped_non_file
              << " skipped_existing_root=" << skipped_existing_root
              << " skipped_range=" << skipped_range
              << " parse_fail=" << parse_fail
              << " dry_run=" << (FLAGS_dry_run ? "true" : "false")
              << std::endl;
    return 0;
}
