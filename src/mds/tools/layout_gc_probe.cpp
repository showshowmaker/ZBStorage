#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "../storage/RocksMetaStore.h"

DEFINE_string(db_path, "", "Path to MDS RocksDB");
DEFINE_string(mode, "inject", "inject|exists|wait_deleted");
DEFINE_string(orphan_id, "", "Orphan layout object id");
DEFINE_uint32(replica_count, 3, "Replica count for injected orphan layout object");
DEFINE_uint64(seed_seen_age_ms, 0, "If >0, set LGS first_seen to now-seed_seen_age_ms");
DEFINE_uint32(timeout_sec, 30, "Timeout seconds for wait_deleted");
DEFINE_uint32(poll_interval_ms, 200, "Poll interval milliseconds for wait_deleted");

namespace zb::mds {

uint64_t NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

bool ExistsPrimaryOrReplica(RocksMetaStore* store, const std::string& orphan_id, bool* exists_any, std::string* error) {
    if (!store || !exists_any || orphan_id.empty()) {
        if (error) {
            *error = "invalid exists probe args";
        }
        return false;
    }
    *exists_any = false;
    std::string local_error;
    if (store->Exists(LayoutObjectKey(orphan_id), &local_error)) {
        *exists_any = true;
        return true;
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    const std::string prefix = LayoutObjectReplicaPrefix(orphan_id);
    std::unique_ptr<rocksdb::Iterator> it(store->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        *exists_any = true;
        return true;
    }
    return true;
}

bool ExistsPrimaryOrReplicaReadOnly(const std::string& db_path,
                                    const std::string& orphan_id,
                                    bool* exists_any,
                                    std::string* error) {
    if (!exists_any || db_path.empty() || orphan_id.empty()) {
        if (error) {
            *error = "invalid readonly exists probe args";
        }
        return false;
    }
    *exists_any = false;

    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::DB* db_ptr = nullptr;
    const rocksdb::Status st = rocksdb::DB::OpenForReadOnly(options, db_path, &db_ptr);
    if (!st.ok()) {
        if (error) {
            *error = st.ToString();
        }
        return false;
    }
    std::unique_ptr<rocksdb::DB> db(db_ptr);

    std::string value;
    rocksdb::Status get_st = db->Get(rocksdb::ReadOptions(), LayoutObjectKey(orphan_id), &value);
    if (get_st.ok()) {
        *exists_any = true;
        return true;
    }
    if (!get_st.IsNotFound()) {
        if (error) {
            *error = get_st.ToString();
        }
        return false;
    }
    const std::string prefix = LayoutObjectReplicaPrefix(orphan_id);
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        *exists_any = true;
        return true;
    }
    if (!it->status().ok()) {
        if (error) {
            *error = it->status().ToString();
        }
        return false;
    }
    return true;
}

bool InjectOrphan(RocksMetaStore* store, const std::string& orphan_id, uint32_t replica_count, uint64_t seed_seen_age_ms, std::string* error) {
    if (!store || orphan_id.empty()) {
        if (error) {
            *error = "invalid inject args";
        }
        return false;
    }
    LayoutNodeRecord node;
    node.node_id = orphan_id;
    node.level = 0;
    const std::string encoded = MetaCodec::EncodeLayoutNode(node);

    rocksdb::WriteBatch batch;
    batch.Put(LayoutObjectKey(orphan_id), encoded);
    const uint32_t rep = std::max<uint32_t>(1, replica_count);
    for (uint32_t i = 1; i < rep; ++i) {
        batch.Put(LayoutObjectReplicaKey(orphan_id, i), encoded);
    }
    if (seed_seen_age_ms > 0) {
        const uint64_t now = NowMilliseconds();
        const uint64_t first_seen = (now > seed_seen_age_ms) ? (now - seed_seen_age_ms) : 0;
        batch.Put(LayoutGcSeenKey(orphan_id), MetaCodec::EncodeUInt64(first_seen));
    }

    std::string write_error;
    if (!store->WriteBatch(&batch, &write_error)) {
        if (error) {
            *error = write_error.empty() ? "inject write batch failed" : write_error;
        }
        return false;
    }
    return true;
}

} // namespace zb::mds

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_db_path.empty() || FLAGS_orphan_id.empty()) {
        std::cerr << "Missing --db_path or --orphan_id" << std::endl;
        return 1;
    }

    std::string error;
    const std::string mode = FLAGS_mode;
    if (mode == "inject") {
        zb::mds::RocksMetaStore store;
        if (!store.Open(FLAGS_db_path, &error)) {
            std::cerr << "Open db failed: " << error << std::endl;
            return 2;
        }
        if (!zb::mds::InjectOrphan(&store,
                                   FLAGS_orphan_id,
                                   FLAGS_replica_count,
                                   FLAGS_seed_seen_age_ms,
                                   &error)) {
            std::cerr << "Inject orphan failed: " << error << std::endl;
            return 3;
        }
        std::cout << "INJECTED orphan_id=" << FLAGS_orphan_id
                  << " replica_count=" << std::max<uint32_t>(1, FLAGS_replica_count)
                  << " seed_seen_age_ms=" << FLAGS_seed_seen_age_ms
                  << std::endl;
        return 0;
    }

    if (mode == "exists") {
        bool exists_any = false;
        if (!zb::mds::ExistsPrimaryOrReplicaReadOnly(FLAGS_db_path, FLAGS_orphan_id, &exists_any, &error)) {
            std::cerr << "Exists probe failed: " << error << std::endl;
            return 4;
        }
        std::cout << (exists_any ? "EXISTS" : "NOT_EXISTS") << " orphan_id=" << FLAGS_orphan_id << std::endl;
        return exists_any ? 0 : 10;
    }

    if (mode == "wait_deleted") {
        const uint32_t timeout_s = std::max<uint32_t>(1, FLAGS_timeout_sec);
        const uint32_t poll_ms = std::max<uint32_t>(50, FLAGS_poll_interval_ms);
        const uint64_t deadline = zb::mds::NowMilliseconds() + static_cast<uint64_t>(timeout_s) * 1000ULL;
        while (true) {
            bool exists_any = false;
            if (!zb::mds::ExistsPrimaryOrReplicaReadOnly(FLAGS_db_path, FLAGS_orphan_id, &exists_any, &error)) {
                std::cerr << "Wait probe failed: " << error << std::endl;
                return 5;
            }
            if (!exists_any) {
                std::cout << "DELETED orphan_id=" << FLAGS_orphan_id << std::endl;
                return 0;
            }
            if (zb::mds::NowMilliseconds() >= deadline) {
                std::cerr << "WAIT_DELETED timeout orphan_id=" << FLAGS_orphan_id << std::endl;
                return 6;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
        }
    }

    std::cerr << "Unknown --mode: " << mode << std::endl;
    return 7;
}
