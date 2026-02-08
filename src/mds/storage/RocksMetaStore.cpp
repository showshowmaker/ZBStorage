#include "RocksMetaStore.h"

namespace zb::mds {

RocksMetaStore::~RocksMetaStore() {
    delete db_;
    db_ = nullptr;
}

bool RocksMetaStore::Open(const std::string& path, std::string* error) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    db_ = db;
    return true;
}

bool RocksMetaStore::Put(const std::string& key, const std::string& value, std::string* error) {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::Get(const std::string& key, std::string* value, std::string* error) const {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return false;
        }
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::Exists(const std::string& key, std::string* error) const {
    std::string value;
    return Get(key, &value, error);
}

bool RocksMetaStore::WriteBatch(rocksdb::WriteBatch* batch, std::string* error) {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), batch);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

} // namespace zb::mds
