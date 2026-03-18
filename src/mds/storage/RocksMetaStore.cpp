#include "RocksMetaStore.h"

#include <algorithm>
#include <vector>

namespace zb::mds {

RocksMetaStore::~RocksMetaStore() {
    for (auto& item : column_families_) {
        delete item.second;
    }
    column_families_.clear();
    delete db_;
    db_ = nullptr;
}

bool RocksMetaStore::Open(const std::string& path, std::string* error) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    std::vector<std::string> cf_names;
    rocksdb::DBOptions db_options(options);
    rocksdb::Status list_status = rocksdb::DB::ListColumnFamilies(db_options, path, &cf_names);
    if (!list_status.ok()) {
        cf_names.clear();
        cf_names.push_back(rocksdb::kDefaultColumnFamilyName);
    }
    auto ensure_cf = [&](const std::string& name) {
        if (std::find(cf_names.begin(), cf_names.end(), name) == cf_names.end()) {
            cf_names.push_back(name);
        }
    };
    ensure_cf(rocksdb::kDefaultColumnFamilyName);
    ensure_cf(kDiskFileLocationColumnFamily);
    ensure_cf(kOpticalFileLocationColumnFamily);

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }

    rocksdb::DB* db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status status = rocksdb::DB::Open(db_options, path, descriptors, &handles, &db);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }

    for (size_t i = 0; i < descriptors.size() && i < handles.size(); ++i) {
        column_families_[descriptors[i].name] = handles[i];
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

bool RocksMetaStore::PutDiskFileLocation(uint64_t inode_id, const std::string& value, std::string* error) {
    return PutToColumnFamily(GetColumnFamily(kDiskFileLocationColumnFamily), LocationKey(inode_id), value, error);
}

bool RocksMetaStore::GetDiskFileLocation(uint64_t inode_id, std::string* value, std::string* error) const {
    return GetFromColumnFamily(GetColumnFamily(kDiskFileLocationColumnFamily), LocationKey(inode_id), value, error);
}

bool RocksMetaStore::DeleteDiskFileLocation(uint64_t inode_id, std::string* error) {
    return DeleteFromColumnFamily(GetColumnFamily(kDiskFileLocationColumnFamily), LocationKey(inode_id), error);
}

bool RocksMetaStore::PutOpticalFileLocation(uint64_t inode_id, const std::string& value, std::string* error) {
    return PutToColumnFamily(GetColumnFamily(kOpticalFileLocationColumnFamily), LocationKey(inode_id), value, error);
}

bool RocksMetaStore::GetOpticalFileLocation(uint64_t inode_id, std::string* value, std::string* error) const {
    return GetFromColumnFamily(GetColumnFamily(kOpticalFileLocationColumnFamily), LocationKey(inode_id), value, error);
}

bool RocksMetaStore::DeleteOpticalFileLocation(uint64_t inode_id, std::string* error) {
    return DeleteFromColumnFamily(GetColumnFamily(kOpticalFileLocationColumnFamily), LocationKey(inode_id), error);
}

bool RocksMetaStore::BatchPutDiskFileLocation(rocksdb::WriteBatch* batch,
                                              uint64_t inode_id,
                                              const std::string& value,
                                              std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::ColumnFamilyHandle* cf = GetColumnFamily(kDiskFileLocationColumnFamily);
    if (!cf) {
        if (error) {
            *error = "disk file location column family is unavailable";
        }
        return false;
    }
    batch->Put(cf, LocationKey(inode_id), value);
    return true;
}

bool RocksMetaStore::BatchDeleteDiskFileLocation(rocksdb::WriteBatch* batch,
                                                 uint64_t inode_id,
                                                 std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::ColumnFamilyHandle* cf = GetColumnFamily(kDiskFileLocationColumnFamily);
    if (!cf) {
        if (error) {
            *error = "disk file location column family is unavailable";
        }
        return false;
    }
    batch->Delete(cf, LocationKey(inode_id));
    return true;
}

bool RocksMetaStore::BatchPutOpticalFileLocation(rocksdb::WriteBatch* batch,
                                                 uint64_t inode_id,
                                                 const std::string& value,
                                                 std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::ColumnFamilyHandle* cf = GetColumnFamily(kOpticalFileLocationColumnFamily);
    if (!cf) {
        if (error) {
            *error = "optical file location column family is unavailable";
        }
        return false;
    }
    batch->Put(cf, LocationKey(inode_id), value);
    return true;
}

bool RocksMetaStore::BatchDeleteOpticalFileLocation(rocksdb::WriteBatch* batch,
                                                    uint64_t inode_id,
                                                    std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::ColumnFamilyHandle* cf = GetColumnFamily(kOpticalFileLocationColumnFamily);
    if (!cf) {
        if (error) {
            *error = "optical file location column family is unavailable";
        }
        return false;
    }
    batch->Delete(cf, LocationKey(inode_id));
    return true;
}

std::string RocksMetaStore::LocationKey(uint64_t inode_id) {
    return std::to_string(inode_id);
}

rocksdb::ColumnFamilyHandle* RocksMetaStore::GetColumnFamily(const std::string& name) const {
    auto it = column_families_.find(name);
    if (it == column_families_.end()) {
        return nullptr;
    }
    return it->second;
}

bool RocksMetaStore::PutToColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                                       const std::string& key,
                                       const std::string& value,
                                       std::string* error) {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    if (!cf) {
        if (error) {
            *error = "column family is unavailable";
        }
        return false;
    }
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), cf, key, value);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::GetFromColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                                         const std::string& key,
                                         std::string* value,
                                         std::string* error) const {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    if (!cf) {
        if (error) {
            *error = "column family is unavailable";
        }
        return false;
    }
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), cf, key, value);
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

bool RocksMetaStore::DeleteFromColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                                            const std::string& key,
                                            std::string* error) {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    if (!cf) {
        if (error) {
            *error = "column family is unavailable";
        }
        return false;
    }
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), cf, key);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

} // namespace zb::mds
