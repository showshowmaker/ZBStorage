#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <string>
#include <unordered_map>

namespace zb::mds {

class RocksMetaStore {
public:
    // Legacy compatibility column families. New stores do not proactively create them.
    // New inode/location writes should stay on the unified inode payload path and only
    // use these for reading/deleting old data when an existing DB still carries them.
    static constexpr const char* kDiskFileLocationColumnFamily = "disk_file_locations";
    static constexpr const char* kOpticalFileLocationColumnFamily = "optical_file_locations";

    RocksMetaStore() = default;
    ~RocksMetaStore();

    bool Open(const std::string& path, std::string* error);
    bool Put(const std::string& key, const std::string& value, std::string* error);
    bool Get(const std::string& key, std::string* value, std::string* error) const;
    bool Exists(const std::string& key, std::string* error) const;
    bool WriteBatch(rocksdb::WriteBatch* batch, std::string* error);
    bool LegacyGetDiskFileLocation(uint64_t inode_id, std::string* value, std::string* error) const;
    bool LegacyDeleteDiskFileLocation(uint64_t inode_id, std::string* error);
    bool LegacyGetOpticalFileLocation(uint64_t inode_id, std::string* value, std::string* error) const;
    bool LegacyDeleteOpticalFileLocation(uint64_t inode_id, std::string* error);
    bool LegacyBatchDeleteDiskFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, std::string* error) const;
    bool LegacyBatchDeleteOpticalFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, std::string* error) const;

    rocksdb::DB* db() { return db_; }
    rocksdb::DB* db() const { return db_; }

private:
    static std::string LocationKey(uint64_t inode_id);
    rocksdb::ColumnFamilyHandle* GetColumnFamily(const std::string& name) const;
    bool GetFromColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                             const std::string& key,
                             std::string* value,
                             std::string* error) const;
    bool DeleteFromColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                                const std::string& key,
                                std::string* error);

    rocksdb::DB* db_{nullptr};
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> column_families_;
};

} // namespace zb::mds
