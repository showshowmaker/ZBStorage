#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <string>
#include <unordered_map>

namespace zb::mds {

class RocksMetaStore {
public:
    static constexpr const char* kDiskFileLocationColumnFamily = "disk_file_locations";
    static constexpr const char* kOpticalFileLocationColumnFamily = "optical_file_locations";

    RocksMetaStore() = default;
    ~RocksMetaStore();

    bool Open(const std::string& path, std::string* error);
    bool Put(const std::string& key, const std::string& value, std::string* error);
    bool Get(const std::string& key, std::string* value, std::string* error) const;
    bool Exists(const std::string& key, std::string* error) const;
    bool WriteBatch(rocksdb::WriteBatch* batch, std::string* error);
    bool PutDiskFileLocation(uint64_t inode_id, const std::string& value, std::string* error);
    bool GetDiskFileLocation(uint64_t inode_id, std::string* value, std::string* error) const;
    bool DeleteDiskFileLocation(uint64_t inode_id, std::string* error);
    bool PutOpticalFileLocation(uint64_t inode_id, const std::string& value, std::string* error);
    bool GetOpticalFileLocation(uint64_t inode_id, std::string* value, std::string* error) const;
    bool DeleteOpticalFileLocation(uint64_t inode_id, std::string* error);
    bool BatchPutDiskFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, const std::string& value, std::string* error) const;
    bool BatchDeleteDiskFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, std::string* error) const;
    bool BatchPutOpticalFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, const std::string& value, std::string* error) const;
    bool BatchDeleteOpticalFileLocation(rocksdb::WriteBatch* batch, uint64_t inode_id, std::string* error) const;

    rocksdb::DB* db() { return db_; }

private:
    static std::string LocationKey(uint64_t inode_id);
    rocksdb::ColumnFamilyHandle* GetColumnFamily(const std::string& name) const;
    bool PutToColumnFamily(rocksdb::ColumnFamilyHandle* cf,
                           const std::string& key,
                           const std::string& value,
                           std::string* error);
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
