#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <string>

namespace zb::mds {

class RocksMetaStore {
public:
    RocksMetaStore() = default;
    ~RocksMetaStore();

    bool Open(const std::string& path, std::string* error);
    bool Put(const std::string& key, const std::string& value, std::string* error);
    bool Get(const std::string& key, std::string* value, std::string* error) const;
    bool Exists(const std::string& key, std::string* error) const;
    bool WriteBatch(rocksdb::WriteBatch* batch, std::string* error);

    rocksdb::DB* db() { return db_; }

private:
    rocksdb::DB* db_{nullptr};
};

} // namespace zb::mds
