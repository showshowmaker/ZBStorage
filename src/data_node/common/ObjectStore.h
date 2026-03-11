#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "../../msg/status.h"

namespace zb::real_node {
class StorageServiceImpl;
}

namespace zb::virtual_node {
class VirtualStorageServiceImpl;
}

namespace zb::data_node {

struct ObjectWriteRequest {
    std::string disk_id;
    std::string object_id;
    uint64_t offset{0};
    std::string_view data;
    uint64_t epoch{0};
    bool is_replication{false};
};

struct ObjectReadRequest {
    std::string disk_id;
    std::string object_id;
    uint64_t offset{0};
    uint64_t size{0};
};

struct ObjectDeleteRequest {
    std::string disk_id;
    std::string object_id;
};

struct ObjectReadResult {
    zb::msg::Status status;
    std::string data;
};

class ObjectStore {
public:
    virtual ~ObjectStore() = default;

    virtual zb::msg::Status PutObject(const ObjectWriteRequest& request) = 0;
    virtual ObjectReadResult GetObject(const ObjectReadRequest& request) = 0;
    virtual zb::msg::Status DeleteObject(const ObjectDeleteRequest& request) = 0;
};

class RealObjectStore final : public ObjectStore {
public:
    explicit RealObjectStore(zb::real_node::StorageServiceImpl* storage);

    zb::msg::Status PutObject(const ObjectWriteRequest& request) override;
    ObjectReadResult GetObject(const ObjectReadRequest& request) override;
    zb::msg::Status DeleteObject(const ObjectDeleteRequest& request) override;

private:
    zb::real_node::StorageServiceImpl* storage_{};
};

class VirtualObjectStore final : public ObjectStore {
public:
    explicit VirtualObjectStore(zb::virtual_node::VirtualStorageServiceImpl* storage);

    zb::msg::Status PutObject(const ObjectWriteRequest& request) override;
    ObjectReadResult GetObject(const ObjectReadRequest& request) override;
    zb::msg::Status DeleteObject(const ObjectDeleteRequest& request) override;

private:
    zb::virtual_node::VirtualStorageServiceImpl* storage_{};
};

} // namespace zb::data_node
