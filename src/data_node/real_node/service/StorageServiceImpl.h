#pragma once

#include "../io/DiskManager.h"
#include "../io/IOExecutor.h"
#include "../io/LocalPathResolver.h"
#include "../../../msg/storage_node_messages.h"

namespace zb::real_node {

class StorageServiceImpl {
public:
    StorageServiceImpl(DiskManager* disk_manager,
                       LocalPathResolver* path_resolver,
                       IOExecutor* io_executor);

    zb::msg::WriteChunkReply WriteChunk(const zb::msg::WriteChunkRequest& request);
    zb::msg::ReadChunkReply ReadChunk(const zb::msg::ReadChunkRequest& request);
    zb::msg::DiskReportReply GetDiskReport() const;

private:
    DiskManager* disk_manager_{};
    LocalPathResolver* path_resolver_{};
    IOExecutor* io_executor_{};
};

} // namespace zb::real_node
