#pragma once

#include <cstdint>
#include <string>

#include "../../../msg/status.h"

namespace zb::real_node {

class IOExecutor {
public:
    zb::msg::Status Write(const std::string& path,
                          uint64_t offset,
                          const std::string& data,
                          uint64_t* bytes_written);

    zb::msg::Status Read(const std::string& path,
                         uint64_t offset,
                         uint64_t size,
                         std::string* out,
                         uint64_t* bytes_read);

    zb::msg::Status Delete(const std::string& path);
};

} // namespace zb::real_node
