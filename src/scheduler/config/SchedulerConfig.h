#pragma once

#include <cstdint>
#include <string>

namespace zb::scheduler {

struct SchedulerConfig {
    uint64_t suspect_timeout_ms{6000};
    uint64_t dead_timeout_ms{15000};
    uint64_t tick_interval_ms{1000};

    std::string start_cmd_template;
    std::string stop_cmd_template;
    std::string reboot_cmd_template;

    static SchedulerConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::scheduler
