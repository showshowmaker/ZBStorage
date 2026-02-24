#include "SchedulerConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <stdexcept>

namespace zb::scheduler {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

bool ParseUint64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = std::stoull(text);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

} // namespace

SchedulerConfig SchedulerConfig::LoadFromFile(const std::string& path, std::string* error) {
    SchedulerConfig cfg;
    std::ifstream input(path);
    if (!input) {
        if (error) {
            *error = "Failed to open config file: " + path;
        }
        return cfg;
    }

    std::string line;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "Invalid config line " + std::to_string(line_no) + ": " + line;
            }
            return {};
        }
        std::string key = Trim(trimmed.substr(0, eq));
        std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "SUSPECT_TIMEOUT_MS") {
            if (!ParseUint64(value, &cfg.suspect_timeout_ms)) {
                if (error) {
                    *error = "Invalid SUSPECT_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DEAD_TIMEOUT_MS") {
            if (!ParseUint64(value, &cfg.dead_timeout_ms)) {
                if (error) {
                    *error = "Invalid DEAD_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "TICK_INTERVAL_MS") {
            if (!ParseUint64(value, &cfg.tick_interval_ms)) {
                if (error) {
                    *error = "Invalid TICK_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "START_CMD_TEMPLATE") {
            cfg.start_cmd_template = value;
        } else if (key == "STOP_CMD_TEMPLATE") {
            cfg.stop_cmd_template = value;
        } else if (key == "REBOOT_CMD_TEMPLATE") {
            cfg.reboot_cmd_template = value;
        }
    }

    return cfg;
}

} // namespace zb::scheduler
