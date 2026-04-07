#include "SystemMemory.h"

#include <fstream>
#include <sstream>
#include <string>

namespace zb::mds {

namespace {

bool ParseMeminfoKiBLine(const std::string& line, const char* key, uint64_t* value_bytes) {
    if (!value_bytes) {
        return false;
    }
    const std::string prefix(key);
    if (line.rfind(prefix, 0) != 0) {
        return false;
    }
    std::istringstream stream(line.substr(prefix.size()));
    uint64_t value_kib = 0;
    std::string unit;
    if (!(stream >> value_kib >> unit)) {
        return false;
    }
    *value_bytes = value_kib * 1024ULL;
    return true;
}

} // namespace

bool ReadSystemMemorySnapshot(SystemMemorySnapshot* snapshot) {
    if (!snapshot) {
        return false;
    }
#if defined(__linux__)
    std::ifstream input("/proc/meminfo");
    if (!input) {
        return false;
    }

    uint64_t total_bytes = 0;
    uint64_t available_bytes = 0;
    std::string line;
    while (std::getline(input, line)) {
        uint64_t parsed = 0;
        if (ParseMeminfoKiBLine(line, "MemTotal:", &parsed)) {
            total_bytes = parsed;
        } else if (ParseMeminfoKiBLine(line, "MemAvailable:", &parsed)) {
            available_bytes = parsed;
        }
    }
    if (total_bytes == 0 || available_bytes == 0) {
        return false;
    }
    snapshot->total_bytes = total_bytes;
    snapshot->available_bytes = available_bytes;
    return true;
#else
    return false;
#endif
}

} // namespace zb::mds
