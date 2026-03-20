#pragma once

#include <cstdint>

namespace zb::mds {

enum class ArchiveTableKind : uint8_t {
    kUnknown = 0,
    kInode = 1,
    kDentry = 2,
};

constexpr uint32_t kArchiveFormatVersion = 1;
constexpr uint32_t kArchiveDefaultPageSizeBytes = 64 * 1024;
constexpr uint32_t kArchiveSegmentFileHeaderSize = 24;
constexpr uint32_t kArchiveIndexFileHeaderSize = 20;
constexpr uint32_t kArchivePageHeaderSize = 8;

} // namespace zb::mds
