#pragma once

#include <cstdint>
#include <string>

#include "MasstreeOpticalProfile.h"

namespace zb::mds {

class MasstreeOpticalAllocator {
public:
    MasstreeOpticalAllocator(const MasstreeOpticalProfile& profile,
                             const MasstreeOpticalClusterCursor& start_cursor);

    bool Allocate(uint64_t file_size_bytes, uint64_t* global_image_id, std::string* error);
    const MasstreeOpticalClusterCursor& cursor() const { return cursor_; }

private:
    bool NormalizeCursor(std::string* error);
    bool AdvanceToNextImage(std::string* error);

    MasstreeOpticalProfile profile_;
    MasstreeOpticalClusterCursor cursor_;
};

} // namespace zb::mds
