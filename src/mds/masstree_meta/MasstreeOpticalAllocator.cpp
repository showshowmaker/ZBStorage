#include "MasstreeOpticalAllocator.h"

namespace zb::mds {

MasstreeOpticalAllocator::MasstreeOpticalAllocator(const MasstreeOpticalProfile& profile,
                                                   const MasstreeOpticalClusterCursor& start_cursor)
    : profile_(profile),
      cursor_(start_cursor) {
}

bool MasstreeOpticalAllocator::Allocate(uint64_t file_size_bytes,
                                        uint64_t* global_image_id,
                                        std::string* error) {
    if (!global_image_id || file_size_bytes == 0) {
        if (error) {
            *error = "invalid masstree optical allocation args";
        }
        return false;
    }
    if (file_size_bytes > profile_.image_capacity_bytes) {
        if (error) {
            *error = "file size exceeds masstree optical image capacity";
        }
        return false;
    }
    if (!NormalizeCursor(error)) {
        return false;
    }
    if (cursor_.image_used_bytes + file_size_bytes > profile_.image_capacity_bytes) {
        if (!AdvanceToNextImage(error)) {
            return false;
        }
    }

    *global_image_id = profile_.GlobalImageId(cursor_);
    cursor_.image_used_bytes += file_size_bytes;
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeOpticalAllocator::NormalizeCursor(std::string* error) {
    while (cursor_.node_index < profile_.optical_node_count) {
        if (cursor_.disk_index >= profile_.disks_per_node) {
            cursor_.disk_index = 0;
            cursor_.image_index_in_disk = 0;
            cursor_.image_used_bytes = 0;
            ++cursor_.node_index;
            continue;
        }
        const uint32_t images_per_disk = profile_.ImagesPerDisk(cursor_.disk_index);
        if (cursor_.image_index_in_disk >= images_per_disk) {
            cursor_.image_index_in_disk = 0;
            cursor_.image_used_bytes = 0;
            ++cursor_.disk_index;
            continue;
        }
        if (cursor_.image_used_bytes >= profile_.image_capacity_bytes) {
            cursor_.image_used_bytes = 0;
            ++cursor_.image_index_in_disk;
            continue;
        }
        if (error) {
            error->clear();
        }
        return true;
    }
    if (error) {
        *error = "masstree optical cluster capacity is exhausted";
    }
    return false;
}

bool MasstreeOpticalAllocator::AdvanceToNextImage(std::string* error) {
    cursor_.image_used_bytes = 0;
    ++cursor_.image_index_in_disk;
    return NormalizeCursor(error);
}

} // namespace zb::mds
