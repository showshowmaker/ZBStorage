#include "MasstreePageReader.h"

#include <algorithm>
#include <fstream>

namespace zb::mds {

namespace {

uint32_t DecodeLe32(const char* data) {
    uint32_t value = 0;
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

} // namespace

bool MasstreePageReader::LoadInodePage(const std::string& path,
                                       uint64_t page_offset,
                                       MasstreeInodePage* page,
                                       std::string* error) {
    if (!page || path.empty()) {
        if (error) {
            *error = "invalid masstree inode page load args";
        }
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open masstree inode pages: " + path;
        }
        return false;
    }
    input.seekg(static_cast<std::streamoff>(page_offset), std::ios::beg);
    if (!input.good()) {
        if (error) {
            *error = "failed to seek masstree inode page";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!input.read(len_buf, static_cast<std::streamsize>(sizeof(len_buf))).good()) {
        if (error) {
            *error = "failed to read masstree inode page length";
        }
        return false;
    }
    const uint32_t payload_len = DecodeLe32(len_buf);
    std::string payload(payload_len, '\0');
    if (payload_len != 0 &&
        !input.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
        if (error) {
            *error = "failed to read masstree inode page payload";
        }
        return false;
    }
    page->page_offset = page_offset;
    page->next_page_offset = page_offset + sizeof(uint32_t) + payload_len;
    return DecodeMasstreeInodePage(payload, page, error);
}

size_t MasstreePageReader::LowerBoundInodeInPage(const MasstreeInodePage& page, uint64_t inode_id) {
    return static_cast<size_t>(std::lower_bound(
        page.entries.begin(),
        page.entries.end(),
        inode_id,
        [](const MasstreeInodePageEntry& entry, uint64_t key) { return entry.inode_id < key; }) -
                               page.entries.begin());
}

bool MasstreePageReader::FindInodeInPage(const MasstreeInodePage& page,
                                         uint64_t inode_id,
                                         MasstreeInodePageEntry* entry,
                                         std::string* error) {
    const size_t index = LowerBoundInodeInPage(page, inode_id);
    if (index >= page.entries.size()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const auto& hit = page.entries[index];
    if (hit.inode_id != inode_id) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (entry) {
        *entry = hit;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreePageReader::LoadDentryPage(const std::string& path,
                                        uint64_t page_offset,
                                        MasstreeDentryPage* page,
                                        std::string* error) {
    if (!page || path.empty()) {
        if (error) {
            *error = "invalid masstree dentry page load args";
        }
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        if (error) {
            *error = "failed to open masstree dentry pages: " + path;
        }
        return false;
    }
    input.seekg(static_cast<std::streamoff>(page_offset), std::ios::beg);
    if (!input.good()) {
        if (error) {
            *error = "failed to seek masstree dentry page";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!input.read(len_buf, static_cast<std::streamsize>(sizeof(len_buf))).good()) {
        if (error) {
            *error = "failed to read masstree dentry page length";
        }
        return false;
    }
    const uint32_t payload_len = DecodeLe32(len_buf);
    std::string payload(payload_len, '\0');
    if (payload_len != 0 &&
        !input.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
        if (error) {
            *error = "failed to read masstree dentry page payload";
        }
        return false;
    }
    page->page_offset = page_offset;
    page->next_page_offset = page_offset + sizeof(uint32_t) + payload_len;
    return DecodeMasstreeDentryPage(payload, page, error);
}

size_t MasstreePageReader::LowerBoundDentryInPage(const MasstreeDentryPage& page,
                                                  uint64_t parent_inode,
                                                  const std::string& name) {
    return static_cast<size_t>(std::lower_bound(
        page.entries.begin(),
        page.entries.end(),
        std::pair<uint64_t, std::string>(parent_inode, name),
        [](const MasstreeDentryPageEntry& entry, const std::pair<uint64_t, std::string>& key) {
            return MasstreeDentryKeyLess(entry.parent_inode, entry.name, key.first, key.second);
        }) -
                               page.entries.begin());
}

bool MasstreePageReader::FindDentryInPage(const MasstreeDentryPage& page,
                                          uint64_t parent_inode,
                                          const std::string& name,
                                          MasstreeDentryPageEntry* entry,
                                          std::string* error) {
    const size_t index = LowerBoundDentryInPage(page, parent_inode, name);
    if (index >= page.entries.size()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const auto& hit = page.entries[index];
    if (hit.parent_inode != parent_inode || hit.name != name) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (entry) {
        *entry = hit;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
