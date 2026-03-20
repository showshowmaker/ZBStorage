#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::mds {

class ArchiveBloomFilter {
public:
    ArchiveBloomFilter() = default;

    bool Initialize(uint64_t expected_items,
                    double false_positive_rate,
                    std::string* error);
    void AddUInt64(uint64_t value);
    bool MayContainUInt64(uint64_t value) const;

    bool SaveToFile(const std::string& path, std::string* error) const;
    bool LoadFromFile(const std::string& path, std::string* error);

    uint64_t bit_count() const {
        return bit_count_;
    }

    uint32_t hash_count() const {
        return hash_count_;
    }

    uint64_t item_count() const {
        return item_count_;
    }

    uint64_t byte_size() const {
        return static_cast<uint64_t>(bits_.size());
    }

private:
    static uint64_t Mix64(uint64_t value);
    void SetBit(uint64_t bit_index);
    bool GetBit(uint64_t bit_index) const;

    uint64_t bit_count_{0};
    uint32_t hash_count_{0};
    uint64_t item_count_{0};
    std::vector<uint8_t> bits_;
};

} // namespace zb::mds
