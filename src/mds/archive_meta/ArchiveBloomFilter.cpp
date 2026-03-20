#include "ArchiveBloomFilter.h"

#include <cmath>
#include <fstream>

namespace zb::mds {

namespace {

constexpr char kArchiveBloomMagic[8] = {'A', 'R', 'C', 'B', 'L', 'M', '1', '\0'};

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

void AppendLe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

uint32_t DecodeLe32(const char* data) {
    return static_cast<uint32_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(data[3])) << 24);
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8);
    }
    return value;
}

} // namespace

bool ArchiveBloomFilter::Initialize(uint64_t expected_items,
                                    double false_positive_rate,
                                    std::string* error) {
    if (expected_items == 0) {
        expected_items = 1;
    }
    if (!(false_positive_rate > 0.0 && false_positive_rate < 1.0)) {
        if (error) {
            *error = "invalid bloom filter false_positive_rate";
        }
        return false;
    }

    const double numerator = -static_cast<double>(expected_items) * std::log(false_positive_rate);
    const double denominator = std::log(2.0) * std::log(2.0);
    uint64_t bit_count = static_cast<uint64_t>(std::ceil(numerator / denominator));
    if (bit_count < 64) {
        bit_count = 64;
    }
    const uint32_t hash_count = static_cast<uint32_t>(
        std::max(1.0, std::round((static_cast<double>(bit_count) / expected_items) * std::log(2.0))));

    bit_count_ = bit_count;
    hash_count_ = hash_count;
    item_count_ = 0;
    bits_.assign(static_cast<size_t>((bit_count_ + 7) / 8), 0);
    if (error) {
        error->clear();
    }
    return true;
}

void ArchiveBloomFilter::AddUInt64(uint64_t value) {
    if (bit_count_ == 0 || hash_count_ == 0 || bits_.empty()) {
        return;
    }
    const uint64_t h1 = Mix64(value ^ 0x9e3779b97f4a7c15ULL);
    const uint64_t h2 = Mix64(value ^ 0xc2b2ae3d27d4eb4fULL) | 1ULL;
    for (uint32_t i = 0; i < hash_count_; ++i) {
        SetBit((h1 + static_cast<uint64_t>(i) * h2) % bit_count_);
    }
    ++item_count_;
}

bool ArchiveBloomFilter::MayContainUInt64(uint64_t value) const {
    if (bit_count_ == 0 || hash_count_ == 0 || bits_.empty()) {
        return false;
    }
    const uint64_t h1 = Mix64(value ^ 0x9e3779b97f4a7c15ULL);
    const uint64_t h2 = Mix64(value ^ 0xc2b2ae3d27d4eb4fULL) | 1ULL;
    for (uint32_t i = 0; i < hash_count_; ++i) {
        if (!GetBit((h1 + static_cast<uint64_t>(i) * h2) % bit_count_)) {
            return false;
        }
    }
    return true;
}

bool ArchiveBloomFilter::SaveToFile(const std::string& path, std::string* error) const {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to open bloom filter output: " + path;
        }
        return false;
    }
    std::string header;
    header.append(kArchiveBloomMagic, sizeof(kArchiveBloomMagic));
    AppendLe64(&header, bit_count_);
    AppendLe32(&header, hash_count_);
    AppendLe64(&header, item_count_);
    if (!header.empty()) {
        out.write(header.data(), static_cast<std::streamsize>(header.size()));
    }
    if (!bits_.empty()) {
        out.write(reinterpret_cast<const char*>(bits_.data()), static_cast<std::streamsize>(bits_.size()));
    }
    if (!out.good()) {
        if (error) {
            *error = "failed to write bloom filter output: " + path;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ArchiveBloomFilter::LoadFromFile(const std::string& path, std::string* error) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        if (error) {
            *error = "failed to open bloom filter file: " + path;
        }
        return false;
    }

    char magic[sizeof(kArchiveBloomMagic)] = {};
    char raw_bit_count[sizeof(uint64_t)] = {};
    char raw_hash_count[sizeof(uint32_t)] = {};
    char raw_item_count[sizeof(uint64_t)] = {};
    if (!in.read(magic, static_cast<std::streamsize>(sizeof(magic))).good() ||
        !in.read(raw_bit_count, static_cast<std::streamsize>(sizeof(raw_bit_count))).good() ||
        !in.read(raw_hash_count, static_cast<std::streamsize>(sizeof(raw_hash_count))).good() ||
        !in.read(raw_item_count, static_cast<std::streamsize>(sizeof(raw_item_count))).good()) {
        if (error) {
            *error = "corrupted bloom filter file: " + path;
        }
        return false;
    }
    if (std::string(magic, sizeof(magic)) != std::string(kArchiveBloomMagic, sizeof(kArchiveBloomMagic))) {
        if (error) {
            *error = "invalid bloom filter header: " + path;
        }
        return false;
    }

    bit_count_ = DecodeLe64(raw_bit_count);
    hash_count_ = DecodeLe32(raw_hash_count);
    item_count_ = DecodeLe64(raw_item_count);
    bits_.assign(static_cast<size_t>((bit_count_ + 7) / 8), 0);
    if (!bits_.empty() &&
        !in.read(reinterpret_cast<char*>(bits_.data()), static_cast<std::streamsize>(bits_.size())).good()) {
        if (error) {
            *error = "corrupted bloom filter bitset: " + path;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

uint64_t ArchiveBloomFilter::Mix64(uint64_t value) {
    value ^= value >> 33;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33;
    return value;
}

void ArchiveBloomFilter::SetBit(uint64_t bit_index) {
    const size_t byte_index = static_cast<size_t>(bit_index / 8);
    const uint8_t mask = static_cast<uint8_t>(1U << (bit_index % 8));
    bits_[byte_index] |= mask;
}

bool ArchiveBloomFilter::GetBit(uint64_t bit_index) const {
    const size_t byte_index = static_cast<size_t>(bit_index / 8);
    const uint8_t mask = static_cast<uint8_t>(1U << (bit_index % 8));
    return (bits_[byte_index] & mask) != 0;
}

} // namespace zb::mds
