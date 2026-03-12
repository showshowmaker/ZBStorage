#include "file_size_sampler.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>

namespace zb::meta_gen {

namespace {

constexpr uint64_t kDefaultLowMax = 1ULL * 1024ULL * 1024ULL;            // 1MB
constexpr uint64_t kDefaultMidMin = 1ULL * 1024ULL * 1024ULL;            // 1MB
constexpr uint64_t kDefaultMidMax = 512ULL * 1024ULL * 1024ULL;          // 512MB
constexpr uint64_t kDefaultHighMin = 512ULL * 1024ULL * 1024ULL;         // 512MB
constexpr uint64_t kDefaultHighMax = 2ULL * 1024ULL * 1024ULL * 1024ULL; // 2GB

uint64_t HashInput(uint64_t seed, uint64_t ns_id, uint64_t ordinal, uint64_t lane) {
    uint64_t x = seed;
    x ^= (ns_id + 0x9E3779B97F4A7C15ULL + (x << 6U) + (x >> 2U));
    x ^= (ordinal + 0xC2B2AE3D27D4EB4FULL + (x << 6U) + (x >> 2U));
    x ^= (lane * 0x165667919E3779F9ULL);
    return x;
}

uint64_t UniformClosed(uint64_t r, uint64_t lo, uint64_t hi) {
    if (lo >= hi) {
        return lo;
    }
    const uint64_t span = hi - lo + 1ULL;
    return lo + (r % span);
}

uint64_t UniformLogScale(uint64_t r, uint64_t lo, uint64_t hi) {
    if (lo >= hi) {
        return lo;
    }
    const long double u = static_cast<long double>(r) /
                          static_cast<long double>(std::numeric_limits<uint64_t>::max());
    const long double l0 = std::log(static_cast<long double>(lo));
    const long double l1 = std::log(static_cast<long double>(hi));
    const long double lv = l0 + (l1 - l0) * u;
    const long double val = std::exp(lv);
    if (val < 0.0L) {
        return lo;
    }
    if (val > static_cast<long double>(std::numeric_limits<uint64_t>::max())) {
        return hi;
    }
    return static_cast<uint64_t>(val);
}

} // namespace

FileSizeSampler::FileSizeSampler(FileSizeSamplerConfig cfg) : cfg_(std::move(cfg)) {
    if (cfg_.min_bytes == 0) {
        cfg_.min_bytes = 1;
    }
    if (cfg_.max_bytes < cfg_.min_bytes) {
        std::swap(cfg_.min_bytes, cfg_.max_bytes);
    }
}

uint64_t FileSizeSampler::Sample(uint64_t namespace_id, uint64_t file_ordinal) const {
    // 目标分布（默认范围下）:
    // - 5% : [1KB, 1MB] 线性
    // - 22%: [1MB, 512MB] 对数
    // - 73%: [512MB, 2GB] 线性
    // 默认参数下平均值约 1.0GB（十进制），满足压测负载目标。
    const uint64_t r0 = SplitMix64(HashInput(cfg_.seed, namespace_id, file_ordinal, 0));
    const uint64_t r1 = SplitMix64(HashInput(cfg_.seed, namespace_id, file_ordinal, 1));
    const long double u = ToUnit(r0);

    uint64_t sampled = cfg_.min_bytes;
    if (u < 0.05L) {
        const uint64_t lo = cfg_.min_bytes;
        const uint64_t hi = Clamp(kDefaultLowMax);
        sampled = UniformClosed(r1, lo, hi);
    } else if (u < 0.27L) {
        uint64_t lo = std::max<uint64_t>(cfg_.min_bytes, Clamp(kDefaultMidMin));
        uint64_t hi = Clamp(kDefaultMidMax);
        if (lo > hi) {
            lo = cfg_.min_bytes;
            hi = cfg_.max_bytes;
        }
        sampled = UniformLogScale(r1, lo, hi);
    } else {
        uint64_t lo = std::max<uint64_t>(cfg_.min_bytes, Clamp(kDefaultHighMin));
        uint64_t hi = Clamp(kDefaultHighMax);
        if (lo > hi) {
            lo = cfg_.min_bytes;
            hi = cfg_.max_bytes;
        }
        sampled = UniformClosed(r1, lo, hi);
    }

    return Clamp(sampled);
}

long double FileSizeSampler::EstimateAverage(uint64_t namespace_id, uint64_t sample_count) const {
    if (sample_count == 0) {
        return 0.0L;
    }
    long double total = 0.0L;
    for (uint64_t i = 0; i < sample_count; ++i) {
        total += static_cast<long double>(Sample(namespace_id, i));
    }
    return total / static_cast<long double>(sample_count);
}

uint64_t FileSizeSampler::SplitMix64(uint64_t x) {
    uint64_t z = x + 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31U);
}

long double FileSizeSampler::ToUnit(uint64_t x) {
    return static_cast<long double>(x) /
           static_cast<long double>(std::numeric_limits<uint64_t>::max());
}

uint64_t FileSizeSampler::Clamp(uint64_t bytes) const {
    if (bytes < cfg_.min_bytes) {
        return cfg_.min_bytes;
    }
    if (bytes > cfg_.max_bytes) {
        return cfg_.max_bytes;
    }
    return bytes;
}

} // namespace zb::meta_gen
