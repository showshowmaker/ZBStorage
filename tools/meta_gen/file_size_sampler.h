#pragma once

#include <cstddef>
#include <cstdint>

#include "config.h"

namespace zb::meta_gen {

class FileSizeSampler {
public:
    explicit FileSizeSampler(FileSizeSamplerConfig cfg);

    uint64_t Sample(uint64_t namespace_id, uint64_t file_ordinal) const;
    long double EstimateAverage(uint64_t namespace_id, uint64_t sample_count) const;

private:
    static uint64_t SplitMix64(uint64_t x);
    static long double ToUnit(uint64_t x);
    uint64_t Clamp(uint64_t bytes) const;

    FileSizeSamplerConfig cfg_;
};

} // namespace zb::meta_gen
