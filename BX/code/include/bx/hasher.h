#pragma once

#include <cstdint>

#include "bx/types.h"

namespace bx {

class Hasher {
 public:
  Fingerprint HashChunk(const ChunkDesc& chunk) const;

 private:
  static std::uint64_t Fnv1a64(const std::uint8_t* data, std::size_t len);
};

}  // namespace bx
