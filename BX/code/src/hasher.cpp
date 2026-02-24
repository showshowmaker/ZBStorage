#include "bx/hasher.h"

namespace bx {

std::uint64_t Hasher::Fnv1a64(const std::uint8_t* data, std::size_t len) {
  std::uint64_t x = 1469598103934665603ULL;
  constexpr std::uint64_t kPrime = 1099511628211ULL;
  for (std::size_t i = 0; i < len; ++i) {
    x ^= static_cast<std::uint64_t>(data[i]);
    x *= kPrime;
  }
  x ^= (x >> 33U);
  x *= 0xff51afd7ed558ccdULL;
  x ^= (x >> 33U);
  return x;
}

Fingerprint Hasher::HashChunk(const ChunkDesc& chunk) const {
  Fingerprint fp;
  fp.length = static_cast<std::uint32_t>(chunk.length);
  if (!chunk.data_owner || chunk.length == 0) {
    return fp;
  }
  const std::uint8_t* begin = chunk.data_owner->data() + chunk.offset;
  fp.hash = Fnv1a64(begin, chunk.length);
  return fp;
}

}  // namespace bx
