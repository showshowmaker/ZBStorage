#pragma once

#include <vector>

#include "bx/config.h"
#include "bx/types.h"

namespace bx {

std::vector<FileTask> BuildDataset(const Config& config);

}  // namespace bx
