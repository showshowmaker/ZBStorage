#pragma once

#include <string>

#include "demo_result.h"

namespace zb::demo {

bool AppendRunLog(const std::string& log_file,
                  bool append_mode,
                  const DemoRunResult& result,
                  std::string* error);

} // namespace zb::demo
