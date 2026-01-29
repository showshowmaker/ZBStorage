#pragma once

#include <string>

namespace zb::msg {

enum class StatusCode : int {
    kOk = 0,
    kInvalidArgument = 1,
    kNotFound = 2,
    kIoError = 3,
    kInternalError = 4
};

struct Status {
    StatusCode code{StatusCode::kOk};
    std::string message;

    bool ok() const { return code == StatusCode::kOk; }

    static Status Ok() { return {}; }
    static Status InvalidArgument(std::string msg) {
        return {StatusCode::kInvalidArgument, std::move(msg)};
    }
    static Status NotFound(std::string msg) {
        return {StatusCode::kNotFound, std::move(msg)};
    }
    static Status IoError(std::string msg) {
        return {StatusCode::kIoError, std::move(msg)};
    }
    static Status InternalError(std::string msg) {
        return {StatusCode::kInternalError, std::move(msg)};
    }
};

} // namespace zb::msg
