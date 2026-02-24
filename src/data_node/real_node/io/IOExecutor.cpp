#include "IOExecutor.h"

#include <filesystem>
#include <fstream>

namespace zb::real_node {

zb::msg::Status IOExecutor::Write(const std::string& path,
                                 uint64_t offset,
                                 const std::string& data,
                                 uint64_t* bytes_written) {
    if (path.empty()) {
        return zb::msg::Status::InvalidArgument("Empty path");
    }
    if (bytes_written) {
        *bytes_written = 0;
    }

    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        file.clear();
        file.open(path, std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            return zb::msg::Status::IoError("Failed to create file: " + path);
        }
        file.close();
        file.open(path, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            return zb::msg::Status::IoError("Failed to open file: " + path);
        }
    }

    file.seekp(static_cast<std::streamoff>(offset));
    if (!file.good()) {
        return zb::msg::Status::IoError("Failed to seek file: " + path);
    }

    file.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!file.good()) {
        return zb::msg::Status::IoError("Failed to write file: " + path);
    }

    if (bytes_written) {
        *bytes_written = data.size();
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status IOExecutor::Read(const std::string& path,
                                uint64_t offset,
                                uint64_t size,
                                std::string* out,
                                uint64_t* bytes_read) {
    if (path.empty()) {
        return zb::msg::Status::InvalidArgument("Empty path");
    }
    if (!out) {
        return zb::msg::Status::InvalidArgument("Output buffer is null");
    }
    if (bytes_read) {
        *bytes_read = 0;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return zb::msg::Status::NotFound("File not found: " + path);
    }

    file.seekg(static_cast<std::streamoff>(offset));
    if (!file.good()) {
        return zb::msg::Status::IoError("Failed to seek file: " + path);
    }

    out->assign(size, '\0');
    file.read(out->data(), static_cast<std::streamsize>(size));
    std::streamsize read_bytes = file.gcount();
    if (read_bytes < 0) {
        return zb::msg::Status::IoError("Failed to read file: " + path);
    }
    out->resize(static_cast<size_t>(read_bytes));
    if (bytes_read) {
        *bytes_read = static_cast<uint64_t>(read_bytes);
    }

    return zb::msg::Status::Ok();
}

zb::msg::Status IOExecutor::Delete(const std::string& path) {
    if (path.empty()) {
        return zb::msg::Status::InvalidArgument("Empty path");
    }
    std::error_code ec;
    bool removed = std::filesystem::remove(path, ec);
    if (ec) {
        return zb::msg::Status::IoError("Failed to delete file: " + path + ", error=" + ec.message());
    }
    if (!removed) {
        return zb::msg::Status::NotFound("File not found: " + path);
    }
    return zb::msg::Status::Ok();
}

} // namespace zb::real_node
