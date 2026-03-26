#include "demo_logger.h"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;

namespace zb::demo {

namespace {

std::string CurrentTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now{};
#ifdef _WIN32
    localtime_s(&tm_now, &now_time);
#else
    localtime_r(&now_time, &tm_now);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string NormalizeBlock(const std::string& text) {
    if (text.empty()) {
        return {};
    }
    if (text.back() == '\n') {
        return text;
    }
    return text + '\n';
}

} // namespace

bool AppendRunLog(const std::string& log_file,
                  bool append_mode,
                  const DemoRunResult& result,
                  std::string* error) {
    if (log_file.empty()) {
        if (error) {
            *error = "log_file is empty";
        }
        return false;
    }

    fs::path path(log_file);
    if (path.has_parent_path()) {
        std::error_code ec;
        fs::create_directories(path.parent_path(), ec);
        if (ec) {
            if (error) {
                *error = "failed to create log directory: " + ec.message();
            }
            return false;
        }
    }

    const std::ios::openmode mode = std::ios::out | (append_mode ? std::ios::app : std::ios::trunc);
    std::ofstream out(path, mode);
    if (!out.is_open()) {
        if (error) {
            *error = "failed to open log file: " + path.string();
        }
        return false;
    }

    out << "==================================================\n";
    out << "timestamp=" << CurrentTimestamp() << '\n';
    out << "title=" << result.title << '\n';
    out << "command=" << result.command << '\n';
    out << "usage=" << result.usage << '\n';
    out << "result=" << (result.ok ? "PASS" : "FAIL") << '\n';
    out << "summary=" << result.summary << '\n';
    out << "[stdout]\n" << NormalizeBlock(result.raw_stdout);
    out << "[stderr]\n" << NormalizeBlock(result.raw_stderr);
    out << '\n';

    if (!out) {
        if (error) {
            *error = "failed to write log file: " + path.string();
        }
        return false;
    }

    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::demo
