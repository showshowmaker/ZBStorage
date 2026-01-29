#include "LocalPathResolver.h"

#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

namespace zb::real_node {

std::string LocalPathResolver::Resolve(const std::string& root_path,
                                       const std::string& chunk_id,
                                       bool create_parent_dirs) {
    if (root_path.empty() || chunk_id.empty()) {
        return {};
    }

    std::string prefix = BuildPrefix(chunk_id);
    std::string level1 = prefix.substr(0, 2);
    std::string level2 = prefix.substr(2, 2);

    fs::path dir_path = fs::path(root_path) / level1 / level2;
    if (create_parent_dirs) {
        if (!EnsureDirectory(dir_path.string())) {
            return {};
        }
    }

    fs::path file_path = dir_path / chunk_id;
    return file_path.string();
}

std::string LocalPathResolver::BuildPrefix(const std::string& chunk_id) const {
    std::string prefix;
    prefix.reserve(4);

    for (char ch : chunk_id) {
        if (prefix.size() >= 4) {
            break;
        }
        if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
            if (ch >= 'A' && ch <= 'F') {
                prefix.push_back(static_cast<char>(ch - 'A' + 'a'));
            } else {
                prefix.push_back(ch);
            }
        }
    }

    while (prefix.size() < 4) {
        prefix.push_back('0');
    }

    return prefix;
}

bool LocalPathResolver::EnsureDirectory(const std::string& dir_path) {
    {
        std::lock_guard<std::mutex> lock(cache_mu_);
        if (created_dirs_cache_.find(dir_path) != created_dirs_cache_.end()) {
            return true;
        }
    }

    std::error_code ec;
    if (!fs::create_directories(dir_path, ec) && ec) {
        return false;
    }

    std::lock_guard<std::mutex> lock(cache_mu_);
    created_dirs_cache_.insert(dir_path);
    return true;
}

} // namespace zb::real_node
