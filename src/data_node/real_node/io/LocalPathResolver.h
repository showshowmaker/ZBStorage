#pragma once

#include <mutex>
#include <string>
#include <unordered_set>

namespace zb::real_node {

class LocalPathResolver {
public:
    std::string Resolve(const std::string& root_path,
                        const std::string& chunk_id,
                        bool create_parent_dirs = false);

private:
    std::string BuildPrefix(const std::string& chunk_id) const;
    bool EnsureDirectory(const std::string& dir_path);

    std::unordered_set<std::string> created_dirs_cache_;
    std::mutex cache_mu_;
};

} // namespace zb::real_node
