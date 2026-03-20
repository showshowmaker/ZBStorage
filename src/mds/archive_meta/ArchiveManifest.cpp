#include "ArchiveManifest.h"

#include <algorithm>
#include <cctype>
#include <fstream>

namespace zb::mds {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

} // namespace

bool ArchiveManifest::LoadFromFile(const std::string& manifest_path,
                                   ArchiveManifest* manifest,
                                   std::string* error) {
    if (!manifest) {
        if (error) {
            *error = "manifest output is null";
        }
        return false;
    }

    std::ifstream input(manifest_path);
    if (!input) {
        if (error) {
            *error = "failed to open archive manifest: " + manifest_path;
        }
        return false;
    }

    ArchiveManifest parsed;
    parsed.manifest_path = manifest_path;

    std::string line;
    size_t line_no = 0;
    bool header_checked = false;
    while (std::getline(input, line)) {
        ++line_no;
        std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "archive_meta_manifest_v1") {
                if (error) {
                    *error = "invalid archive manifest header: " + manifest_path;
                }
                return false;
            }
            continue;
        }
        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "invalid archive manifest line " + std::to_string(line_no);
            }
            return false;
        }
        const std::string key = Trim(trimmed.substr(0, eq));
        const std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "namespace_id") {
            parsed.namespace_id = value;
        } else if (key == "path_prefix") {
            parsed.path_prefix = value;
        } else if (key == "generation_id") {
            parsed.generation_id = value;
        } else if (key == "inode_root") {
            parsed.inode_root = value;
        } else if (key == "inode_index_root") {
            parsed.inode_index_root = value;
        } else if (key == "inode_bloom_root") {
            parsed.inode_bloom_root = value;
        } else if (key == "dentry_root") {
            parsed.dentry_root = value;
        } else if (key == "dentry_index_root") {
            parsed.dentry_index_root = value;
        } else if (key == "root_inode_id") {
            try {
                parsed.root_inode_id = static_cast<uint64_t>(std::stoull(value));
            } catch (...) {
                if (error) {
                    *error = "invalid root_inode_id in archive manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_min") {
            try {
                parsed.inode_min = static_cast<uint64_t>(std::stoull(value));
            } catch (...) {
                if (error) {
                    *error = "invalid inode_min in archive manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_max") {
            try {
                parsed.inode_max = static_cast<uint64_t>(std::stoull(value));
            } catch (...) {
                if (error) {
                    *error = "invalid inode_max in archive manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_count") {
            try {
                parsed.inode_count = static_cast<uint64_t>(std::stoull(value));
            } catch (...) {
                if (error) {
                    *error = "invalid inode_count in archive manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "page_size_bytes") {
            try {
                parsed.page_size_bytes = static_cast<uint64_t>(std::stoull(value));
            } catch (...) {
                if (error) {
                    *error = "invalid page_size_bytes in archive manifest: " + manifest_path;
                }
                return false;
            }
        }
    }

    if (!header_checked) {
        if (error) {
            *error = "empty archive manifest: " + manifest_path;
        }
        return false;
    }
    if (parsed.namespace_id.empty()) {
        if (error) {
            *error = "archive manifest missing namespace_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.path_prefix.empty()) {
        if (error) {
            *error = "archive manifest missing path_prefix: " + manifest_path;
        }
        return false;
    }
    if (parsed.generation_id.empty()) {
        if (error) {
            *error = "archive manifest missing generation_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.inode_root.empty()) {
        if (error) {
            *error = "archive manifest missing inode_root: " + manifest_path;
        }
        return false;
    }
    if (parsed.dentry_root.empty()) {
        if (error) {
            *error = "archive manifest missing dentry_root: " + manifest_path;
        }
        return false;
    }
    if (parsed.root_inode_id == 0) {
        if (error) {
            *error = "archive manifest missing root_inode_id: " + manifest_path;
        }
        return false;
    }

    *manifest = std::move(parsed);
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
