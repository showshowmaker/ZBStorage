#include "MasstreeManifest.h"

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

bool ParseU64Field(const std::string& value, uint64_t* out) {
    if (!out) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

bool MasstreeNamespaceManifest::LoadFromFile(const std::string& manifest_path,
                                             MasstreeNamespaceManifest* manifest,
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
            *error = "failed to open masstree namespace manifest: " + manifest_path;
        }
        return false;
    }

    MasstreeNamespaceManifest parsed;
    parsed.manifest_path = manifest_path;
    std::string line;
    bool header_checked = false;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "masstree_namespace_manifest_v1") {
                if (error) {
                    *error = "invalid masstree namespace manifest header: " + manifest_path;
                }
                return false;
            }
            continue;
        }
        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "invalid masstree namespace manifest line " + std::to_string(line_no);
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
        } else if (key == "inode_records_path") {
            parsed.inode_records_path = value;
        } else if (key == "dentry_records_path") {
            parsed.dentry_records_path = value;
        } else if (key == "verify_manifest_path") {
            parsed.verify_manifest_path = value;
        } else if (key == "inode_blob_path") {
            parsed.inode_blob_path = value;
        } else if (key == "dentry_data_path") {
            parsed.dentry_data_path = value;
        } else if (key == "cluster_stats_path") {
            parsed.cluster_stats_path = value;
        } else if (key == "allocation_summary_path") {
            parsed.allocation_summary_path = value;
        } else if (key == "root_inode_id") {
            if (!ParseU64Field(value, &parsed.root_inode_id)) {
                if (error) {
                    *error = "invalid root_inode_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_min") {
            if (!ParseU64Field(value, &parsed.inode_min)) {
                if (error) {
                    *error = "invalid inode_min in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_max") {
            if (!ParseU64Field(value, &parsed.inode_max)) {
                if (error) {
                    *error = "invalid inode_max in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_count") {
            if (!ParseU64Field(value, &parsed.inode_count)) {
                if (error) {
                    *error = "invalid inode_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "dentry_count") {
            if (!ParseU64Field(value, &parsed.dentry_count)) {
                if (error) {
                    *error = "invalid dentry_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "file_count") {
            if (!ParseU64Field(value, &parsed.file_count)) {
                if (error) {
                    *error = "invalid file_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "level1_dir_count") {
            if (!ParseU64Field(value, &parsed.level1_dir_count)) {
                if (error) {
                    *error = "invalid level1_dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "leaf_dir_count") {
            if (!ParseU64Field(value, &parsed.leaf_dir_count)) {
                if (error) {
                    *error = "invalid leaf_dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_files_per_leaf_dir") {
            if (!ParseU64Field(value, &parsed.max_files_per_leaf_dir)) {
                if (error) {
                    *error = "invalid max_files_per_leaf_dir in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_subdirs_per_dir") {
            if (!ParseU64Field(value, &parsed.max_subdirs_per_dir)) {
                if (error) {
                    *error = "invalid max_subdirs_per_dir in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "min_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.min_file_size_bytes)) {
                if (error) {
                    *error = "invalid min_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.max_file_size_bytes)) {
                if (error) {
                    *error = "invalid max_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "avg_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.avg_file_size_bytes)) {
                if (error) {
                    *error = "invalid avg_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "total_file_bytes") {
            parsed.total_file_bytes = value;
        } else if (key == "start_global_image_id") {
            if (!ParseU64Field(value, &parsed.start_global_image_id)) {
                if (error) {
                    *error = "invalid start_global_image_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "end_global_image_id") {
            if (!ParseU64Field(value, &parsed.end_global_image_id)) {
                if (error) {
                    *error = "invalid end_global_image_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "start_cursor_node_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_node_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_node_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_disk_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_disk_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_disk_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_image_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_image_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_image_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_image_used_bytes") {
            if (!ParseU64Field(value, &parsed.start_cursor_image_used_bytes)) {
                if (error) {
                    *error = "invalid start_cursor_image_used_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "end_cursor_node_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_node_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_node_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_disk_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_disk_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_disk_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_image_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_image_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_image_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_image_used_bytes") {
            if (!ParseU64Field(value, &parsed.end_cursor_image_used_bytes)) {
                if (error) {
                    *error = "invalid end_cursor_image_used_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        }
    }

    if (!header_checked) {
        if (error) {
            *error = "empty masstree namespace manifest: " + manifest_path;
        }
        return false;
    }
    if (parsed.namespace_id.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing namespace_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.path_prefix.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing path_prefix: " + manifest_path;
        }
        return false;
    }
    if (parsed.generation_id.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing generation_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.root_inode_id == 0) {
        if (error) {
            *error = "masstree namespace manifest missing root_inode_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.inode_blob_path.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing inode_blob_path: " + manifest_path;
        }
        return false;
    }

    *manifest = std::move(parsed);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeNamespaceManifest::SaveToFile(const std::string& manifest_path, std::string* error) const {
    std::ofstream out(manifest_path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree namespace manifest: " + manifest_path;
        }
        return false;
    }

    out << "masstree_namespace_manifest_v1\n";
    out << "namespace_id=" << namespace_id << "\n";
    out << "path_prefix=" << path_prefix << "\n";
    out << "generation_id=" << generation_id << "\n";
    out << "inode_records_path=" << inode_records_path << "\n";
    out << "dentry_records_path=" << dentry_records_path << "\n";
    out << "verify_manifest_path=" << verify_manifest_path << "\n";
    out << "inode_blob_path=" << inode_blob_path << "\n";
    out << "dentry_data_path=" << dentry_data_path << "\n";
    out << "cluster_stats_path=" << cluster_stats_path << "\n";
    out << "allocation_summary_path=" << allocation_summary_path << "\n";
    out << "root_inode_id=" << root_inode_id << "\n";
    out << "inode_min=" << inode_min << "\n";
    out << "inode_max=" << inode_max << "\n";
    out << "inode_count=" << inode_count << "\n";
    out << "dentry_count=" << dentry_count << "\n";
    out << "file_count=" << file_count << "\n";
    out << "level1_dir_count=" << level1_dir_count << "\n";
    out << "leaf_dir_count=" << leaf_dir_count << "\n";
    out << "max_files_per_leaf_dir=" << max_files_per_leaf_dir << "\n";
    out << "max_subdirs_per_dir=" << max_subdirs_per_dir << "\n";
    out << "min_file_size_bytes=" << min_file_size_bytes << "\n";
    out << "max_file_size_bytes=" << max_file_size_bytes << "\n";
    out << "avg_file_size_bytes=" << avg_file_size_bytes << "\n";
    out << "total_file_bytes=" << total_file_bytes << "\n";
    out << "start_global_image_id=" << start_global_image_id << "\n";
    out << "end_global_image_id=" << end_global_image_id << "\n";
    out << "start_cursor_node_index=" << start_cursor_node_index << "\n";
    out << "start_cursor_disk_index=" << start_cursor_disk_index << "\n";
    out << "start_cursor_image_index=" << start_cursor_image_index << "\n";
    out << "start_cursor_image_used_bytes=" << start_cursor_image_used_bytes << "\n";
    out << "end_cursor_node_index=" << end_cursor_node_index << "\n";
    out << "end_cursor_disk_index=" << end_cursor_disk_index << "\n";
    out << "end_cursor_image_index=" << end_cursor_image_index << "\n";
    out << "end_cursor_image_used_bytes=" << end_cursor_image_used_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree namespace manifest: " + manifest_path;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
