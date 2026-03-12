#pragma once

#include <cerrno>
#include <string>

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace zb::meta_gen {

inline std::string NormalizePath(std::string path) {
    for (char& c : path) {
        if (c == '\\') {
            c = '/';
        }
    }
    while (path.size() > 1 && path.back() == '/') {
        path.pop_back();
    }
    return path;
}

inline std::string JoinPath(const std::string& a, const std::string& b) {
    if (a.empty()) {
        return b;
    }
    if (b.empty()) {
        return a;
    }
    if (a.back() == '/' || a.back() == '\\') {
        return a + b;
    }
    return a + "/" + b;
}

inline bool MakeDirSingle(const std::string& path) {
    if (path.empty()) {
        return true;
    }
#ifdef _WIN32
    const int rc = _mkdir(path.c_str());
#else
    const int rc = mkdir(path.c_str(), 0755);
#endif
    return rc == 0 || errno == EEXIST;
}

inline bool EnsureDirRecursive(const std::string& raw_path, std::string* error) {
    const std::string path = NormalizePath(raw_path);
    if (path.empty()) {
        if (error) {
            *error = "empty directory path";
        }
        return false;
    }

    std::string current;
    size_t i = 0;
    if (path.size() >= 2 && path[1] == ':') {
        current = path.substr(0, 2);
        i = 2;
    } else if (path[0] == '/') {
        current = "/";
        i = 1;
    }

    while (i < path.size()) {
        while (i < path.size() && path[i] == '/') {
            ++i;
        }
        size_t j = i;
        while (j < path.size() && path[j] != '/') {
            ++j;
        }
        if (j == i) {
            break;
        }
        const std::string part = path.substr(i, j - i);
        if (!current.empty() && current.back() != '/') {
            current.push_back('/');
        }
        current += part;
        if (!MakeDirSingle(current)) {
            if (error) {
                *error = "failed to create directory: " + current + ", errno=" + std::to_string(errno);
            }
            return false;
        }
        i = j;
    }
    return true;
}

} // namespace zb::meta_gen
