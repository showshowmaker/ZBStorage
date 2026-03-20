#include <cstdint>
#include <iostream>
#include <string>

#include "mds/masstree_meta/MasstreeBulkImporter.h"
#include "mds/masstree_meta/MasstreeIndexRuntime.h"

namespace {

std::string GetFlagValue(const std::string& arg, const std::string& name) {
    const std::string prefix = "--" + name + "=";
    return arg.rfind(prefix, 0) == 0 ? arg.substr(prefix.size()) : std::string();
}

bool ParseU32(const std::string& value, uint32_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint32_t>(std::stoul(value));
        return true;
    } catch (...) {
        return false;
    }
}

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  masstree_meta_import_tool"
        << " --manifest_path=<path>"
        << " [--verify_inode_samples=<u32>]"
        << " [--verify_dentry_samples=<u32>]\n";
}

} // namespace

int main(int argc, char** argv) {
    zb::mds::MasstreeBulkImporter::Request request;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (const std::string value = GetFlagValue(arg, "manifest_path"); !value.empty()) {
            request.manifest_path = value;
        } else if (const std::string value = GetFlagValue(arg, "verify_inode_samples"); !value.empty()) {
            if (!ParseU32(value, &request.verify_inode_samples)) {
                std::cerr << "invalid --verify_inode_samples\n";
                return 1;
            }
        } else if (const std::string value = GetFlagValue(arg, "verify_dentry_samples"); !value.empty()) {
            if (!ParseU32(value, &request.verify_dentry_samples)) {
                std::cerr << "invalid --verify_dentry_samples\n";
                return 1;
            }
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            return 0;
        } else {
            std::cerr << "unknown arg: " << arg << "\n";
            PrintUsage();
            return 1;
        }
    }

    if (request.manifest_path.empty()) {
        PrintUsage();
        return 1;
    }

    zb::mds::MasstreeIndexRuntime runtime;
    std::string init_error;
    if (!runtime.Init(&init_error)) {
        std::cerr << "runtime init failed: " << init_error << "\n";
        return 1;
    }

    zb::mds::MasstreeBulkImporter importer;
    zb::mds::MasstreeBulkImporter::Result result;
    std::string error;
    if (!importer.Import(request, &runtime, &result, &error)) {
        std::cerr << "import failed: " << error << "\n";
        return 1;
    }

    std::cout << "inode_imported=" << result.inode_imported << "\n";
    std::cout << "dentry_imported=" << result.dentry_imported << "\n";
    std::cout << "inode_pages_bytes=" << result.inode_pages_bytes << "\n";
    std::cout << "verified_inode_samples=" << result.verified_inode_samples << "\n";
    std::cout << "verified_dentry_samples=" << result.verified_dentry_samples << "\n";
    return 0;
}
