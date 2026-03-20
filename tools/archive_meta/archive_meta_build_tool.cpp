#include "mds/archive_meta/ArchiveImportService.h"
#include "mds/archive_meta/ArchiveNamespaceCatalog.h"
#include "mds/storage/RocksMetaStore.h"

#include <iostream>
#include <string>
#include <unordered_map>

namespace {

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  archive_meta_build_tool --db_path=<rocksdb>"
           " --path_prefix=<path> --output_dir=<dir>"
           " --namespace_id=<id> --generation_id=<id>"
           " [--page_size_bytes=65536] [--register_route=1]\n";
}

bool ParseArgs(int argc, char* argv[], std::unordered_map<std::string, std::string>* args) {
    if (!args) {
        return false;
    }
    for (int i = 1; i < argc; ++i) {
        std::string token = argv[i];
        if (token.rfind("--", 0) != 0) {
            continue;
        }
        const size_t eq = token.find('=');
        if (eq == std::string::npos) {
            (*args)[token.substr(2)] = "1";
        } else {
            (*args)[token.substr(2, eq - 2)] = token.substr(eq + 1);
        }
    }
    return true;
}

std::string GetArg(const std::unordered_map<std::string, std::string>& args, const std::string& key) {
    auto it = args.find(key);
    return it == args.end() ? std::string() : it->second;
}

} // namespace

int main(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> args;
    if (!ParseArgs(argc, argv, &args)) {
        PrintUsage();
        return 1;
    }

    const std::string db_path = GetArg(args, "db_path");
    const std::string path_prefix = GetArg(args, "path_prefix");
    const std::string output_dir = GetArg(args, "output_dir");
    const std::string namespace_id = GetArg(args, "namespace_id");
    const std::string generation_id = GetArg(args, "generation_id");
    if (db_path.empty() || path_prefix.empty() || output_dir.empty() || namespace_id.empty() || generation_id.empty()) {
        PrintUsage();
        return 1;
    }

    std::string error;
    zb::mds::RocksMetaStore store;
    if (!store.Open(db_path, &error)) {
        std::cerr << "failed to open db: " << error << "\n";
        return 1;
    }

    zb::mds::ArchiveNamespaceCatalog catalog(&store);
    zb::mds::ArchiveImportService importer(&store, &catalog);
    zb::mds::ArchiveImportService::Request request;
    request.archive_root = output_dir;
    request.namespace_id = namespace_id;
    request.generation_id = generation_id;
    request.path_prefix = path_prefix;
    request.publish_route = GetArg(args, "register_route") == "1";
    const std::string page_size = GetArg(args, "page_size_bytes");
    if (!page_size.empty()) {
        try {
            request.page_size_bytes = static_cast<uint32_t>(std::stoul(page_size));
        } catch (...) {
            std::cerr << "invalid page_size_bytes\n";
            return 1;
        }
    }

    zb::mds::ArchiveImportService::Result result;
    if (!importer.ImportPathPrefix(request, &result, &error)) {
        std::cerr << "build failed: " << error << "\n";
        return 1;
    }

    std::cout << "ok\n";
    std::cout << "manifest=" << result.manifest_path << "\n";
    std::cout << "inode_records=" << result.inode_count << "\n";
    std::cout << "dentry_records=" << result.dentry_count << "\n";
    std::cout << "inode_min=" << result.inode_min << "\n";
    std::cout << "inode_max=" << result.inode_max << "\n";
    return 0;
}
