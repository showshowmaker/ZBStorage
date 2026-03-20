#include "mds/archive_meta/ArchiveNamespaceCatalog.h"
#include "mds/archive_meta/ArchiveGenerationPublisher.h"
#include "mds/storage/RocksMetaStore.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  archive_ns_route_tool --db_path=<path> --get --path_prefix=<prefix>\n"
        << "  archive_ns_route_tool --db_path=<path> --get_current --path_prefix=<prefix>\n"
        << "  archive_ns_route_tool --db_path=<path> --put --path_prefix=<prefix>"
           " --namespace_id=<id> --generation_id=<id> [--manifest_path=<path>]"
           " [--inode_min=<id>] [--inode_max=<id>] [--inode_count=<n>]\n"
        << "  archive_ns_route_tool --db_path=<path> --set_current --path_prefix=<prefix>"
           " --generation_id=<id> [--manifest_path=<path>] [--namespace_id=<id>]\n"
        << "  archive_ns_route_tool --db_path=<path> --archive_root=<dir> --namespace_id=<id> --list_generations\n"
        << "  archive_ns_route_tool --db_path=<path> --archive_root=<dir> --namespace_id=<id> --recover_current\n"
        << "  archive_ns_route_tool --db_path=<path> --archive_root=<dir> --namespace_id=<id> --cleanup_staging\n"
        << "  archive_ns_route_tool --db_path=<path> --archive_root=<dir> --namespace_id=<id> --prune_generations"
           " [--keep_last=<n>] [--path_prefix=<prefix>]\n"
        << "  archive_ns_route_tool --db_path=<path> --delete --path_prefix=<prefix>\n";
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

bool HasFlag(const std::unordered_map<std::string, std::string>& args, const std::string& key) {
    return args.find(key) != args.end();
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
    const std::string archive_root = GetArg(args, "archive_root");
    const std::string namespace_id = GetArg(args, "namespace_id");
    const bool do_get = HasFlag(args, "get");
    const bool do_get_current = HasFlag(args, "get_current");
    const bool do_put = HasFlag(args, "put");
    const bool do_set_current = HasFlag(args, "set_current");
    const bool do_list_generations = HasFlag(args, "list_generations");
    const bool do_recover_current = HasFlag(args, "recover_current");
    const bool do_cleanup_staging = HasFlag(args, "cleanup_staging");
    const bool do_prune_generations = HasFlag(args, "prune_generations");
    const bool do_delete = HasFlag(args, "delete");
    if (db_path.empty()) {
        PrintUsage();
        return 1;
    }
    const int ops = static_cast<int>(do_get) + static_cast<int>(do_get_current) +
                    static_cast<int>(do_put) + static_cast<int>(do_set_current) +
                    static_cast<int>(do_list_generations) + static_cast<int>(do_recover_current) +
                    static_cast<int>(do_cleanup_staging) + static_cast<int>(do_prune_generations) +
                    static_cast<int>(do_delete);
    if (ops != 1) {
        PrintUsage();
        return 1;
    }
    if ((do_get || do_get_current || do_put || do_set_current || do_delete) && path_prefix.empty()) {
        PrintUsage();
        return 1;
    }
    if ((do_list_generations || do_recover_current || do_cleanup_staging || do_prune_generations) &&
        (archive_root.empty() || namespace_id.empty())) {
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
    zb::mds::ArchiveGenerationPublisher publisher(&catalog);

    if (do_get) {
        zb::mds::ArchiveNamespaceRoute route;
        if (!catalog.LookupByPath(path_prefix, &route, &error)) {
            if (!error.empty()) {
                std::cerr << "lookup failed: " << error << "\n";
                return 1;
            }
            std::cerr << "route not found\n";
            return 2;
        }
        std::cout << "namespace_id=" << route.namespace_id << "\n";
        std::cout << "path_prefix=" << route.path_prefix << "\n";
        std::cout << "generation_id=" << route.generation_id << "\n";
        std::cout << "manifest_path=" << route.manifest_path << "\n";
        std::cout << "inode_min=" << route.inode_min << "\n";
        std::cout << "inode_max=" << route.inode_max << "\n";
        std::cout << "inode_count=" << route.inode_count << "\n";
        return 0;
    }

    if (do_get_current) {
        zb::mds::ArchiveNamespaceRoute route;
        if (!catalog.LookupCurrentRoute(path_prefix, &route, &error)) {
            if (!error.empty()) {
                std::cerr << "lookup current failed: " << error << "\n";
                return 1;
            }
            std::cerr << "current route not found\n";
            return 2;
        }
        std::cout << "namespace_id=" << route.namespace_id << "\n";
        std::cout << "path_prefix=" << route.path_prefix << "\n";
        std::cout << "generation_id=" << route.generation_id << "\n";
        std::cout << "manifest_path=" << route.manifest_path << "\n";
        std::cout << "inode_min=" << route.inode_min << "\n";
        std::cout << "inode_max=" << route.inode_max << "\n";
        std::cout << "inode_count=" << route.inode_count << "\n";
        return 0;
    }

    if (do_put) {
        zb::mds::ArchiveNamespaceRoute route;
        route.namespace_id = GetArg(args, "namespace_id");
        route.path_prefix = path_prefix;
        route.generation_id = GetArg(args, "generation_id");
        route.manifest_path = GetArg(args, "manifest_path");
        const std::string inode_min = GetArg(args, "inode_min");
        const std::string inode_max = GetArg(args, "inode_max");
        const std::string inode_count = GetArg(args, "inode_count");
        if (route.namespace_id.empty() || route.generation_id.empty()) {
            PrintUsage();
            return 1;
        }
        try {
            if (!inode_min.empty()) {
                route.inode_min = static_cast<uint64_t>(std::stoull(inode_min));
            }
            if (!inode_max.empty()) {
                route.inode_max = static_cast<uint64_t>(std::stoull(inode_max));
            }
            if (!inode_count.empty()) {
                route.inode_count = static_cast<uint64_t>(std::stoull(inode_count));
            }
        } catch (...) {
            std::cerr << "invalid inode_min/inode_max/inode_count\n";
            return 1;
        }
        if (!catalog.PutRoute(route, &error)) {
            std::cerr << "put failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        return 0;
    }

    if (do_set_current) {
        zb::mds::ArchiveNamespaceRoute current;
        zb::mds::ArchiveNamespaceRoute existing;
        if (catalog.LookupByPath(path_prefix, &existing, &error)) {
            current = existing;
        } else if (!error.empty()) {
            std::cerr << "lookup existing failed: " << error << "\n";
            return 1;
        }
        current.path_prefix = path_prefix;
        const std::string namespace_id = GetArg(args, "namespace_id");
        if (!namespace_id.empty()) {
            current.namespace_id = namespace_id;
        }
        current.generation_id = GetArg(args, "generation_id");
        current.manifest_path = GetArg(args, "manifest_path");
        if (current.generation_id.empty()) {
            PrintUsage();
            return 1;
        }
        if (!catalog.SetCurrentRoute(current, &error)) {
            std::cerr << "set current failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        return 0;
    }

    if (do_list_generations) {
        std::vector<std::string> generations;
        if (!publisher.ListPublishedGenerations(archive_root, namespace_id, &generations, &error)) {
            std::cerr << "list generations failed: " << error << "\n";
            return 1;
        }
        for (const auto& generation : generations) {
            std::cout << generation << "\n";
        }
        return 0;
    }

    if (do_recover_current) {
        zb::mds::ArchiveNamespaceRoute route;
        if (!publisher.RecoverCurrentRouteFromLatest(archive_root, namespace_id, &route, &error)) {
            std::cerr << "recover current failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        std::cout << "namespace_id=" << route.namespace_id << "\n";
        std::cout << "path_prefix=" << route.path_prefix << "\n";
        std::cout << "generation_id=" << route.generation_id << "\n";
        std::cout << "manifest_path=" << route.manifest_path << "\n";
        return 0;
    }

    if (do_cleanup_staging) {
        std::vector<std::string> removed_paths;
        if (!publisher.CleanupNamespaceStaging(archive_root, namespace_id, &removed_paths, &error)) {
            std::cerr << "cleanup staging failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        for (const auto& removed : removed_paths) {
            std::cout << "removed=" << removed << "\n";
        }
        return 0;
    }

    if (do_prune_generations) {
        size_t keep_last = 1;
        const std::string keep_last_arg = GetArg(args, "keep_last");
        if (!keep_last_arg.empty()) {
            try {
                keep_last = static_cast<size_t>(std::stoull(keep_last_arg));
            } catch (...) {
                std::cerr << "invalid keep_last\n";
                return 1;
            }
        }
        std::string keep_generation_id;
        if (!path_prefix.empty()) {
            zb::mds::ArchiveNamespaceRoute current;
            std::string current_error;
            if (catalog.LookupCurrentRoute(path_prefix, &current, &current_error)) {
                keep_generation_id = current.generation_id;
            } else if (!current_error.empty()) {
                std::cerr << "lookup current failed: " << current_error << "\n";
                return 1;
            }
        }
        std::vector<std::string> removed_generations;
        if (!publisher.PruneOldGenerations(archive_root,
                                           namespace_id,
                                           keep_last,
                                           keep_generation_id,
                                           &removed_generations,
                                           &error)) {
            std::cerr << "prune generations failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        for (const auto& generation : removed_generations) {
            std::cout << "removed=" << generation << "\n";
        }
        return 0;
    }

    if (!catalog.DeleteRoute(path_prefix, &error)) {
        std::cerr << "delete failed: " << error << "\n";
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}
