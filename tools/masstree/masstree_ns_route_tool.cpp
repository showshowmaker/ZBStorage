#include "mds/masstree_meta/MasstreeManifest.h"
#include "mds/masstree_meta/MasstreeNamespaceCatalog.h"
#include "mds/storage/MetaSchema.h"
#include "mds/storage/RocksMetaStore.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  masstree_ns_route_tool --db_path=<path> --get --path_prefix=<prefix>\n"
        << "  masstree_ns_route_tool --db_path=<path> --get_current --path_prefix=<prefix>\n"
        << "  masstree_ns_route_tool --db_path=<path> --put_manifest --manifest_path=<path>\n"
        << "  masstree_ns_route_tool --db_path=<path> --set_current_manifest --manifest_path=<path>\n"
        << "  masstree_ns_route_tool --db_path=<path> --delete --path_prefix=<prefix>\n";
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

zb::mds::MasstreeNamespaceRoute BuildRouteFromManifest(const zb::mds::MasstreeNamespaceManifest& manifest) {
    zb::mds::MasstreeNamespaceRoute route;
    route.namespace_id = manifest.namespace_id;
    route.path_prefix = manifest.path_prefix;
    route.generation_id = manifest.generation_id;
    route.manifest_path = manifest.manifest_path;
    route.root_inode_id = manifest.root_inode_id;
    route.inode_min = manifest.inode_min;
    route.inode_max = manifest.inode_max;
    route.inode_count = manifest.inode_count;
    route.dentry_count = manifest.dentry_count;
    return route;
}

std::vector<uint64_t> BuildBucketIds(const zb::mds::MasstreeNamespaceManifest& manifest) {
    std::vector<uint64_t> bucket_ids;
    if (manifest.inode_min == 0 || manifest.inode_max < manifest.inode_min) {
        return bucket_ids;
    }
    const uint64_t first_bucket = zb::mds::MasstreeInodeBucketId(manifest.inode_min);
    const uint64_t last_bucket = zb::mds::MasstreeInodeBucketId(manifest.inode_max);
    bucket_ids.reserve(static_cast<size_t>(last_bucket - first_bucket + 1U));
    for (uint64_t bucket_id = first_bucket; bucket_id <= last_bucket; ++bucket_id) {
        bucket_ids.push_back(bucket_id);
    }
    return bucket_ids;
}

void PrintRoute(const zb::mds::MasstreeNamespaceRoute& route) {
    std::cout << "namespace_id=" << route.namespace_id << "\n";
    std::cout << "path_prefix=" << route.path_prefix << "\n";
    std::cout << "generation_id=" << route.generation_id << "\n";
    std::cout << "manifest_path=" << route.manifest_path << "\n";
    std::cout << "root_inode_id=" << route.root_inode_id << "\n";
    std::cout << "inode_min=" << route.inode_min << "\n";
    std::cout << "inode_max=" << route.inode_max << "\n";
    std::cout << "inode_count=" << route.inode_count << "\n";
    std::cout << "dentry_count=" << route.dentry_count << "\n";
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
    const std::string manifest_path = GetArg(args, "manifest_path");
    const bool do_get = HasFlag(args, "get");
    const bool do_get_current = HasFlag(args, "get_current");
    const bool do_put_manifest = HasFlag(args, "put_manifest");
    const bool do_set_current_manifest = HasFlag(args, "set_current_manifest");
    const bool do_delete = HasFlag(args, "delete");
    if (db_path.empty()) {
        PrintUsage();
        return 1;
    }

    const int ops = static_cast<int>(do_get) + static_cast<int>(do_get_current) +
                    static_cast<int>(do_put_manifest) + static_cast<int>(do_set_current_manifest) +
                    static_cast<int>(do_delete);
    if (ops != 1) {
        PrintUsage();
        return 1;
    }
    if ((do_get || do_get_current || do_delete) && path_prefix.empty()) {
        PrintUsage();
        return 1;
    }
    if ((do_put_manifest || do_set_current_manifest) && manifest_path.empty()) {
        PrintUsage();
        return 1;
    }

    std::string error;
    zb::mds::RocksMetaStore store;
    if (!store.Open(db_path, &error)) {
        std::cerr << "failed to open db: " << error << "\n";
        return 1;
    }

    zb::mds::MasstreeNamespaceCatalog catalog(&store);

    if (do_get) {
        zb::mds::MasstreeNamespaceRoute route;
        if (!catalog.LookupByPath(path_prefix, &route, &error)) {
            if (!error.empty()) {
                std::cerr << "lookup failed: " << error << "\n";
                return 1;
            }
            std::cerr << "route not found\n";
            return 2;
        }
        PrintRoute(route);
        return 0;
    }

    if (do_get_current) {
        zb::mds::MasstreeNamespaceRoute route;
        if (!catalog.LookupCurrentRoute(path_prefix, &route, &error)) {
            if (!error.empty()) {
                std::cerr << "lookup current failed: " << error << "\n";
                return 1;
            }
            std::cerr << "current route not found\n";
            return 2;
        }
        PrintRoute(route);
        return 0;
    }

    if (do_put_manifest || do_set_current_manifest) {
        zb::mds::MasstreeNamespaceManifest manifest;
        if (!zb::mds::MasstreeNamespaceManifest::LoadFromFile(manifest_path, &manifest, &error)) {
            std::cerr << "load manifest failed: " << error << "\n";
            return 1;
        }
        zb::mds::MasstreeNamespaceRoute route = BuildRouteFromManifest(manifest);
        if (do_put_manifest) {
            const std::vector<uint64_t> bucket_ids = BuildBucketIds(manifest);
            if (!catalog.PutRoute(route, bucket_ids, &error)) {
                std::cerr << "put route failed: " << error << "\n";
                return 1;
            }
        } else if (!catalog.SetCurrentRoute(route, &error)) {
            std::cerr << "set current failed: " << error << "\n";
            return 1;
        }
        std::cout << "ok\n";
        PrintRoute(route);
        return 0;
    }

    if (!catalog.DeleteRoute(path_prefix, &error)) {
        std::cerr << "delete failed: " << error << "\n";
        return 1;
    }
    std::cout << "ok\n";
    return 0;
}
