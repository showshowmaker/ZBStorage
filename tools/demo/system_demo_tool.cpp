#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "mds.pb.h"
#include "scheduler.pb.h"

namespace fs = std::filesystem;

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(mount_point, "/mnt/zbstorage", "Mounted FUSE path");
DEFINE_string(real_dir, "real", "Top-level mounted directory for real-node files");
DEFINE_string(virtual_dir, "virtual", "Top-level mounted directory for virtual-node files");
DEFINE_string(scenario, "interactive", "Scenario: interactive|health|posix|masstree|masstree_import|masstree_query|all");
DEFINE_string(masstree_namespace_id, "demo-ns", "Masstree demo namespace id");
DEFINE_string(masstree_generation_id, "", "Masstree demo generation id; auto-generated if empty");
DEFINE_string(masstree_path_prefix, "", "Masstree demo path prefix; defaults to /masstree_demo/<namespace>");
DEFINE_uint64(masstree_file_count, 10000, "Masstree demo file count");
DEFINE_uint32(masstree_max_files_per_leaf_dir, 2048, "Masstree max files per leaf dir");
DEFINE_uint32(masstree_max_subdirs_per_dir, 256, "Masstree max subdirs per dir");
DEFINE_uint32(masstree_verify_inode_samples, 32, "Masstree import inode verify sample count");
DEFINE_uint32(masstree_verify_dentry_samples, 32, "Masstree import dentry verify sample count");
DEFINE_int32(timeout_ms, 5000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

std::string TimestampToken() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now{};
#ifdef _WIN32
    localtime_s(&tm_now, &now_time);
#else
    localtime_r(&now_time, &tm_now);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm_now, "%Y%m%d_%H%M%S");
    return oss.str();
}

std::string NormalizeLogicalPath(const std::string& path) {
    if (path.empty()) {
        return "/";
    }
    std::string normalized = path;
    std::replace(normalized.begin(), normalized.end(), '\\', '/');
    if (normalized.front() != '/') {
        normalized.insert(normalized.begin(), '/');
    }
    std::string out;
    out.reserve(normalized.size());
    bool prev_slash = false;
    for (char ch : normalized) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        out.push_back(ch);
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    return out.empty() ? "/" : out;
}

std::string JoinMountedPath(const std::string& mount_point, const std::string& logical_path) {
    fs::path mounted = fs::path(mount_point);
    std::string normalized = NormalizeLogicalPath(logical_path);
    if (normalized == "/") {
        return mounted.string();
    }
    return (mounted / normalized.substr(1)).string();
}

std::string BaseNodeIdFromVirtual(const std::string& node_id) {
    const size_t pos = node_id.rfind("-v");
    if (pos == std::string::npos || pos + 2 >= node_id.size()) {
        return node_id;
    }
    for (size_t i = pos + 2; i < node_id.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(node_id[i]))) {
            return node_id;
        }
    }
    return node_id.substr(0, pos);
}

std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream oss;
    if (unit == 0) {
        oss << static_cast<uint64_t>(value) << units[unit];
    } else {
        oss << std::fixed << std::setprecision(2) << value << units[unit];
    }
    return oss.str();
}

std::string NodeTypeName(zb::rpc::NodeType type) {
    switch (type) {
        case zb::rpc::NODE_REAL:
            return "real";
        case zb::rpc::NODE_VIRTUAL_POOL:
            return "virtual";
        case zb::rpc::NODE_OPTICAL:
            return "optical";
        default:
            return "unknown";
    }
}

void PrintSection(const std::string& title) {
    std::cout << "\n==== " << title << " ====\n";
}

class MdsClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool Lookup(const std::string& path, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::LookupRequest request;
        request.set_path(path);
        zb::rpc::LookupReply reply;
        brpc::Controller cntl;
        stub_.Lookup(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Getattr(uint64_t inode_id, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::GetattrRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetattrReply reply;
        brpc::Controller cntl;
        stub_.Getattr(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool GetFileLocation(uint64_t inode_id, zb::rpc::FileLocationView* view, zb::rpc::MdsStatus* status) {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller cntl;
        stub_.GetFileLocation(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (view) {
            *view = reply.location();
        }
        return true;
    }

    bool ImportMasstreeNamespace(const zb::rpc::ImportMasstreeNamespaceRequest& request,
                                 zb::rpc::ImportMasstreeNamespaceReply* reply_out) {
        zb::rpc::ImportMasstreeNamespaceReply reply;
        brpc::Controller cntl;
        stub_.ImportMasstreeNamespace(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetRandomMasstreeFileAttr(const zb::rpc::GetRandomMasstreeFileAttrRequest& request,
                                   zb::rpc::GetRandomMasstreeFileAttrReply* reply_out) {
        zb::rpc::GetRandomMasstreeFileAttrReply reply;
        brpc::Controller cntl;
        stub_.GetRandomMasstreeFileAttr(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetMasstreeClusterStats(zb::rpc::GetMasstreeClusterStatsReply* reply_out) {
        zb::rpc::GetMasstreeClusterStatsRequest request;
        zb::rpc::GetMasstreeClusterStatsReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeClusterStats(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

private:
    static void SetRpcFailureStatus(const brpc::Controller& cntl, zb::rpc::MdsStatus* status) {
        if (!status) {
            return;
        }
        status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status->set_message(cntl.ErrorText());
    }

    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class SchedulerClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool GetClusterView(std::vector<zb::rpc::NodeView>* nodes, uint64_t* generation, std::string* error) {
        zb::rpc::GetClusterViewRequest request;
        request.set_min_generation(0);
        zb::rpc::GetClusterViewReply reply;
        brpc::Controller cntl;
        stub_.GetClusterView(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (error) {
                *error = cntl.ErrorText();
            }
            return false;
        }
        if (reply.status().code() != zb::rpc::SCHED_OK) {
            if (error) {
                *error = reply.status().message();
            }
            return false;
        }
        if (nodes) {
            nodes->assign(reply.nodes().begin(), reply.nodes().end());
        }
        if (generation) {
            *generation = reply.generation();
        }
        return true;
    }

private:
    brpc::Channel channel_;
    zb::rpc::SchedulerService_Stub stub_{&channel_};
};

class DemoApp {
public:
    bool Init() {
        if (!mds_.Init(FLAGS_mds)) {
            std::cerr << "Failed to connect to MDS " << FLAGS_mds << '\n';
            return false;
        }
        if (!scheduler_.Init(FLAGS_scheduler)) {
            std::cerr << "Failed to connect to scheduler " << FLAGS_scheduler << '\n';
            return false;
        }
        return RefreshClusterView();
    }

    int Run() {
        const std::string scenario = FLAGS_scenario;
        if (scenario == "interactive") {
            return RunInteractive();
        }
        if (scenario == "health") {
            return RunHealthCheck() ? 0 : 1;
        }
        if (scenario == "posix") {
            return RunPosixSuite() ? 0 : 1;
        }
        if (scenario == "masstree") {
            return RunMasstreeSuite() ? 0 : 1;
        }
        if (scenario == "masstree_import") {
            return RunMasstreeImportDemo() ? 0 : 1;
        }
        if (scenario == "masstree_query") {
            return RunMasstreeQueryDemo() ? 0 : 1;
        }
        if (scenario == "all") {
            const bool ok = RunHealthCheck() && RunPosixSuite() && RunMasstreeSuite();
            return ok ? 0 : 1;
        }
        std::cerr << "Unknown --scenario=" << scenario << '\n';
        return 1;
    }

private:
    bool RefreshClusterView() {
        std::string error;
        if (!scheduler_.GetClusterView(&nodes_, &cluster_generation_, &error)) {
            std::cerr << "GetClusterView failed: " << error << '\n';
            return false;
        }
        node_type_by_id_.clear();
        for (const auto& node : nodes_) {
            node_type_by_id_[node.node_id()] = node.node_type();
        }
        return true;
    }

    bool RunHealthCheck() {
        PrintSection("Health Check");
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup("/", &attr, &status)) {
            std::cerr << "MDS root lookup failed: " << status.message() << '\n';
            return false;
        }
        std::cout << "mds_root_inode=" << attr.inode_id() << '\n';
        std::cout << "scheduler_generation=" << cluster_generation_ << '\n';
        std::cout << "cluster_nodes=" << nodes_.size() << '\n';

        const std::string real_root = BuildTierLogicalPath(FLAGS_real_dir);
        const std::string virtual_root = BuildTierLogicalPath(FLAGS_virtual_dir);
        if (!CheckTierDirectory(real_root)) {
            return false;
        }
        if (!CheckTierDirectory(virtual_root)) {
            return false;
        }
        std::cout << "mount_point=" << FLAGS_mount_point << '\n';
        std::cout << "real_root=" << real_root << '\n';
        std::cout << "virtual_root=" << virtual_root << '\n';
        return true;
    }

    bool RunPosixSuite() {
        const bool real_ok = RunTierFileDemo(FLAGS_real_dir, "real", &last_real_logical_path_);
        const bool virtual_ok = RunTierFileDemo(FLAGS_virtual_dir, "virtual", &last_virtual_logical_path_);
        return real_ok && virtual_ok;
    }

    bool RunMasstreeSuite() {
        return RunMasstreeImportDemo() && RunMasstreeQueryDemo();
    }

    int RunInteractive() {
        for (;;) {
            PrintMenu();
            const std::string choice = PromptLine("choice");
            if (choice == "q" || choice == "quit") {
                return 0;
            }
            bool ok = false;
            if (choice == "1") {
                ok = RunHealthCheck();
            } else if (choice == "2") {
                ok = RunTierFileDemo(FLAGS_real_dir, "real", &last_real_logical_path_);
            } else if (choice == "3") {
                ok = RunTierFileDemo(FLAGS_virtual_dir, "virtual", &last_virtual_logical_path_);
            } else if (choice == "4") {
                ok = InspectKnownFile(last_real_logical_path_, "last real");
            } else if (choice == "5") {
                ok = InspectKnownFile(last_virtual_logical_path_, "last virtual");
            } else if (choice == "6") {
                ok = RunMasstreeImportDemo();
            } else if (choice == "7") {
                ok = RunMasstreeQueryDemo();
            } else if (choice == "8") {
                ok = RunHealthCheck() && RunPosixSuite() && RunMasstreeSuite();
            } else {
                std::cout << "unknown choice\n";
                continue;
            }
            std::cout << (ok ? "result=OK\n" : "result=FAILED\n");
        }
    }

    bool CheckTierDirectory(const std::string& logical_path) {
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup(logical_path, &attr, &status)) {
            std::cerr << "Lookup failed for " << logical_path << ": " << status.message() << '\n';
            return false;
        }
        if (attr.type() != zb::rpc::INODE_DIR) {
            std::cerr << logical_path << " exists but is not a directory\n";
            return false;
        }
        return true;
    }

    bool RunTierFileDemo(const std::string& dir_name,
                         const std::string& expected_tier,
                         std::string* saved_logical_path) {
        const std::string token = TimestampToken();
        const std::string logical_dir = BuildTierLogicalPath(dir_name) + "/demo";
        const std::string logical_path = logical_dir + "/demo_" + token + ".txt";
        const std::string mounted_dir = JoinMountedPath(FLAGS_mount_point, logical_dir);
        const std::string mounted_path = JoinMountedPath(FLAGS_mount_point, logical_path);
        const std::string content = "demo tier=" + expected_tier + " token=" + token + "\n";

        PrintSection("POSIX " + expected_tier + " Demo");
        std::error_code ec;
        fs::create_directories(mounted_dir, ec);
        if (ec) {
            std::cerr << "create_directories failed for " << mounted_dir << ": " << ec.message() << '\n';
            return false;
        }
        if (!WriteTextFile(mounted_path, content)) {
            return false;
        }
        std::string read_back;
        if (!ReadTextFile(mounted_path, &read_back)) {
            return false;
        }
        std::cout << "logical_path=" << logical_path << '\n';
        std::cout << "mounted_path=" << mounted_path << '\n';
        std::cout << "bytes_written=" << content.size() << '\n';
        std::cout << "bytes_read=" << read_back.size() << '\n';
        if (read_back != content) {
            std::cerr << "POSIX content mismatch\n";
            return false;
        }
        if (!InspectFile(logical_path, expected_tier)) {
            return false;
        }
        if (saved_logical_path) {
            *saved_logical_path = logical_path;
        }
        return true;
    }

    bool InspectKnownFile(const std::string& logical_path, const std::string& label) {
        if (logical_path.empty()) {
            std::cerr << "no " << label << " file recorded yet\n";
            return false;
        }
        PrintSection("Inspect " + label + " File");
        return InspectFile(logical_path, "");
    }

    bool InspectFile(const std::string& logical_path, const std::string& expected_tier) {
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup(logical_path, &attr, &status)) {
            std::cerr << "Lookup failed for " << logical_path << ": " << status.message() << '\n';
            return false;
        }
        zb::rpc::FileLocationView view;
        if (!mds_.GetFileLocation(attr.inode_id(), &view, &status)) {
            std::cerr << "GetFileLocation failed for inode " << attr.inode_id() << ": " << status.message() << '\n';
            return false;
        }
        const std::string node_id = view.disk_location().node_id();
        const std::string disk_id = view.disk_location().disk_id();
        const std::string base_node_id = BaseNodeIdFromVirtual(node_id);
        auto it = node_type_by_id_.find(base_node_id);
        std::string actual_tier = "unknown";
        if (it != node_type_by_id_.end()) {
            actual_tier = NodeTypeName(it->second);
        } else if (node_id != base_node_id) {
            actual_tier = "virtual";
        }

        std::cout << "inode_id=" << attr.inode_id() << '\n';
        std::cout << "size=" << attr.size() << " (" << FormatBytes(attr.size()) << ")\n";
        std::cout << "node_id=" << node_id << '\n';
        std::cout << "disk_id=" << disk_id << '\n';
        std::cout << "resolved_node_type=" << actual_tier << '\n';
        if (!expected_tier.empty() && actual_tier != expected_tier) {
            std::cerr << "expected tier " << expected_tier << " but got " << actual_tier << '\n';
            return false;
        }
        return true;
    }

    bool RunMasstreeImportDemo() {
        PrintSection("Masstree Import Demo");
        const std::string namespace_id = FLAGS_masstree_namespace_id.empty()
                                             ? "demo-ns"
                                             : FLAGS_masstree_namespace_id;
        const std::string generation_id = FLAGS_masstree_generation_id.empty()
                                              ? "gen-" + TimestampToken()
                                              : FLAGS_masstree_generation_id;
        const std::string path_prefix = FLAGS_masstree_path_prefix.empty()
                                            ? "/masstree_demo/" + namespace_id
                                            : NormalizeLogicalPath(FLAGS_masstree_path_prefix);

        zb::rpc::ImportMasstreeNamespaceRequest request;
        request.set_namespace_id(namespace_id);
        request.set_generation_id(generation_id);
        request.set_path_prefix(path_prefix);
        request.set_file_count(FLAGS_masstree_file_count);
        request.set_max_files_per_leaf_dir(FLAGS_masstree_max_files_per_leaf_dir);
        request.set_max_subdirs_per_dir(FLAGS_masstree_max_subdirs_per_dir);
        request.set_verify_inode_samples(FLAGS_masstree_verify_inode_samples);
        request.set_verify_dentry_samples(FLAGS_masstree_verify_dentry_samples);
        request.set_publish_route(true);

        zb::rpc::ImportMasstreeNamespaceReply reply;
        if (!mds_.ImportMasstreeNamespace(request, &reply)) {
            std::cerr << "ImportMasstreeNamespace failed: " << reply.status().message() << '\n';
            return false;
        }
        last_masstree_namespace_id_ = namespace_id;
        last_masstree_path_prefix_ = path_prefix;
        last_masstree_generation_id_ = generation_id;
        std::cout << "namespace_id=" << namespace_id << '\n';
        std::cout << "generation_id=" << generation_id << '\n';
        std::cout << "path_prefix=" << path_prefix << '\n';
        std::cout << "manifest_path=" << reply.manifest_path() << '\n';
        std::cout << "root_inode_id=" << reply.root_inode_id() << '\n';
        std::cout << "inode_count=" << reply.inode_count() << '\n';
        std::cout << "dentry_count=" << reply.dentry_count() << '\n';
        std::cout << "inode_range=[" << reply.inode_min() << ", " << reply.inode_max() << "]\n";
        return true;
    }

    bool RunMasstreeQueryDemo() {
        PrintSection("Masstree Query Demo");
        if (last_masstree_namespace_id_.empty()) {
            last_masstree_namespace_id_ = FLAGS_masstree_namespace_id;
        }
        if (last_masstree_path_prefix_.empty()) {
            last_masstree_path_prefix_ = FLAGS_masstree_path_prefix.empty()
                                             ? "/masstree_demo/" + FLAGS_masstree_namespace_id
                                             : NormalizeLogicalPath(FLAGS_masstree_path_prefix);
        }

        zb::rpc::GetRandomMasstreeFileAttrRequest attr_request;
        attr_request.set_namespace_id(last_masstree_namespace_id_);
        attr_request.set_path_prefix(last_masstree_path_prefix_);
        zb::rpc::GetRandomMasstreeFileAttrReply attr_reply;
        if (!mds_.GetRandomMasstreeFileAttr(attr_request, &attr_reply)) {
            std::cerr << "GetRandomMasstreeFileAttr failed: " << attr_reply.status().message() << '\n';
            return false;
        }
        std::cout << "namespace_id=" << attr_reply.namespace_id() << '\n';
        std::cout << "path_prefix=" << attr_reply.path_prefix() << '\n';
        std::cout << "generation_id=" << attr_reply.generation_id() << '\n';
        std::cout << "inode_id=" << attr_reply.inode_id() << '\n';
        std::cout << "random_attr_size=" << attr_reply.attr().size()
                  << " (" << FormatBytes(attr_reply.attr().size()) << ")\n";

        zb::rpc::GetMasstreeClusterStatsReply stats_reply;
        if (!mds_.GetMasstreeClusterStats(&stats_reply)) {
            std::cerr << "GetMasstreeClusterStats failed: " << stats_reply.status().message() << '\n';
            return false;
        }
        std::cout << "optical_node_count=" << stats_reply.optical_node_count() << '\n';
        std::cout << "optical_device_count=" << stats_reply.optical_device_count() << '\n';
        std::cout << "total_file_count=" << stats_reply.total_file_count() << '\n';
        std::cout << "total_file_bytes=" << stats_reply.total_file_bytes() << '\n';
        std::cout << "avg_file_size_bytes=" << stats_reply.avg_file_size_bytes() << '\n';
        std::cout << "used_capacity_bytes=" << stats_reply.used_capacity_bytes() << '\n';
        return true;
    }

    static bool WriteTextFile(const std::string& path, const std::string& content) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            std::cerr << "failed to open for write: " << path << '\n';
            return false;
        }
        out.write(content.data(), static_cast<std::streamsize>(content.size()));
        out.close();
        if (!out) {
            std::cerr << "write failed: " << path << '\n';
            return false;
        }
        return true;
    }

    static bool ReadTextFile(const std::string& path, std::string* out) {
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            std::cerr << "failed to open for read: " << path << '\n';
            return false;
        }
        std::ostringstream buffer;
        buffer << in.rdbuf();
        if (out) {
            *out = buffer.str();
        }
        return true;
    }

    static std::string BuildTierLogicalPath(const std::string& dir_name) {
        return NormalizeLogicalPath("/" + dir_name);
    }

    static std::string PromptLine(const std::string& label) {
        std::cout << label << "> " << std::flush;
        std::string line;
        std::getline(std::cin, line);
        return line;
    }

    void PrintMenu() const {
        std::cout << "\n==== System Demo Menu ====\n";
        std::cout << "1) Check health\n";
        std::cout << "2) Run real POSIX demo\n";
        std::cout << "3) Run virtual POSIX demo\n";
        std::cout << "4) Inspect last real file via RPC\n";
        std::cout << "5) Inspect last virtual file via RPC\n";
        std::cout << "6) Import masstree namespace\n";
        std::cout << "7) Query masstree\n";
        std::cout << "8) Run all\n";
        std::cout << "q) Quit\n";
    }

    MdsClient mds_;
    SchedulerClient scheduler_;
    std::vector<zb::rpc::NodeView> nodes_;
    std::unordered_map<std::string, zb::rpc::NodeType> node_type_by_id_;
    uint64_t cluster_generation_{0};
    std::string last_real_logical_path_;
    std::string last_virtual_logical_path_;
    std::string last_masstree_namespace_id_;
    std::string last_masstree_generation_id_;
    std::string last_masstree_path_prefix_;
};

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    DemoApp app;
    if (!app.Init()) {
        return 1;
    }
    return app.Run();
}
