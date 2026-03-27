#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cerrno>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstring>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "demo_menu.h"
#include "demo_logger.h"
#include "demo_params.h"
#include "demo_result.h"
#include "mds/masstree_meta/MasstreeDecimalUtils.h"
#include "mds.pb.h"
#include "scheduler.pb.h"

namespace fs = std::filesystem;

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(mount_point, "/mnt/zbstorage", "Mounted FUSE path");
DEFINE_string(real_dir, "real", "Top-level mounted directory for real-node files");
DEFINE_string(virtual_dir, "virtual", "Top-level mounted directory for virtual-node files");
DEFINE_string(scenario,
              "interactive",
              "Scenario: interactive|health|stats|posix|masstree|masstree_import|masstree_query|all");
DEFINE_string(masstree_namespace_id, "demo-ns", "Masstree demo namespace id");
DEFINE_string(masstree_generation_id, "", "Masstree demo generation id; auto-generated if empty");
DEFINE_string(masstree_path_prefix, "", "Masstree demo path prefix; defaults to /masstree_demo/<namespace>");
DEFINE_string(masstree_template_id, "", "Masstree import template id; empty disables template reuse");
DEFINE_string(masstree_template_mode,
              "",
              "Masstree import template mode: empty|page_fast|legacy_records; empty preserves the current fast path");
DEFINE_uint64(masstree_file_count, 100000000, "Masstree demo file count");
DEFINE_uint32(masstree_max_files_per_leaf_dir, 2048, "Masstree max files per leaf dir");
DEFINE_uint32(masstree_max_subdirs_per_dir, 256, "Masstree max subdirs per dir");
DEFINE_uint32(masstree_verify_inode_samples, 32, "Masstree import inode verify sample count");
DEFINE_uint32(masstree_verify_dentry_samples, 32, "Masstree import dentry verify sample count");
DEFINE_uint32(masstree_job_poll_interval_ms, 1000, "Masstree import job poll interval in ms");
DEFINE_uint32(masstree_query_samples, 1, "Masstree query sample count");
DEFINE_uint64(posix_file_size_mb, 100, "POSIX tier demo file size in MiB");
DEFINE_uint32(posix_chunk_size_kb, 1024, "POSIX tier demo chunk size in KiB");
DEFINE_uint32(posix_repeat, 1, "POSIX tier demo repeat count");
DEFINE_bool(posix_keep_file, true, "Keep generated POSIX tier demo files");
DEFINE_bool(posix_verify_hash, true, "Verify read-back hash for POSIX tier demos");
DEFINE_bool(posix_sync_on_close, false, "Flush file contents before closing POSIX tier demo files");
DEFINE_uint64(tc_p1_expected_real_node_count, 0, "TC-P1 expected real node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_virtual_node_count, 0, "TC-P1 expected virtual logical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_node_count, 0, "TC-P1 expected online logical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_disks_per_node,
              0,
              "TC-P1 expected online disks per logical node; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_disk_capacity_bytes,
              0,
              "TC-P1 expected per-disk online capacity in bytes; 0 disables this check");
DEFINE_uint64(tc_p1_expected_optical_node_count, 10000, "TC-P1 expected optical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_optical_device_count,
              100000000,
              "TC-P1 expected optical device count; 0 disables this check");
DEFINE_string(tc_p1_expected_cold_total_capacity_bytes,
              "",
              "TC-P1 expected cold total capacity in decimal bytes; empty disables this check");
DEFINE_string(tc_p1_expected_cold_used_capacity_bytes,
              "",
              "TC-P1 expected cold used capacity in decimal bytes; empty disables this check");
DEFINE_string(tc_p1_expected_cold_free_capacity_bytes,
              "",
              "TC-P1 expected cold free capacity in decimal bytes; empty disables this check");
DEFINE_uint64(tc_p1_expected_total_file_count, 0, "TC-P1 expected total file count; 0 disables this check");
DEFINE_string(tc_p1_expected_total_file_bytes,
              "",
              "TC-P1 expected total file bytes in decimal form; empty disables this check");
DEFINE_string(tc_p1_expected_total_metadata_bytes,
              "",
              "TC-P1 expected total metadata bytes in decimal form; empty disables this check");
DEFINE_uint64(tc_p1_expected_min_file_size_bytes,
              500000000ULL,
              "TC-P1 expected minimum generated file size; 0 disables this check");
DEFINE_uint64(tc_p1_expected_max_file_size_bytes,
              1500000000ULL,
              "TC-P1 expected maximum generated file size; 0 disables this check");
DEFINE_string(log_file, "logs/system_demo.log", "Demo execution log file path");
DEFINE_bool(enable_log_file, true, "Write demo command output to a log file");
DEFINE_bool(log_append, true, "Append demo execution logs instead of overwriting the file");
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

std::string FormatDurationSeconds(uint64_t seconds) {
    const uint64_t hours = seconds / 3600ULL;
    const uint64_t minutes = (seconds % 3600ULL) / 60ULL;
    const uint64_t secs = seconds % 60ULL;
    std::ostringstream oss;
    if (hours != 0) {
        oss << hours << "h";
    }
    if (hours != 0 || minutes != 0) {
        if (hours != 0) {
            oss << " ";
        }
        oss << minutes << "m";
    }
    if (hours != 0 || minutes != 0) {
        oss << " ";
    }
    oss << secs << "s";
    return oss.str();
}

const char* MasstreeJobStateName(zb::rpc::MasstreeImportJobState state) {
    switch (state) {
    case zb::rpc::MASSTREE_IMPORT_JOB_PENDING:
        return "???";
    case zb::rpc::MASSTREE_IMPORT_JOB_RUNNING:
        return "???";
    case zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED:
        return "???";
    case zb::rpc::MASSTREE_IMPORT_JOB_FAILED:
        return "??";
    default:
        return "??";
    }
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

std::string DisplayTierName(const std::string& tier) {
    if (tier == "real") {
        return "真实";
    }
    if (tier == "virtual") {
        return "虚拟";
    }
    if (tier == "optical") {
        return "光盘";
    }
    return tier.empty() ? "未知" : tier;
}

void PrintSection(const std::string& title) {
    std::cout << "\n==== " << title << " ====\n";
}

std::string FormatDecimalBytes(const std::string& bytes) {
    return zb::mds::NormalizeDecimalString(bytes);
}

std::string FormatDecimalBytesWithHuman(const std::string& bytes) {
    const std::string normalized = zb::mds::NormalizeDecimalString(bytes);
    long double value = 0.0L;
    for (char ch : normalized) {
        if (ch < '0' || ch > '9') {
            continue;
        }
        value = value * 10.0L + static_cast<long double>(ch - '0');
    }
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    size_t unit = 0;
    while (value >= 1024.0L && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0L;
        ++unit;
    }
    std::ostringstream human;
    if (unit == 0) {
        human << normalized << "B";
    } else {
        human << std::fixed << std::setprecision(2) << static_cast<double>(value) << units[unit];
    }
    return normalized + " (" + human.str() + ")";
}

struct TierStats {
    uint64_t physical_node_count{0};
    uint64_t logical_node_count{0};
    uint64_t disk_count{0};
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    uint64_t free_capacity_bytes{0};
};

struct TierIoDiagnostics {
    bool has_host_space{false};
    uint64_t host_capacity_bytes{0};
    uint64_t host_available_bytes{0};
    TierStats real_stats;
    TierStats virtual_stats;
};

struct CheckResult {
    std::string name;
    bool ok{false};
    std::string detail;
};

struct TierIoOptions {
    std::string dir_name;
    std::string expected_tier;
    uint64_t file_size_bytes{100ULL * 1024ULL * 1024ULL};
    uint32_t chunk_size_bytes{1024U * 1024U};
    uint32_t repeat{1};
    bool keep_file{true};
    bool verify_hash{true};
    bool sync_on_close{false};
};

struct FileInspectionResult {
    uint64_t inode_id{0};
    uint64_t size_bytes{0};
    std::string node_id;
    std::string disk_id;
    std::string actual_tier{"unknown"};
};

struct TierIoIterationResult {
    std::string logical_path;
    std::string mounted_path;
    uint64_t expected_size_bytes{0};
    uint64_t bytes_written{0};
    uint64_t bytes_read{0};
    uint64_t write_elapsed_us{0};
    uint64_t read_elapsed_us{0};
    uint64_t write_hash{0};
    uint64_t read_hash{0};
    FileInspectionResult inspection;
};

void PrintByteMetric(const std::string& key, uint64_t value) {
    std::cout << key << "=" << value << " (" << FormatBytes(value) << ")\n";
}

void PrintDecimalMetric(const std::string& key, const std::string& value) {
    std::cout << key << "=" << FormatDecimalBytesWithHuman(value) << '\n';
}

void PrintBoolMetric(const std::string& key, bool value) {
    std::cout << key << "=" << (value ? "是" : "否") << '\n';
}

void CollectTierStats(const std::vector<zb::rpc::NodeView>& nodes, TierStats* real_stats, TierStats* virtual_stats) {
    if (!real_stats || !virtual_stats) {
        return;
    }
    *real_stats = TierStats{};
    *virtual_stats = TierStats{};
    for (const auto& node : nodes) {
        TierStats* target = nullptr;
        uint64_t fanout = 1;
        if (node.node_type() == zb::rpc::NODE_REAL) {
            target = real_stats;
        } else if (node.node_type() == zb::rpc::NODE_VIRTUAL_POOL) {
            target = virtual_stats;
            fanout = std::max<uint64_t>(1, node.virtual_node_count());
        } else {
            continue;
        }
        target->physical_node_count += 1;
        target->logical_node_count += fanout;
        target->disk_count += static_cast<uint64_t>(node.disks_size()) * fanout;

        uint64_t node_total = 0;
        uint64_t node_free = 0;
        for (const auto& disk : node.disks()) {
            node_total += disk.capacity_bytes();
            node_free += disk.free_bytes();
        }
        target->total_capacity_bytes += node_total * fanout;
        target->free_capacity_bytes += node_free * fanout;
    }
    real_stats->used_capacity_bytes = real_stats->total_capacity_bytes > real_stats->free_capacity_bytes
                                          ? (real_stats->total_capacity_bytes - real_stats->free_capacity_bytes)
                                          : 0;
    virtual_stats->used_capacity_bytes = virtual_stats->total_capacity_bytes > virtual_stats->free_capacity_bytes
                                             ? (virtual_stats->total_capacity_bytes - virtual_stats->free_capacity_bytes)
                                             : 0;
}

void AddCheck(std::vector<CheckResult>* checks,
              const std::string& name,
              bool ok,
              const std::string& detail) {
    if (!checks) {
        return;
    }
    checks->push_back(CheckResult{name, ok, detail});
}

bool ParseUint64Value(const std::string& name,
                      const std::string& value,
                      uint64_t* out,
                      std::string* error) {
    if (!out) {
        if (error) {
            *error = "null output for " + name;
        }
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        if (error) {
            error->clear();
        }
        return true;
    } catch (...) {
        if (error) {
            *error = "invalid uint64 for " + name + ": " + value;
        }
        return false;
    }
}

bool ParseUint32Value(const std::string& name,
                      const std::string& value,
                      uint32_t* out,
                      std::string* error) {
    uint64_t parsed = 0;
    if (!ParseUint64Value(name, value, &parsed, error)) {
        return false;
    }
    if (parsed > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "value too large for uint32 " + name + ": " + value;
        }
        return false;
    }
    if (out) {
        *out = static_cast<uint32_t>(parsed);
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string ToLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool ParseBoolValue(const std::string& name,
                    const std::string& value,
                    bool* out,
                    std::string* error) {
    if (!out) {
        if (error) {
            *error = "null output for " + name;
        }
        return false;
    }
    const std::string normalized = ToLowerCopy(value);
    if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on") {
        *out = true;
    } else if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off") {
        *out = false;
    } else {
        if (error) {
            *error = "invalid bool for " + name + ": " + value;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

uint64_t Fnv1a64Append(uint64_t hash, const char* data, size_t size) {
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= kFnvPrime;
    }
    return hash;
}

std::string FormatHex64(uint64_t value) {
    std::ostringstream oss;
    oss << "0x" << std::hex << std::setw(16) << std::setfill('0') << std::nouppercase << value;
    return oss.str();
}

std::string FormatDouble(double value, int precision = 2) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

const char* InodeTypeToString(zb::rpc::InodeType type) {
    switch (type) {
    case zb::rpc::INODE_FILE:
        return "文件";
    case zb::rpc::INODE_DIR:
        return "目录";
    default:
        return "未知";
    }
}

const char* ArchiveStateToString(zb::rpc::InodeArchiveState state) {
    switch (state) {
    case zb::rpc::INODE_ARCHIVE_PENDING:
        return "待归档";
    case zb::rpc::INODE_ARCHIVE_ARCHIVING:
        return "归档中";
    case zb::rpc::INODE_ARCHIVE_ARCHIVED:
        return "已归档";
    default:
        return "未知";
    }
}

double ThroughputMiBS(uint64_t bytes, uint64_t elapsed_us) {
    if (elapsed_us == 0) {
        return 0.0;
    }
    const double seconds = static_cast<double>(elapsed_us) / 1000000.0;
    return static_cast<double>(bytes) / (1024.0 * 1024.0) / seconds;
}

std::string FormatSignedDecimalDelta(const std::string& after, const std::string& before) {
    const int cmp = zb::mds::CompareDecimalStrings(after, before);
    if (cmp == 0) {
        return "0";
    }
    if (cmp > 0) {
        return zb::mds::SubtractDecimalStrings(after, before);
    }
    return "-" + zb::mds::SubtractDecimalStrings(before, after);
}

std::string FormatSignedUint64Delta(uint64_t after, uint64_t before) {
    if (after >= before) {
        return std::to_string(after - before);
    }
    return "-" + std::to_string(before - after);
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

    bool GetMasstreeNamespaceStats(const std::string& namespace_id,
                                   zb::rpc::GetMasstreeNamespaceStatsReply* reply_out) {
        zb::rpc::GetMasstreeNamespaceStatsRequest request;
        request.set_namespace_id(namespace_id);
        zb::rpc::GetMasstreeNamespaceStatsReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeNamespaceStats(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed();
    }

    bool GetMasstreeImportJob(const std::string& job_id,
                              zb::rpc::GetMasstreeImportJobReply* reply_out) {
        zb::rpc::GetMasstreeImportJobRequest request;
        request.set_job_id(job_id);
        zb::rpc::GetMasstreeImportJobReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeImportJob(&cntl, &request, &reply, nullptr);
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
        InitializeMenuActions();
        return RefreshClusterView();
    }

    int Run() {
        const std::string scenario = FLAGS_scenario;
        if (scenario == "interactive") {
            return RunInteractive();
        }
        if (scenario == "health") {
            return RunScenarioCommand("health", "环境健康检查", "环境健康检查通过", "环境健康检查失败",
                                      [&]() { return RunHealthCheck(); });
        }
        if (scenario == "stats") {
            return RunScenarioCommand("stats", "TC-P1 全局统计", "TC-P1 统计校验通过", "TC-P1 统计校验失败",
                                      [&]() { return RunStatsScenario(); });
        }
        if (scenario == "posix") {
            return RunScenarioCommand("posix", "POSIX 在线层测试", "POSIX 在线层测试通过", "POSIX 在线层测试失败",
                                      [&]() { return RunPosixSuite(); });
        }
        if (scenario == "masstree") {
            return RunScenarioCommand("masstree",
                                      "Masstree 测试集",
                                      "Masstree 测试集通过",
                                      "Masstree 测试集失败",
                                      [&]() { return RunMasstreeSuite(); });
        }
        if (scenario == "masstree_import") {
            return RunScenarioCommand("masstree_import",
                                      "TC-P4 Masstree 导入",
                                      "Masstree 导入完成",
                                      "Masstree 导入失败",
                                      [&]() { return RunMasstreeImportDemo(); });
        }
        if (scenario == "masstree_query") {
            return RunScenarioCommand("masstree_query",
                                      "TC-P5 Masstree 查询",
                                      "Masstree 查询完成",
                                      "Masstree 查询失败",
                                      [&]() { return RunMasstreeQueryDemo(); });
        }
        if (scenario == "all") {
            return RunScenarioCommand("all",
                                      "完整测试集",
                                      "完整测试集通过",
                                      "完整测试集失败",
                                      [&]() {
                                          return RunHealthCheck() && RunStatsScenario() && RunPosixSuite() &&
                                                 RunMasstreeSuite();
                                      });
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
        PrintSection("环境健康检查");
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup("/", &attr, &status)) {
            std::cerr << "查询 MDS 根目录失败: " << status.message() << '\n';
            return false;
        }
        std::cout << "MDS根inode=" << attr.inode_id() << '\n';
        std::cout << "调度代次=" << cluster_generation_ << '\n';
        std::cout << "集群节点数=" << nodes_.size() << '\n';

        const std::string real_root = BuildTierLogicalPath(FLAGS_real_dir);
        const std::string virtual_root = BuildTierLogicalPath(FLAGS_virtual_dir);
        if (!CheckTierDirectory(real_root)) {
            return false;
        }
        if (!CheckTierDirectory(virtual_root)) {
            return false;
        }
        std::cout << "挂载点=" << FLAGS_mount_point << '\n';
        std::cout << "真实层根目录=" << real_root << '\n';
        std::cout << "虚拟层根目录=" << virtual_root << '\n';
        return true;
    }

    bool RunPosixSuite() {
        const bool real_ok = RunTierFileDemo(FLAGS_real_dir, "real", &last_real_logical_path_);
        const bool virtual_ok = RunTierFileDemo(FLAGS_virtual_dir, "virtual", &last_virtual_logical_path_);
        return real_ok && virtual_ok;
    }

    bool RunStatsScenario() {
        PrintSection("TC-P1 ????");
        if (!RefreshClusterView()) {
            return false;
        }

        TierStats real_stats;
        TierStats virtual_stats;
        CollectTierStats(nodes_, &real_stats, &virtual_stats);

        zb::rpc::GetMasstreeClusterStatsReply masstree_stats;
        if (!mds_.GetMasstreeClusterStats(&masstree_stats)) {
            std::cerr << "?? Masstree ??????: " << masstree_stats.status().message() << '\n';
            return false;
        }

        const uint64_t online_logical_node_count = real_stats.logical_node_count + virtual_stats.logical_node_count;
        std::cout << "????????=" << real_stats.physical_node_count << '\n';
        std::cout << "????????=" << real_stats.logical_node_count << '\n';
        std::cout << "??????=" << real_stats.disk_count << '\n';
        PrintByteMetric("????????", real_stats.total_capacity_bytes);
        PrintByteMetric("?????????", real_stats.used_capacity_bytes);
        PrintByteMetric("?????????", real_stats.free_capacity_bytes);

        std::cout << "????????=" << virtual_stats.logical_node_count << '\n';
        std::cout << "??????=" << virtual_stats.disk_count << '\n';
        PrintByteMetric("????????", virtual_stats.total_capacity_bytes);
        PrintByteMetric("?????????", virtual_stats.used_capacity_bytes);
        PrintByteMetric("?????????", virtual_stats.free_capacity_bytes);

        std::cout << "???????=" << online_logical_node_count << '\n';
        std::cout << "?????=" << masstree_stats.optical_node_count() << '\n';
        std::cout << "?????=" << masstree_stats.optical_device_count() << '\n';
        PrintDecimalMetric("???????", masstree_stats.total_capacity_bytes());
        PrintDecimalMetric("????????", masstree_stats.used_capacity_bytes());
        PrintDecimalMetric("????????", masstree_stats.free_capacity_bytes());
        std::cout << "????=" << masstree_stats.total_file_count() << '\n';
        PrintDecimalMetric("??????", masstree_stats.total_file_bytes());
        std::cout << "????????=" << masstree_stats.avg_file_size_bytes()
                  << " (" << FormatBytes(masstree_stats.avg_file_size_bytes()) << ")\n";
        PrintDecimalMetric("???????", masstree_stats.total_metadata_bytes());
        std::cout << "????????=" << masstree_stats.min_file_size_bytes()
                  << " (" << FormatBytes(masstree_stats.min_file_size_bytes()) << ")\n";
        std::cout << "????????=" << masstree_stats.max_file_size_bytes()
                  << " (" << FormatBytes(masstree_stats.max_file_size_bytes()) << ")\n";
        return true;
    }

    bool RunMasstreeSuite() {
        return RunMasstreeImportDemo() && RunMasstreeQueryDemo();
    }

    void InitializeMenuActions() {
        if (!actions_.empty()) {
            return;
        }
        actions_.push_back({"1", "环境健康检查", "检查 MDS、Scheduler 与 tier 根目录", "1", {"health"}});
        actions_.push_back({"2", "TC-P1 全局统计", "执行节点、容量、文件和元数据统计", "2 [key=value ...]", {"stats", "p1"}});
        actions_.push_back({"3", "TC-P2 真实节点读写", "向真实节点路径写入并回读测试文件", "3 [dir=<real_dir>]", {"real", "p2"}});
        actions_.push_back({"4", "TC-P3 虚拟节点读写", "向虚拟节点路径写入并回读测试文件", "4 [dir=<virtual_dir>]", {"virtual", "p3"}});
        actions_.push_back({"5",
                            "TC-P4 Masstree 导入",
                            "执行 Masstree namespace 批量导入",
	                            "5 namespace=<id> generation=<id> file_count=<n> [template_id=<id>] [template_mode=<mode>] [key=value ...]",
                            {"import", "p4"}});
        actions_.push_back({"6",
                            "TC-P5 Masstree 查询",
                            "执行随机元数据查询并输出统计",
                            "6 namespace=<id> [n=<count>] [path_prefix=<path>]",
                            {"query", "p5"}});
        actions_.push_back({"7", "执行完整测试集", "按顺序执行健康检查、P1、P2、P3、P4、P5", "7", {"all"}});
        actions_.push_back({"8", "查看上次结果", "重新展示最近一次测试结果", "8", {"last"}});
        actions_.push_back({"9", "帮助", "显示菜单和参数示例", "9", {"help", "h"}});
        actions_.push_back({"0", "退出", "退出测试控制台", "0", {"quit", "exit", "q"}});
    }

    zb::demo::DemoRunResult BuildInfoResult(const std::string& title,
                                            bool ok,
                                            const std::string& summary,
                                            const std::string& usage,
                                            const std::string& raw_stdout = std::string(),
                                            const std::string& raw_stderr = std::string()) const {
        return zb::demo::BuildResultFromOutput(title,
                                               std::string(),
                                               usage,
                                               ok,
                                               summary,
                                               summary,
                                               raw_stdout,
                                               raw_stderr);
    }

    zb::demo::DemoRunResult ExecuteCapturedAction(const zb::demo::MenuActionSpec& action,
                                                  const std::string& command,
                                                  const std::string& success_summary,
                                                  const std::string& failure_summary,
                                                  const std::function<bool()>& fn) {
        zb::demo::ScopedStreamCapture capture;
        const bool ok = fn();
        return zb::demo::BuildResultFromOutput(action.title,
                                               command,
                                               action.usage,
                                               ok,
                                               success_summary,
                                               failure_summary,
                                               capture.Stdout(),
                                               capture.Stderr());
    }

    void MaybeAppendLog(const zb::demo::DemoRunResult& result) const {
        if (!FLAGS_enable_log_file || result.title.empty()) {
            return;
        }
        std::string error;
        if (!zb::demo::AppendRunLog(FLAGS_log_file, FLAGS_log_append, result, &error)) {
            std::cerr << "log_write_error=" << error << '\n';
        }
    }

    int RunScenarioCommand(const std::string& command,
                           const std::string& title,
                           const std::string& success_summary,
                           const std::string& failure_summary,
                           const std::function<bool()>& fn) {
        bool ok = false;
        std::string stdout_text;
        std::string stderr_text;
        {
            zb::demo::ScopedStreamCapture capture;
            ok = fn();
            stdout_text = capture.Stdout();
            stderr_text = capture.Stderr();
        }
        zb::demo::DemoRunResult result = zb::demo::BuildResultFromOutput(title,
                                                                         command,
                                                                         command,
                                                                         ok,
                                                                         success_summary,
                                                                         failure_summary,
                                                                         stdout_text,
                                                                         stderr_text);
        if (!result.raw_stdout.empty()) {
            std::cout << result.raw_stdout;
            if (result.raw_stdout.back() != '\n') {
                std::cout << '\n';
            }
        }
        if (!result.raw_stderr.empty()) {
            std::cerr << result.raw_stderr;
            if (result.raw_stderr.back() != '\n') {
                std::cerr << '\n';
            }
        }
        MaybeAppendLog(result);
        return result.ok ? 0 : 1;
    }

    std::string BuildHelpText() const {
        std::ostringstream out;
        out << "功能列表:\n";
        for (const auto& action : actions_) {
            out << "  " << action.id << "  " << action.title;
            if (!action.description.empty()) {
                out << " - " << action.description;
            }
            out << "\n     用法: " << action.usage << '\n';
        }
        out << "\n示例:\n";
        out << "  2 tc_p1_expected_real_node_count=1 tc_p1_expected_virtual_node_count=99\n";
	        out << "  5 namespace=demo-ns generation=gen-report-001 file_count=100000000 template_id=template-100m-v1 template_mode=legacy_records\n";
        out << "  6 namespace=demo-ns n=1000\n";
        out << "  6 namespace=demo-ns n=1000 log_file=logs/p5_run.log\n";
        return out.str();
    }

    bool ApplyCommandArgs(const zb::demo::ParsedCommand& command, std::string* error) {
        for (const auto& item : command.args) {
            const std::string& key = item.first;
            const std::string& value = item.second;
            uint64_t parsed_u64 = 0;
            uint32_t parsed_u32 = 0;

            if (key == "mds") {
                FLAGS_mds = value;
            } else if (key == "scheduler") {
                FLAGS_scheduler = value;
            } else if (key == "log_file") {
                FLAGS_log_file = value;
            } else if (key == "enable_log_file") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_enable_log_file = parsed_bool;
            } else if (key == "log_append") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_log_append = parsed_bool;
            } else if (key == "mount" || key == "mount_point") {
                FLAGS_mount_point = value;
            } else if (key == "dir") {
                continue;
            } else if (key == "real_dir" || key == "real_root") {
                FLAGS_real_dir = value;
            } else if (key == "virtual_dir" || key == "virtual_root") {
                FLAGS_virtual_dir = value;
            } else if (key == "namespace" || key == "masstree_namespace_id") {
                FLAGS_masstree_namespace_id = value;
            } else if (key == "generation" || key == "masstree_generation_id") {
                FLAGS_masstree_generation_id = value;
	            } else if (key == "path_prefix" || key == "masstree_path_prefix") {
	                FLAGS_masstree_path_prefix = value;
	            } else if (key == "template_id" || key == "masstree_template_id") {
	                FLAGS_masstree_template_id = value;
	            } else if (key == "template_mode" || key == "masstree_template_mode") {
	                FLAGS_masstree_template_mode = value;
	            } else if (key == "file_count" || key == "masstree_file_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_file_count = parsed_u64;
            } else if (key == "max_files_per_leaf_dir" || key == "masstree_max_files_per_leaf_dir") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_max_files_per_leaf_dir = parsed_u32;
            } else if (key == "max_subdirs_per_dir" || key == "masstree_max_subdirs_per_dir") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_max_subdirs_per_dir = parsed_u32;
            } else if (key == "verify_inode_samples" || key == "masstree_verify_inode_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_verify_inode_samples = parsed_u32;
            } else if (key == "verify_dentry_samples" || key == "masstree_verify_dentry_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_verify_dentry_samples = parsed_u32;
            } else if (key == "job_poll_interval_ms" || key == "masstree_job_poll_interval_ms") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_job_poll_interval_ms = parsed_u32;
            } else if (key == "n" || key == "samples" || key == "masstree_query_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_query_samples = parsed_u32;
            } else if (key == "file_size_mb" || key == "posix_file_size_mb") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_posix_file_size_mb = parsed_u64;
            } else if (key == "chunk_size_kb" || key == "posix_chunk_size_kb") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_posix_chunk_size_kb = parsed_u32;
            } else if (key == "repeat" || key == "posix_repeat") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_posix_repeat = parsed_u32;
            } else if (key == "keep_file" || key == "posix_keep_file") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_keep_file = parsed_bool;
            } else if (key == "verify_hash" || key == "posix_verify_hash") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_verify_hash = parsed_bool;
            } else if (key == "sync_on_close" || key == "posix_sync_on_close") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_sync_on_close = parsed_bool;
            } else if (key == "tc_p1_expected_real_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_real_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_virtual_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_virtual_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_online_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_online_disks_per_node") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_disks_per_node = parsed_u64;
            } else if (key == "tc_p1_expected_online_disk_capacity_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_disk_capacity_bytes = parsed_u64;
            } else if (key == "tc_p1_expected_optical_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_optical_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_optical_device_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_optical_device_count = parsed_u64;
            } else if (key == "tc_p1_expected_cold_total_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_total_capacity_bytes = value;
            } else if (key == "tc_p1_expected_cold_used_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_used_capacity_bytes = value;
            } else if (key == "tc_p1_expected_cold_free_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_free_capacity_bytes = value;
            } else if (key == "tc_p1_expected_total_file_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_total_file_count = parsed_u64;
            } else if (key == "tc_p1_expected_total_file_bytes") {
                FLAGS_tc_p1_expected_total_file_bytes = value;
            } else if (key == "tc_p1_expected_total_metadata_bytes") {
                FLAGS_tc_p1_expected_total_metadata_bytes = value;
            } else if (key == "tc_p1_expected_min_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_min_file_size_bytes = parsed_u64;
            } else if (key == "tc_p1_expected_max_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_max_file_size_bytes = parsed_u64;
            } else {
                if (error) {
                    *error = "unknown parameter: " + key;
                }
                return false;
            }
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    zb::demo::DemoRunResult ExecuteInteractiveCommand(const zb::demo::ParsedCommand& command, bool* should_exit) {
        if (should_exit) {
            *should_exit = false;
        }
        const zb::demo::MenuActionSpec* action = zb::demo::FindAction(actions_, command.action);
        if (!action) {
            return BuildInfoResult("未知命令", false, "不支持的序号或命令: " + command.action, "9");
        }
        if (action->id == "0") {
            if (should_exit) {
                *should_exit = true;
            }
            return {};
        }
        if (action->id == "9") {
            return BuildInfoResult("帮助", true, "可用功能与输入格式如下", action->usage, BuildHelpText());
        }
        if (action->id == "8") {
            if (!last_result_.has_value()) {
                return BuildInfoResult("上次结果", false, "当前没有可展示的历史结果", action->usage);
            }
            zb::demo::DemoRunResult replay = *last_result_;
            replay.title = "上次结果 - " + replay.title;
            replay.summary = "重新展示最近一次执行结果";
            return replay;
        }

        std::string apply_error;
        if (!ApplyCommandArgs(command, &apply_error)) {
            return BuildInfoResult(action->title, false, apply_error, action->usage);
        }

        if (action->id == "1") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "环境健康检查通过",
                                         "环境健康检查失败",
                                         [&]() { return RunHealthCheck(); });
        }
        if (action->id == "2") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "TC-P1 统计校验通过",
                                         "TC-P1 统计校验失败",
                                         [&]() { return RunStatsScenario(); });
        }
        if (action->id == "3") {
            const std::string dir = command.args.count("dir") != 0 ? command.args.at("dir") : FLAGS_real_dir;
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "真实节点读写测试通过",
                                         "真实节点读写测试失败",
                                         [&]() { return RunTierFileDemo(dir, "real", &last_real_logical_path_); });
        }
        if (action->id == "4") {
            const std::string dir = command.args.count("dir") != 0 ? command.args.at("dir") : FLAGS_virtual_dir;
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "虚拟节点读写测试通过",
                                         "虚拟节点读写测试失败",
                                         [&]() { return RunTierFileDemo(dir, "virtual", &last_virtual_logical_path_); });
        }
        if (action->id == "5") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "Masstree 导入完成",
                                         "Masstree 导入失败",
                                         [&]() { return RunMasstreeImportDemo(); });
        }
        if (action->id == "6") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "Masstree 查询完成",
                                         "Masstree 查询失败",
                                         [&]() { return RunMasstreeQueryDemo(); });
        }
        if (action->id == "7") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "完整测试集执行通过",
                                         "完整测试集中存在失败项",
                                         [&]() {
                                             return RunHealthCheck() && RunStatsScenario() && RunPosixSuite() &&
                                                    RunMasstreeSuite();
                                         });
        }
        return BuildInfoResult(action->title, false, "未实现的动作分发", action->usage);
    }

    int RunInteractive() {
        std::cout << "进入交互模式后，可输入序号和参数执行测试项。\n";
        for (;;) {
            zb::demo::RenderMenu("ZB Storage Demo Console", actions_);
            const std::string input = PromptLine("input");
            const zb::demo::ParsedCommand command = zb::demo::ParseCommandLine(input);
            if (!command.ok) {
                zb::demo::RenderResult(BuildInfoResult("输入错误", false, command.error, "9"));
                continue;
            }
            bool should_exit = false;
            zb::demo::DemoRunResult result = ExecuteInteractiveCommand(command, &should_exit);
            if (should_exit) {
                return 0;
            }
            if (!result.title.empty()) {
                zb::demo::RenderResult(result);
                MaybeAppendLog(result);
                if (command.action != "8" && command.action != "9") {
                    last_result_ = result;
                }
            }
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
        TierIoOptions options;
        options.dir_name = dir_name;
        options.expected_tier = expected_tier;
        options.file_size_bytes = FLAGS_posix_file_size_mb * 1024ULL * 1024ULL;
        options.chunk_size_bytes = std::max<uint32_t>(1U, FLAGS_posix_chunk_size_kb) * 1024U;
        options.repeat = std::max<uint32_t>(1U, FLAGS_posix_repeat);
        options.keep_file = FLAGS_posix_keep_file;
        options.verify_hash = FLAGS_posix_verify_hash;
        options.sync_on_close = FLAGS_posix_sync_on_close;
        return RunTierIoScenario(options, saved_logical_path);
    }

    bool BuildTierIoDiagnostics(TierIoDiagnostics* diagnostics) {
        if (!diagnostics) {
            return false;
        }
        if (!RefreshClusterView()) {
            return false;
        }
        CollectTierStats(nodes_, &diagnostics->real_stats, &diagnostics->virtual_stats);
        std::error_code ec;
        const fs::space_info info = fs::space(fs::path(FLAGS_mount_point), ec);
        if (!ec) {
            diagnostics->has_host_space = true;
            diagnostics->host_capacity_bytes = info.capacity;
            diagnostics->host_available_bytes = info.available;
        } else {
            diagnostics->has_host_space = false;
            diagnostics->host_capacity_bytes = 0;
            diagnostics->host_available_bytes = 0;
        }
        return true;
    }

    const TierStats& SelectTierStats(const std::string& tier, const TierIoDiagnostics& diagnostics) const {
        if (tier == "virtual") {
            return diagnostics.virtual_stats;
        }
        return diagnostics.real_stats;
    }



    void PrintTierIoDiagnostics(const TierIoOptions& options, const TierIoDiagnostics& diagnostics) {
        std::cout << "挂载点=" << FLAGS_mount_point << std::endl;
        if (diagnostics.has_host_space) {
            PrintByteMetric("宿主机文件系统可用字节", diagnostics.host_available_bytes);
        }
        const TierStats& tier_stats = SelectTierStats(options.expected_tier, diagnostics);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "层总容量字节", tier_stats.total_capacity_bytes);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "层已用容量字节", tier_stats.used_capacity_bytes);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "层剩余容量字节", tier_stats.free_capacity_bytes);
    }

    std::string ExplainNoSpaceForTierWrite(const TierIoOptions& options,
                                           const TierIoDiagnostics& diagnostics) const {
        const TierStats& tier_stats = SelectTierStats(options.expected_tier, diagnostics);
        std::ostringstream oss;
        if (diagnostics.has_host_space && diagnostics.host_available_bytes < options.file_size_bytes) {
            oss << "宿主机挂载文件系统可用空间不足，当前可用="
                << diagnostics.host_available_bytes << " (" << FormatBytes(diagnostics.host_available_bytes)
                << ")，待写入=" << options.file_size_bytes << " (" << FormatBytes(options.file_size_bytes) << ")";
            return oss.str();
        }
        if (tier_stats.free_capacity_bytes < options.file_size_bytes) {
            oss << DisplayTierName(options.expected_tier)
                << "层的可写剩余容量不足，当前剩余="
                << tier_stats.free_capacity_bytes << " (" << FormatBytes(tier_stats.free_capacity_bytes)
                << ")，待写入=" << options.file_size_bytes << " (" << FormatBytes(options.file_size_bytes)
                << ")。这类场景通常会被 MDS/FUSE 映射成 ENOSPC，而不一定是宿主机磁盘真正写满。";
            return oss.str();
        }
        oss << "本次 ENOSPC 更可能来自 MDS 的放置策略拒绝。FUSE 会把 NO_SPACE_REAL_POLICY/NO_SPACE_VIRTUAL_POLICY "
               "映射成 errno=28，因此即使宿主机磁盘还有空间，也可能返回 No space left on device。";
        return oss.str();
    }

    bool RunTierIoScenario(const TierIoOptions& options, std::string* saved_logical_path) {
        TierIoDiagnostics diagnostics;
        if (!BuildTierIoDiagnostics(&diagnostics)) {
            return false;
        }

        PrintSection("POSIX " + DisplayTierName(options.expected_tier) + "层演示");
        std::cout << "目标目录=" << BuildTierLogicalPath(options.dir_name) + "/demo" << std::endl;
        std::cout << "重复次数=" << options.repeat << std::endl;
        std::cout << "期望文件大小字节=" << options.file_size_bytes
                  << " (" << FormatBytes(options.file_size_bytes) << ")" << std::endl;
        std::cout << "块大小字节=" << options.chunk_size_bytes
                  << " (" << FormatBytes(options.chunk_size_bytes) << ")" << std::endl;
        PrintBoolMetric("校验哈希", options.verify_hash);
        PrintBoolMetric("保留文件", options.keep_file);
        PrintBoolMetric("关闭前刷盘", options.sync_on_close);
        PrintTierIoDiagnostics(options, diagnostics);

        uint64_t total_written = 0;
        uint64_t total_read = 0;
        uint64_t total_write_us = 0;
        uint64_t total_read_us = 0;
        TierIoIterationResult last_result;

        for (uint32_t attempt = 0; attempt < options.repeat; ++attempt) {
            TierIoIterationResult iteration;
            if (!RunSingleTierIoIteration(options, diagnostics, attempt, &iteration)) {
                return false;
            }
            last_result = iteration;
            total_written += iteration.bytes_written;
            total_read += iteration.bytes_read;
            total_write_us += iteration.write_elapsed_us;
            total_read_us += iteration.read_elapsed_us;
        }

        const double write_throughput_mib_s = ThroughputMiBS(total_written, total_write_us);
        const double read_throughput_mib_s = ThroughputMiBS(total_read, total_read_us);

        std::cout << "逻辑路径=" << last_result.logical_path << std::endl;
        std::cout << "挂载路径=" << last_result.mounted_path << std::endl;
        std::cout << "写入字节数=" << last_result.bytes_written << std::endl;
        std::cout << "读取字节数=" << last_result.bytes_read << std::endl;
        std::cout << "写入哈希=" << FormatHex64(last_result.write_hash) << std::endl;
        std::cout << "读取哈希=" << FormatHex64(last_result.read_hash) << std::endl;
        std::cout << "写入耗时毫秒=" << FormatDouble(static_cast<double>(last_result.write_elapsed_us) / 1000.0) << std::endl;
        std::cout << "读取耗时毫秒=" << FormatDouble(static_cast<double>(last_result.read_elapsed_us) / 1000.0) << std::endl;
        std::cout << "写入吞吐MiB每秒="
                  << FormatDouble(ThroughputMiBS(last_result.bytes_written, last_result.write_elapsed_us)) << std::endl;
        std::cout << "读取吞吐MiB每秒="
                  << FormatDouble(ThroughputMiBS(last_result.bytes_read, last_result.read_elapsed_us)) << std::endl;
        std::cout << "总写入字节数=" << total_written << std::endl;
        std::cout << "总读取字节数=" << total_read << std::endl;
        std::cout << "平均写入耗时毫秒="
                  << FormatDouble(static_cast<double>(total_write_us) / 1000.0 / options.repeat) << std::endl;
        std::cout << "平均读取耗时毫秒="
                  << FormatDouble(static_cast<double>(total_read_us) / 1000.0 / options.repeat) << std::endl;
        std::cout << "平均写入吞吐MiB每秒=" << FormatDouble(write_throughput_mib_s) << std::endl;
        std::cout << "平均读取吞吐MiB每秒=" << FormatDouble(read_throughput_mib_s) << std::endl;
        std::cout << "inode编号=" << last_result.inspection.inode_id << std::endl;
        std::cout << "文件大小=" << last_result.inspection.size_bytes << " ("
                  << FormatBytes(last_result.inspection.size_bytes) << ")" << std::endl;
        std::cout << "节点编号=" << last_result.inspection.node_id << std::endl;
        std::cout << "磁盘编号=" << last_result.inspection.disk_id << std::endl;
        std::cout << "解析节点类型=" << DisplayTierName(last_result.inspection.actual_tier) << std::endl;

        std::vector<CheckResult> checks;
        AddCheck(&checks,
                 "文件大小写入",
                 last_result.bytes_written == options.file_size_bytes,
                 "实际=" + std::to_string(last_result.bytes_written) +
                     " 期望=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "文件大小读取",
                 last_result.bytes_read == options.file_size_bytes,
                 "实际=" + std::to_string(last_result.bytes_read) +
                     " 期望=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "文件哈希一致",
                 !options.verify_hash || last_result.read_hash == last_result.write_hash,
                 options.verify_hash ? ("写入=" + FormatHex64(last_result.write_hash) +
                                        " 读取=" + FormatHex64(last_result.read_hash))
                                     : "未启用哈希校验");
        AddCheck(&checks,
                 "属性大小一致",
                 last_result.inspection.size_bytes == options.file_size_bytes,
                 "实际=" + std::to_string(last_result.inspection.size_bytes) +
                     " 期望=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "落盘层级正确",
                 last_result.inspection.actual_tier == options.expected_tier,
                 "实际=" + DisplayTierName(last_result.inspection.actual_tier) +
                     " 期望=" + DisplayTierName(options.expected_tier));

        bool ok = true;
        for (const auto& check : checks) {
            ok = ok && check.ok;
        }
        if (saved_logical_path) {
            *saved_logical_path = options.keep_file ? last_result.logical_path : std::string();
        }
        return ok;
    }

    bool RunSingleTierIoIteration(const TierIoOptions& options,
                                  const TierIoDiagnostics& diagnostics,
                                  uint32_t attempt,
                                  TierIoIterationResult* result) {
        if (!result) {
            std::cerr << "缺少层级读写结果输出对象" << std::endl;
            return false;
        }
        const std::string token = TimestampToken() + (options.repeat > 1 ? ("_" + std::to_string(attempt + 1)) : "");
        const std::string logical_dir = BuildTierLogicalPath(options.dir_name) + "/demo";
        const std::string logical_path = logical_dir + "/" + options.expected_tier + "_" +
                                         std::to_string(options.file_size_bytes / (1024ULL * 1024ULL)) +
                                         "MB_" + token + ".bin";
        const std::string mounted_dir = JoinMountedPath(FLAGS_mount_point, logical_dir);
        const std::string mounted_path = JoinMountedPath(FLAGS_mount_point, logical_path);

        std::error_code ec;
        fs::create_directories(mounted_dir, ec);
        if (ec) {
            std::cerr << "创建目录失败 " << mounted_dir << ": " << ec.message() << std::endl;
            return false;
        }

        uint64_t write_hash = 14695981039346656037ULL;
        uint64_t bytes_written = 0;
        uint64_t write_elapsed_us = 0;
        int write_errno = 0;
        if (!WritePatternFile(mounted_path,
                              options.expected_tier,
                              options.chunk_size_bytes,
                              options.file_size_bytes,
                              options.sync_on_close,
                              &bytes_written,
                              &write_hash,
                              &write_elapsed_us,
                              &write_errno)) {
            if (write_errno == ENOSPC) {
                std::cerr << ExplainNoSpaceForTierWrite(options, diagnostics) << std::endl;
            }
            return false;
        }

        uint64_t read_hash = 14695981039346656037ULL;
        uint64_t bytes_read = 0;
        uint64_t read_elapsed_us = 0;
        if (!ReadPatternFile(mounted_path,
                             options.chunk_size_bytes,
                             options.verify_hash,
                             &bytes_read,
                             &read_hash,
                             &read_elapsed_us)) {
            return false;
        }

        FileInspectionResult inspection;
        if (!InspectFile(logical_path, options.expected_tier, &inspection)) {
            return false;
        }

        result->logical_path = logical_path;
        result->mounted_path = mounted_path;
        result->expected_size_bytes = options.file_size_bytes;
        result->bytes_written = bytes_written;
        result->bytes_read = bytes_read;
        result->write_elapsed_us = write_elapsed_us;
        result->read_elapsed_us = read_elapsed_us;
        result->write_hash = write_hash;
        result->read_hash = read_hash;
        result->inspection = inspection;

        if (!options.keep_file) {
            fs::remove(fs::path(mounted_path), ec);
            if (ec) {
                std::cerr << "删除文件失败 " << mounted_path << ": " << ec.message() << std::endl;
                return false;
            }
        }
        return true;
    }

    bool InspectKnownFile(const std::string& logical_path, const std::string& label) {
        if (logical_path.empty()) {
            std::cerr << "没有记录可供查看的" << label << "路径" << std::endl;
            return false;
        }
        PrintSection("查看" + label + "文件");
        return InspectFile(logical_path, "", nullptr);
    }

    bool InspectFile(const std::string& logical_path,
                     const std::string& expected_tier,
                     FileInspectionResult* out) {
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup(logical_path, &attr, &status)) {
            std::cerr << "查询文件失败 " << logical_path << ": " << status.message() << std::endl;
            return false;
        }
        zb::rpc::FileLocationView view;
        if (!mds_.GetFileLocation(attr.inode_id(), &view, &status)) {
            std::cerr << "查询文件位置失败 inode=" << attr.inode_id() << ": " << status.message() << std::endl;
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

        std::cout << "inode编号=" << attr.inode_id() << std::endl;
        std::cout << "属性大小字节=" << attr.size() << " (" << FormatBytes(attr.size()) << ")" << std::endl;
        std::cout << "节点编号=" << node_id << std::endl;
        std::cout << "磁盘编号=" << disk_id << std::endl;
        std::cout << "解析节点类型=" << DisplayTierName(actual_tier) << std::endl;
        if (out) {
            out->inode_id = attr.inode_id();
            out->size_bytes = attr.size();
            out->node_id = node_id;
            out->disk_id = disk_id;
            out->actual_tier = actual_tier;
        }
        if (!expected_tier.empty() && actual_tier != expected_tier) {
            std::cerr << "期望层级为" << DisplayTierName(expected_tier)
                      << "，实际为" << DisplayTierName(actual_tier) << std::endl;
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

        zb::rpc::GetMasstreeClusterStatsReply cluster_before;
        if (!mds_.GetMasstreeClusterStats(&cluster_before)) {
            std::cerr << "GetMasstreeClusterStats(before) failed: " << cluster_before.status().message() << '\n';
            return false;
        }

        bool had_previous_namespace = false;
        zb::rpc::GetMasstreeNamespaceStatsReply previous_namespace_stats;
        if (!mds_.GetMasstreeNamespaceStats(namespace_id, &previous_namespace_stats)) {
            std::cerr << "GetMasstreeNamespaceStats(before) failed: "
                      << previous_namespace_stats.status().message() << '\n';
            return false;
        }
        if (previous_namespace_stats.status().code() == zb::rpc::MDS_OK) {
            had_previous_namespace = true;
        } else if (previous_namespace_stats.status().code() != zb::rpc::MDS_NOT_FOUND) {
            std::cerr << "GetMasstreeNamespaceStats(before) failed: "
                      << previous_namespace_stats.status().message() << '\n';
            return false;
        }

        zb::rpc::ImportMasstreeNamespaceRequest request;
	        request.set_namespace_id(namespace_id);
	        request.set_generation_id(generation_id);
	        request.set_path_prefix(path_prefix);
	        request.set_template_id(FLAGS_masstree_template_id);
	        request.set_template_mode(FLAGS_masstree_template_mode);
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
	        if (!FLAGS_masstree_template_id.empty()) {
	            std::cout << "template_id=" << FLAGS_masstree_template_id << '\n';
	        }
	        if (!FLAGS_masstree_template_mode.empty()) {
	            std::cout << "template_mode=" << FLAGS_masstree_template_mode << '\n';
	        }
	        std::cout << "job_id=" << reply.job_id() << '\n';

        const auto poll_started_at = std::chrono::steady_clock::now();
        zb::rpc::MasstreeImportJobState last_state = zb::rpc::MASSTREE_IMPORT_JOB_PENDING;
        bool has_last_state = false;
        uint64_t last_printed_elapsed_bucket = std::numeric_limits<uint64_t>::max();

        while (true) {
            zb::rpc::GetMasstreeImportJobReply job_reply;
            if (!mds_.GetMasstreeImportJob(reply.job_id(), &job_reply)) {
                std::cerr << "GetMasstreeImportJob failed: " << job_reply.status().message() << '\n';
                return false;
            }
            if (!job_reply.found()) {
                std::cerr << "Masstree import job disappeared: " << reply.job_id() << '\n';
                return false;
            }

            const auto& job = job_reply.job();
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - poll_started_at);
            const uint64_t elapsed_seconds = static_cast<uint64_t>(elapsed.count());
            const uint64_t elapsed_bucket = elapsed_seconds / 10ULL;
            if (!has_last_state || job.state() != last_state || elapsed_bucket != last_printed_elapsed_bucket) {
                std::cout << "job_status=" << MasstreeJobStateName(job.state())
                          << " elapsed=" << FormatDurationSeconds(elapsed_seconds) << '\n';
                last_state = job.state();
                has_last_state = true;
                last_printed_elapsed_bucket = elapsed_bucket;
            }
            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED) {
                zb::rpc::GetMasstreeClusterStatsReply cluster_after;
                if (!mds_.GetMasstreeClusterStats(&cluster_after)) {
                    std::cerr << "GetMasstreeClusterStats(after) failed: " << cluster_after.status().message() << '\n';
                    return false;
                }

                const uint64_t previous_namespace_file_count =
                    had_previous_namespace ? previous_namespace_stats.file_count() : 0;
                const std::string previous_namespace_total_file_bytes =
                    had_previous_namespace ? previous_namespace_stats.total_file_bytes() : std::string("0");
                const std::string previous_namespace_total_metadata_bytes =
                    had_previous_namespace ? previous_namespace_stats.total_metadata_bytes() : std::string("0");
                const std::string replaced_total_file_bytes_base =
                    zb::mds::CompareDecimalStrings(cluster_before.total_file_bytes(),
                                                   previous_namespace_total_file_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.total_file_bytes(),
                                                          previous_namespace_total_file_bytes)
                        : std::string("0");
                const std::string replaced_total_metadata_bytes_base =
                    zb::mds::CompareDecimalStrings(cluster_before.total_metadata_bytes(),
                                                   previous_namespace_total_metadata_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.total_metadata_bytes(),
                                                          previous_namespace_total_metadata_bytes)
                        : std::string("0");
                const std::string replaced_used_capacity_base =
                    zb::mds::CompareDecimalStrings(cluster_before.used_capacity_bytes(),
                                                   previous_namespace_total_file_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.used_capacity_bytes(),
                                                          previous_namespace_total_file_bytes)
                        : std::string("0");
                const uint64_t expected_total_file_count =
                    cluster_before.total_file_count() >= previous_namespace_file_count
                        ? (cluster_before.total_file_count() - previous_namespace_file_count + job.file_count())
                        : job.file_count();
                const std::string expected_total_file_bytes =
                    zb::mds::AddDecimalStrings(replaced_total_file_bytes_base, job.total_file_bytes());
                const std::string expected_total_metadata_bytes =
                    zb::mds::AddDecimalStrings(replaced_total_metadata_bytes_base,
                                               std::to_string(job.inode_pages_bytes()));
                const std::string expected_used_capacity_bytes =
                    zb::mds::AddDecimalStrings(replaced_used_capacity_base, job.total_file_bytes());
                const std::string expected_free_capacity_bytes =
                    zb::mds::SubtractDecimalStrings(cluster_after.total_capacity_bytes(),
                                                    expected_used_capacity_bytes);

                std::cout << "manifest_path=" << job.manifest_path() << '\n';
                std::cout << "root_inode_id=" << job.root_inode_id() << '\n';
                std::cout << "inode_count=" << job.inode_count() << '\n';
                std::cout << "dentry_count=" << job.dentry_count() << '\n';
                std::cout << "level1_dir_count=" << job.level1_dir_count() << '\n';
                std::cout << "leaf_dir_count=" << job.leaf_dir_count() << '\n';
                std::cout << "file_count=" << job.file_count() << '\n';
                PrintDecimalMetric("import_total_file_bytes", job.total_file_bytes());
                std::cout << "import_avg_file_size_bytes=" << job.avg_file_size_bytes() << '\n';
                std::cout << "inode_range=[" << job.inode_min() << ", " << job.inode_max() << "]\n";
                std::cout << "inode_pages_bytes=" << job.inode_pages_bytes() << '\n';
                std::cout << "previous_namespace_present=" << (had_previous_namespace ? "true" : "false") << '\n';
                if (had_previous_namespace) {
                    std::cout << "previous_namespace_generation_id=" << previous_namespace_stats.generation_id() << '\n';
                    std::cout << "previous_namespace_file_count=" << previous_namespace_stats.file_count() << '\n';
                    PrintDecimalMetric("previous_namespace_total_file_bytes",
                                       previous_namespace_stats.total_file_bytes());
                    PrintDecimalMetric("previous_namespace_total_metadata_bytes",
                                       previous_namespace_stats.total_metadata_bytes());
                }
                std::cout << "before_total_file_count=" << cluster_before.total_file_count() << '\n';
                PrintDecimalMetric("before_total_file_bytes", cluster_before.total_file_bytes());
                PrintDecimalMetric("before_total_metadata_bytes", cluster_before.total_metadata_bytes());
                PrintDecimalMetric("before_used_capacity_bytes", cluster_before.used_capacity_bytes());
                PrintDecimalMetric("before_free_capacity_bytes", cluster_before.free_capacity_bytes());
                std::cout << "after_total_file_count=" << cluster_after.total_file_count() << '\n';
                PrintDecimalMetric("after_total_file_bytes", cluster_after.total_file_bytes());
                PrintDecimalMetric("after_total_metadata_bytes", cluster_after.total_metadata_bytes());
                PrintDecimalMetric("after_used_capacity_bytes", cluster_after.used_capacity_bytes());
                PrintDecimalMetric("after_free_capacity_bytes", cluster_after.free_capacity_bytes());
                std::cout << "delta_total_file_count="
                          << FormatSignedUint64Delta(cluster_after.total_file_count(),
                                                    cluster_before.total_file_count()) << '\n';
                std::cout << "delta_total_file_bytes="
                          << FormatSignedDecimalDelta(cluster_after.total_file_bytes(),
                                                      cluster_before.total_file_bytes()) << '\n';
                std::cout << "delta_total_metadata_bytes="
                          << FormatSignedDecimalDelta(cluster_after.total_metadata_bytes(),
                                                      cluster_before.total_metadata_bytes()) << '\n';
                std::cout << "delta_used_capacity_bytes="
                          << FormatSignedDecimalDelta(cluster_after.used_capacity_bytes(),
                                                      cluster_before.used_capacity_bytes()) << '\n';
                std::cout << "delta_free_capacity_bytes="
                          << FormatSignedDecimalDelta(cluster_after.free_capacity_bytes(),
                                                      cluster_before.free_capacity_bytes()) << '\n';

                std::vector<CheckResult> checks;
                AddCheck(&checks,
                         "import.job_file_count",
                         job.file_count() == request.file_count(),
                         "actual=" + std::to_string(job.file_count()) +
                             " expected=" + std::to_string(request.file_count()));
                AddCheck(&checks,
                         "import.namespace_id",
                         job.namespace_id() == namespace_id,
                         "actual=" + job.namespace_id() + " expected=" + namespace_id);
                AddCheck(&checks,
                         "import.generation_id",
                         job.generation_id() == generation_id,
                         "actual=" + job.generation_id() + " expected=" + generation_id);
                AddCheck(&checks,
                         "import.path_prefix",
                         job.path_prefix() == path_prefix,
                         "actual=" + job.path_prefix() + " expected=" + path_prefix);
                AddCheck(&checks,
                         "stats.total_file_count_after",
                         cluster_after.total_file_count() == expected_total_file_count,
                         "actual=" + std::to_string(cluster_after.total_file_count()) +
                             " expected=" + std::to_string(expected_total_file_count));
                AddCheck(&checks,
                         "stats.total_file_bytes_after",
                         zb::mds::CompareDecimalStrings(cluster_after.total_file_bytes(),
                                                        expected_total_file_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.total_file_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_total_file_bytes));
                AddCheck(&checks,
                         "stats.total_metadata_bytes_after",
                         zb::mds::CompareDecimalStrings(cluster_after.total_metadata_bytes(),
                                                        expected_total_metadata_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.total_metadata_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_total_metadata_bytes));
                AddCheck(&checks,
                         "stats.used_capacity_after",
                         zb::mds::CompareDecimalStrings(cluster_after.used_capacity_bytes(),
                                                        expected_used_capacity_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.used_capacity_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_used_capacity_bytes));
                AddCheck(&checks,
                         "stats.free_capacity_after",
                         zb::mds::CompareDecimalStrings(cluster_after.free_capacity_bytes(),
                                                        expected_free_capacity_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.free_capacity_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_free_capacity_bytes));

                bool ok = true;
                for (const auto& check : checks) {
                    ok = ok && check.ok;
                }
                return ok;
            }
            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_FAILED) {
                std::cerr << "Masstree import job failed: " << job.error_message() << '\n';
                return false;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::max<uint32_t>(1, FLAGS_masstree_job_poll_interval_ms)));
        }
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
        const uint32_t sample_count = std::max<uint32_t>(1, FLAGS_masstree_query_samples);
        struct QuerySample {
            uint32_t index{0};
            bool ok{false};
            uint64_t latency_us{0};
            zb::rpc::GetRandomMasstreeFileAttrReply reply;
            std::string error_message;
        };
        std::vector<QuerySample> samples;
        samples.reserve(sample_count);
        uint32_t success_count = 0;
        uint32_t failure_count = 0;
        uint64_t total_latency_us = 0;
        uint64_t min_latency_us = std::numeric_limits<uint64_t>::max();
        uint64_t max_latency_us = 0;

        for (uint32_t i = 0; i < sample_count; ++i) {
            const auto started_at = std::chrono::steady_clock::now();
            QuerySample sample;
            sample.index = i + 1;
            sample.ok = mds_.GetRandomMasstreeFileAttr(attr_request, &sample.reply);
            sample.latency_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started_at)
                    .count());
            if (!sample.ok) {
                sample.error_message = sample.reply.status().message();
            }
            total_latency_us += sample.latency_us;
            min_latency_us = std::min<uint64_t>(min_latency_us, sample.latency_us);
            max_latency_us = std::max<uint64_t>(max_latency_us, sample.latency_us);
            if (sample.ok) {
                ++success_count;
            } else {
                ++failure_count;
            }
            samples.push_back(std::move(sample));
        }

        std::string resolved_namespace_id = attr_request.namespace_id();
        std::string resolved_path_prefix = attr_request.path_prefix();
        std::string resolved_generation_id;
        for (const auto& sample : samples) {
            if (!sample.ok) {
                continue;
            }
            if (resolved_namespace_id.empty()) {
                resolved_namespace_id = sample.reply.namespace_id();
            }
            if (resolved_path_prefix.empty()) {
                resolved_path_prefix = sample.reply.path_prefix();
            }
            resolved_generation_id = sample.reply.generation_id();
            break;
        }

        std::cout << "namespace_id=" << resolved_namespace_id << '\n';
        std::cout << "path_prefix=" << resolved_path_prefix << '\n';
        std::cout << "generation_id=" << resolved_generation_id << '\n';
        std::cout << "query_samples=" << sample_count << '\n';
        std::cout << "query_success_count=" << success_count << '\n';
        std::cout << "query_failure_count=" << failure_count << '\n';
        std::cout << "query_success_rate="
                  << FormatDouble(sample_count == 0 ? 0.0
                                                    : static_cast<double>(success_count) / static_cast<double>(sample_count),
                                  4)
                  << '\n';
        std::cout << "total_query_latency_us=" << total_latency_us << '\n';
        std::cout << "avg_query_latency_us=" << (sample_count == 0 ? 0 : (total_latency_us / sample_count)) << '\n';
        std::cout << "min_query_latency_us=" << (sample_count == 0 ? 0 : min_latency_us) << '\n';
        std::cout << "max_query_latency_us=" << max_latency_us << '\n';
        for (const auto& sample : samples) {
            const std::string prefix = "query[" + std::to_string(sample.index) + "]";
            std::cout << prefix << ".ok=" << (sample.ok ? "true" : "false") << '\n';
            std::cout << prefix << ".latency_us=" << sample.latency_us << '\n';
            std::cout << prefix << ".status_code=" << static_cast<int>(sample.reply.status().code()) << '\n';
            std::cout << prefix << ".status_message="
                      << (sample.ok ? "OK" : sample.error_message) << '\n';
            if (!sample.ok) {
                continue;
            }
            const auto& attr = sample.reply.attr();
            std::cout << prefix << ".namespace_id=" << sample.reply.namespace_id() << '\n';
            std::cout << prefix << ".path_prefix=" << sample.reply.path_prefix() << '\n';
            std::cout << prefix << ".generation_id=" << sample.reply.generation_id() << '\n';
            std::cout << prefix << ".inode_id=" << sample.reply.inode_id() << '\n';
            std::cout << prefix << ".file_name=" << sample.reply.file_name() << '\n';
            std::cout << prefix << ".type=" << InodeTypeToString(attr.type()) << '\n';
            std::cout << prefix << ".mode=" << attr.mode() << '\n';
            std::cout << prefix << ".uid=" << attr.uid() << '\n';
            std::cout << prefix << ".gid=" << attr.gid() << '\n';
            std::cout << prefix << ".size_bytes=" << attr.size() << '\n';
            std::cout << prefix << ".size_human=" << FormatBytes(attr.size()) << '\n';
            std::cout << prefix << ".atime=" << attr.atime() << '\n';
            std::cout << prefix << ".mtime=" << attr.mtime() << '\n';
            std::cout << prefix << ".ctime=" << attr.ctime() << '\n';
            std::cout << prefix << ".nlink=" << attr.nlink() << '\n';
            std::cout << prefix << ".object_unit_size=" << attr.object_unit_size() << '\n';
            std::cout << prefix << ".replica=" << attr.replica() << '\n';
            std::cout << prefix << ".version=" << attr.version() << '\n';
            std::cout << prefix << ".file_archive_state="
                      << ArchiveStateToString(attr.file_archive_state()) << '\n';
        }

        zb::rpc::GetMasstreeClusterStatsReply stats_reply;
        if (!mds_.GetMasstreeClusterStats(&stats_reply)) {
            std::cerr << "GetMasstreeClusterStats failed: " << stats_reply.status().message() << '\n';
            return false;
        }
        std::cout << "disk_node_count=" << stats_reply.disk_node_count() << '\n';
        std::cout << "optical_node_count=" << stats_reply.optical_node_count() << '\n';
        std::cout << "disk_device_count=" << stats_reply.disk_device_count() << '\n';
        std::cout << "optical_device_count=" << stats_reply.optical_device_count() << '\n';
        std::cout << "total_capacity_bytes=" << stats_reply.total_capacity_bytes() << '\n';
        std::cout << "total_file_count=" << stats_reply.total_file_count() << '\n';
        std::cout << "total_file_bytes=" << stats_reply.total_file_bytes() << '\n';
        std::cout << "avg_file_size_bytes=" << stats_reply.avg_file_size_bytes() << '\n';
        std::cout << "used_capacity_bytes=" << stats_reply.used_capacity_bytes() << '\n';
        std::cout << "free_capacity_bytes=" << stats_reply.free_capacity_bytes() << '\n';
        std::cout << "total_metadata_bytes=" << stats_reply.total_metadata_bytes() << '\n';
        std::cout << "min_file_size_bytes=" << stats_reply.min_file_size_bytes() << '\n';
        std::cout << "max_file_size_bytes=" << stats_reply.max_file_size_bytes() << '\n';
        return failure_count == 0;
    }

    static void FillPatternChunk(std::vector<char>* buffer,
                                 size_t size,
                                 uint64_t absolute_offset,
                                 const std::string& label) {
        if (!buffer) {
            return;
        }
        buffer->resize(size);
        const uint64_t label_hash = std::hash<std::string>{}(label);
        for (size_t i = 0; i < size; ++i) {
            const uint64_t value = absolute_offset + i + label_hash;
            (*buffer)[i] = static_cast<char>('A' + (value % 26ULL));
        }
    }

    static bool WritePatternFile(const std::string& path,
                                 const std::string& label,
                                 uint32_t chunk_size_bytes,
                                 uint64_t total_size_bytes,
                                 bool sync_on_close,
                                 uint64_t* bytes_written,
                                 uint64_t* hash_out,
                                 uint64_t* elapsed_us,
                                 int* failure_errno) {
        if (failure_errno) {
            *failure_errno = 0;
        }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            const int open_errno = errno;
            if (failure_errno) {
                *failure_errno = open_errno;
            }
            std::cerr << "打开写入文件失败: " << path
                      << " errno=" << open_errno
                      << " 错误=" << std::strerror(open_errno) << '\n';
            return false;
        }
        uint64_t hash = 14695981039346656037ULL;
        uint64_t written = 0;
        std::vector<char> buffer;
        const auto started_at = std::chrono::steady_clock::now();
        while (written < total_size_bytes) {
            const size_t current_size = static_cast<size_t>(
                std::min<uint64_t>(chunk_size_bytes, total_size_bytes - written));
            FillPatternChunk(&buffer, current_size, written, label);
            out.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            if (!out) {
                const int write_errno = errno;
                if (failure_errno) {
                    *failure_errno = write_errno;
                }
                std::cerr << "写入文件失败: " << path;
                if (write_errno != 0) {
                    std::cerr << " errno=" << write_errno << " 错误=" << std::strerror(write_errno);
                }
                std::cerr << '\n';
                return false;
            }
            hash = Fnv1a64Append(hash, buffer.data(), buffer.size());
            written += static_cast<uint64_t>(buffer.size());
        }
        if (sync_on_close) {
            out.flush();
            if (!out) {
                const int flush_errno = errno;
                if (failure_errno) {
                    *failure_errno = flush_errno;
                }
                std::cerr << "刷新文件失败: " << path;
                if (flush_errno != 0) {
                    std::cerr << " errno=" << flush_errno << " 错误=" << std::strerror(flush_errno);
                }
                std::cerr << '\n';
                return false;
            }
        }
        out.close();
        if (!out) {
            const int close_errno = errno;
            if (failure_errno) {
                *failure_errno = close_errno;
            }
            std::cerr << "关闭写入文件失败: " << path;
            if (close_errno != 0) {
                std::cerr << " errno=" << close_errno << " 错误=" << std::strerror(close_errno);
            }
            std::cerr << '\n';
            return false;
        }
        const auto finished_at = std::chrono::steady_clock::now();
        if (bytes_written) {
            *bytes_written = written;
        }
        if (hash_out) {
            *hash_out = hash;
        }
        if (elapsed_us) {
            *elapsed_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
        }
        return true;
    }

    static bool ReadPatternFile(const std::string& path,
                                uint32_t chunk_size_bytes,
                                bool verify_hash,
                                uint64_t* bytes_read,
                                uint64_t* hash_out,
                                uint64_t* elapsed_us) {
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            const int open_errno = errno;
            std::cerr << "打开读取文件失败: " << path
                      << " errno=" << open_errno
                      << " 错误=" << std::strerror(open_errno) << '\n';
            return false;
        }
        uint64_t hash = 14695981039346656037ULL;
        uint64_t read_total = 0;
        std::vector<char> buffer(std::max<uint32_t>(1U, chunk_size_bytes));
        const auto started_at = std::chrono::steady_clock::now();
        while (in) {
            in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize count = in.gcount();
            if (count <= 0) {
                break;
            }
            if (verify_hash) {
                hash = Fnv1a64Append(hash, buffer.data(), static_cast<size_t>(count));
            }
            read_total += static_cast<uint64_t>(count);
        }
        if (in.bad()) {
            std::cerr << "读取文件失败: " << path << '\n';
            return false;
        }
        const auto finished_at = std::chrono::steady_clock::now();
        if (bytes_read) {
            *bytes_read = read_total;
        }
        if (hash_out) {
            *hash_out = verify_hash ? hash : 0;
        }
        if (elapsed_us) {
            *elapsed_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
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

    MdsClient mds_;
    SchedulerClient scheduler_;
    std::vector<zb::demo::MenuActionSpec> actions_;
    std::vector<zb::rpc::NodeView> nodes_;
    std::unordered_map<std::string, zb::rpc::NodeType> node_type_by_id_;
    uint64_t cluster_generation_{0};
    std::string last_real_logical_path_;
    std::string last_virtual_logical_path_;
    std::string last_masstree_namespace_id_;
    std::string last_masstree_generation_id_;
    std::string last_masstree_path_prefix_;
    std::optional<zb::demo::DemoRunResult> last_result_;
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
