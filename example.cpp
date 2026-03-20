#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gflags/gflags.h>

#include "mds/inode/InodeStorage.h"
#include "mds/server/Server.h"

namespace fs = std::filesystem;

DEFINE_string(mds_addr, "127.0.0.1:8010", "MDS address host:port (unused in posix mode)");
DEFINE_string(srm_addr, "127.0.0.1:9100", "SRM address host:port (unused in posix mode)");
DEFINE_string(mount_point, "/mnt/zbstorage", "FUSE mount point for POSIX operations");
DEFINE_string(inode_dir, "", "Directory holding inode batch .bin files");
DEFINE_string(start_file, "", "Start inode batch file (name or full path)");
DEFINE_uint64(start_index, 0, "Start inode index within start_file");
DEFINE_uint32(ssd_nodes, 0, "SSD node count");
DEFINE_uint32(hdd_nodes, 100, "HDD node count");
DEFINE_uint32(mix_nodes, 0, "Mix node count");
DEFINE_uint32(ssd_devices_per_node, 1, "SSD devices per SSD/Mix node");
DEFINE_uint32(hdd_devices_per_node, 32, "HDD devices per HDD/Mix node");
DEFINE_uint64(ssd_capacity_bytes, 0, "SSD device capacity bytes");
DEFINE_uint64(hdd_capacity_bytes, 16ULL * 1024 * 1024 * 1024 * 1024, "HDD device capacity bytes");
DEFINE_string(kv_inode_dir, "/mnt/md0/inodeNS", "Global KV inode directory");
DEFINE_string(kv_bitmap_path, "", "Global KV inode bitmap path");
DEFINE_string(kv_rocksdb_global, "", "Global KV rocksdb directory");
DEFINE_string(kv_dir_store, "./_mds_real_kv_tmp/dir_store", "Global KV dir_store base");
DEFINE_uint64(kv_probe_slots, 200000, "Global KV probe slots in inode_chunk_0.bin");
DEFINE_uint64(kv_random_lookup, 0, "Global KV random lookup count");
DEFINE_uint32(kv_random_lookup_batches, 16, "Global KV random lookup batch count");
DEFINE_uint32(kv_random_print, 5, "Global KV random lookup print limit");
DEFINE_uint64(kv_random_lookup_warmup, 0, "Global KV random lookup warmup count");
DEFINE_uint64(kv_random_seed, 0, "Global KV random seed");

namespace {

struct DeviceState {
    std::string device_id;
    uint64_t capacity{0};
    uint64_t used{0};
};

struct NodeState {
    std::string node_id;
    uint8_t type{0};
    std::vector<DeviceState> ssd_devices;
    std::vector<DeviceState> hdd_devices;
};

struct Stats {
    uint64_t inodes{0};
    uint64_t bytes{0};
    uint64_t failed{0};
    uint64_t missing_node{0};
};

constexpr uint64_t kDisc100GCount = 200000ULL;
constexpr uint64_t kDisc1TCount = 506000ULL;
constexpr uint64_t kDisc10TCount = 94000ULL;
constexpr uint64_t kDiscTotalCount = 1000000ULL;
constexpr uint64_t kDiscLibraryCount = 100000ULL;
constexpr uint64_t kGiB = 1024ULL * 1024 * 1024;
constexpr uint64_t kTiB = 1024ULL * 1024 * 1024 * 1024;
constexpr uint64_t kDisc100GBytes = 100ULL * kGiB;
constexpr uint64_t kDisc1TBytes = 1ULL * kTiB;
constexpr uint64_t kDisc10TBytes = 10ULL * kTiB;
constexpr uint64_t kOpticalNodeCount = 100000ULL;
constexpr uint64_t kDiskNodeCount = 100ULL;
constexpr uint64_t kDiskDevicesPerNode = 32ULL;
constexpr uint64_t kDiskDeviceBytes = 16ULL * kTiB;
constexpr uint64_t kInitialFileCount = 1000000000ULL;
constexpr uint64_t kNamespaceCount = 1000ULL;
constexpr uint64_t kInitialFileBytes = 1ULL << 60;

std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit < (sizeof(units) / sizeof(units[0]) - 1)) {
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

std::string FormatBytesLongDouble(long double bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    long double value = bytes;
    size_t unit = 0;
    const size_t max_unit = (sizeof(units) / sizeof(units[0]) - 1);
    while (value >= 1024.0L && unit < max_unit) {
        value /= 1024.0L;
        ++unit;
    }
    std::ostringstream oss;
    if (unit == 0) {
        oss << static_cast<uint64_t>(value) << units[unit];
    } else {
        oss << std::fixed << std::setprecision(2) << static_cast<double>(value) << units[unit];
    }
    return oss.str();
}

long double CalcOpticalCapacityBytes() {
    long double total = 0.0L;
    total += static_cast<long double>(kDisc100GCount) * static_cast<long double>(kDisc100GBytes);
    total += static_cast<long double>(kDisc1TCount) * static_cast<long double>(kDisc1TBytes);
    total += static_cast<long double>(kDisc10TCount) * static_cast<long double>(kDisc10TBytes);
    return total;
}

uint64_t AddSaturating(uint64_t a, uint64_t b) {
    if (UINT64_MAX - a < b) {
        return UINT64_MAX;
    }
    return a + b;
}

uint64_t RandomFileSizeBytes() {
    constexpr uint64_t kMinSize = 10ULL * 1024;
    constexpr uint64_t kMaxSize = 2ULL * 1024 * 1024 * 1024;
    static thread_local std::mt19937_64 rng(
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) ^
        (static_cast<uint64_t>(std::random_device{}()) << 1));
    static std::uniform_int_distribution<uint64_t> dist(kMinSize, kMaxSize);
    return dist(rng);
}

uint64_t MapFileSizeFromMeta(uint64_t meta_size, uint64_t inode_id) {
    constexpr uint64_t kMinSize = 10ULL * 1024;
    constexpr uint64_t kMaxSize = 2ULL * 1024 * 1024 * 1024;
    const uint64_t range = kMaxSize - kMinSize + 1;
    uint64_t seed = meta_size ^ (inode_id * 0x9e3779b97f4a7c15ULL);
    seed ^= (seed << 13);
    seed ^= (seed >> 7);
    seed ^= (seed << 17);
    return kMinSize + (seed % range);
}

struct MdsGlobalKVRealTesterConfig {
    std::string inode_dir = "/mnt/md0/inodeNS";
    std::string bitmap_path;
    std::string rocksdb_global;
    std::string dir_store_base = "./_mds_real_kv_tmp/dir_store";
    uint64_t probe_slots = 200000;
    uint64_t random_lookup_n = 0;
    uint32_t random_lookup_batches = 16;
    uint32_t random_print_limit = 5;
    uint64_t random_lookup_warmup_n = 0;
    uint64_t random_seed = 0;
};

static void ensure_dir(const std::string& path) {
    std::error_code ec;
    fs::create_directories(path, ec);
    if (ec) {
        throw std::runtime_error("failed to create dir: " + path + " err=" + ec.message());
    }
}

static std::string NodeTypeName(uint8_t type) {
    switch (type & 0x03) {
        case 0: return "SSD";
        case 1: return "HDD";
        case 2: return "Mix";
        default: return "Reserved";
    }
}

static void PrintInodeDumpStyle(const Inode& inode, uint64_t index) {
    std::cout << "inode[" << index << "]\n";
    std::cout << "  inode_id=" << inode.inode << "\n";
    std::cout << "  namespace_id=" << inode.getNamespaceId() << "\n";
    std::cout << "  node_id=" << inode.location_id.fields.node_id
              << " node_type=" << NodeTypeName(inode.location_id.fields.node_type) << "\n";
    std::cout << "  file_size_bytes=" << inode.getFileSize() << "\n";
    std::cout << "  filename=" << inode.filename << "\n";
    std::cout << "  volume_id=" << inode.getVolumeUUID() << "\n";
    std::cout << "  block_segments=" << inode.block_segments.size() << "\n";
}

enum class TemperatureClass { Hot = 0, Warm = 1, Cold = 2 };

static TemperatureClass pick_temperature(const InodeStorage::BatchGenerationConfig& cfg,
                                         std::mt19937_64& rng) {
    std::vector<double> weights = {
        std::max(cfg.temp_ratio.hot, 0.0),
        std::max(cfg.temp_ratio.warm, 0.0),
        std::max(cfg.temp_ratio.cold, 0.0),
    };
    double sum = weights[0] + weights[1] + weights[2];
    if (sum <= 0.0) {
        weights = {0.2, 0.3, 0.5};
    }
    std::discrete_distribution<int> dist(weights.begin(), weights.end());
    return static_cast<TemperatureClass>(dist(rng));
}

static const InodeStorage::SizeRange& range_for_temperature(
    const InodeStorage::BatchGenerationConfig& cfg,
    TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot: return cfg.hot_range;
        case TemperatureClass::Warm: return cfg.warm_range;
        default: return cfg.cold_range;
    }
}

static uint64_t pick_size(const InodeStorage::SizeRange& range, std::mt19937_64& rng) {
    if (range.max_bytes <= range.min_bytes) {
        return range.min_bytes;
    }
    std::uniform_int_distribution<uint64_t> dist(range.min_bytes, range.max_bytes);
    return dist(rng);
}

static std::string build_path_name(uint64_t ino,
                                   TemperatureClass klass,
                                   const InodeStorage::BatchGenerationConfig& cfg,
                                   std::mt19937_64& rng) {
    const char* prefix = "cold";
    if (klass == TemperatureClass::Hot) {
        prefix = "hot";
    } else if (klass == TemperatureClass::Warm) {
        prefix = "warm";
    }

    std::string root = cfg.root_path.empty() ? "/dataset" : cfg.root_path;
    if (root.size() > 1 && root.back() == '/') {
        root.pop_back();
    }
    if (root.empty()) {
        root = "/dataset";
    } else if (root.front() != '/') {
        root = "/" + root;
    }

    size_t depth = cfg.dir_depth == 0 ? 1 : cfg.dir_depth;
    size_t fanout = cfg.dir_fanout == 0 ? 4 : cfg.dir_fanout;
    std::uniform_int_distribution<size_t> dir_dist(0, fanout - 1);

    std::ostringstream oss;
    oss << root;
    for (size_t level = 0; level < depth; ++level) {
        size_t idx = dir_dist(rng);
        oss << "/level" << level << '_' << idx;
    }
    oss << '/' << prefix << "_file_" << ino;
    return oss.str();
}

static void consume_digest(std::mt19937_64& rng, size_t length = 32) {
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (size_t i = 0; i < length; ++i) {
        (void)byte_dist(rng);
    }
}

static void consume_segments(size_t total_blocks,
                             size_t max_segments,
                             std::mt19937_64& rng) {
    if (total_blocks == 0) {
        total_blocks = 1;
    }
    size_t capped_segments = max_segments == 0 ? 1 : max_segments;
    std::uniform_int_distribution<size_t> seg_count_dist(1, capped_segments);
    size_t segment_count = std::min(total_blocks, seg_count_dist(rng));

    size_t blocks_left = total_blocks;
    for (size_t s = 0; s < segment_count; ++s) {
        size_t remaining_segments = segment_count - s;
        size_t max_for_segment = blocks_left - (remaining_segments - 1);
        std::uniform_int_distribution<size_t> len_dist(1, max_for_segment);
        size_t len = len_dist(rng);
        blocks_left -= len;
    }
}

static void consume_timestamps(TemperatureClass klass, std::mt19937_64& rng) {
    int min_days = 180;
    int max_days = 365;
    if (klass == TemperatureClass::Hot) {
        min_days = 0;
        max_days = 30;
    } else if (klass == TemperatureClass::Warm) {
        min_days = 60;
        max_days = 120;
    }

    std::uniform_int_distribution<int> day_dist(min_days, max_days);
    std::uniform_int_distribution<int> hour_dist(0, 23);
    std::uniform_int_distribution<int> minute_dist(0, 59);
    std::uniform_int_distribution<int> modify_extra(1, 14);
    std::uniform_int_distribution<int> create_extra(15, 60);

    (void)day_dist(rng);
    (void)hour_dist(rng);
    (void)minute_dist(rng);
    (void)modify_extra(rng);
    (void)hour_dist(rng);
    (void)minute_dist(rng);
    (void)create_extra(rng);
    (void)hour_dist(rng);
    (void)minute_dist(rng);
}

static std::string build_root_path(uint64_t batch_idx) {
    std::ostringstream oss;
    oss << "/dataset/batch_" << batch_idx;
    return oss.str();
}

static void apply_temperature_ratio(InodeStorage::BatchGenerationConfig& cfg, TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot:
            cfg.temp_ratio = {1.0, 0.0, 0.0};
            break;
        case TemperatureClass::Warm:
            cfg.temp_ratio = {0.0, 1.0, 0.0};
            break;
        case TemperatureClass::Cold:
        default:
            cfg.temp_ratio = {0.0, 0.0, 1.0};
            break;
    }
}

static uint8_t infer_node_type(TemperatureClass klass) {
    switch (klass) {
        case TemperatureClass::Hot: return 0;
        case TemperatureClass::Warm: return 1;
        case TemperatureClass::Cold:
        default: return 2;
    }
}

static uint16_t pick_node_id(uint64_t batch_idx) {
    constexpr uint16_t MAX_NODE_ID = 10'000;
    return static_cast<uint16_t>((batch_idx % MAX_NODE_ID) + 1);
}

struct GeneratedSample {
    std::string path;
    uint64_t expected_ino = 0;
};

struct RandomLookupResult {
    uint64_t requested = 0;
    uint64_t succeeded = 0;
    uint64_t failed = 0;
    std::chrono::nanoseconds total_cost{};

    double avg_us() const {
        if (requested == 0) return 0.0;
        return static_cast<double>(total_cost.count()) / 1000.0 / static_cast<double>(requested);
    }
};

static uint64_t get_inodes_per_chunk_from_chunk0(const std::string& inode_dir) {
    const fs::path chunk0 = fs::path(inode_dir) / "inode_chunk_0.bin";
    std::error_code ec;
    if (!fs::exists(chunk0, ec) || ec) {
        throw std::runtime_error("missing inode_chunk_0.bin: " + chunk0.string());
    }
    const uint64_t file_size = static_cast<uint64_t>(fs::file_size(chunk0, ec));
    if (ec || file_size == 0) {
        throw std::runtime_error("failed to stat inode_chunk_0.bin: " + chunk0.string());
    }
    return file_size / InodeStorage::INODE_DISK_SLOT_SIZE;
}

static RandomLookupResult RandomLookupNPathsViaGlobalKV(MdsServer& mds,
                                                        const MdsGlobalKVRealTesterConfig& opt) {
    RandomLookupResult res;
    res.requested = opt.random_lookup_n;
    if (opt.random_lookup_n == 0) return res;

    const uint64_t total_inodes = mds.GetTotalInodes();
    if (total_inodes == 0) {
        throw std::runtime_error("GetTotalInodes() == 0");
    }
    const uint64_t inodes_per_chunk = get_inodes_per_chunk_from_chunk0(opt.inode_dir);
    if (inodes_per_chunk == 0) {
        throw std::runtime_error("inodes_per_chunk == 0");
    }

    const uint64_t total_batches = (total_inodes + inodes_per_chunk - 1) / inodes_per_chunk;
    if (total_batches == 0) {
        throw std::runtime_error("total_batches == 0");
    }

    uint64_t actual_seed = opt.random_seed;
    if (actual_seed == 0) {
        std::random_device rd;
        actual_seed = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
    }
    std::mt19937_64 rng(actual_seed);

    const uint32_t batch_pool = std::min<uint64_t>(
        std::max<uint32_t>(1, opt.random_lookup_batches),
        total_batches);
    std::uniform_int_distribution<uint64_t> batch_dist(0, total_batches - 1);

    std::vector<uint64_t> chosen_batches;
    chosen_batches.reserve(batch_pool);
    {
        std::unordered_set<uint64_t> uniq;
        while (chosen_batches.size() < batch_pool) {
            uint64_t b = batch_dist(rng);
            if (uniq.insert(b).second) {
                chosen_batches.push_back(b);
            }
        }
    }
    std::uniform_int_distribution<size_t> pick_chosen(0, chosen_batches.size() - 1);

    struct Req {
        uint64_t batch = 0;
        uint64_t idx = 0;
        size_t out_pos = 0;
    };

    std::vector<GeneratedSample> samples(opt.random_lookup_n);
    std::vector<Req> reqs;
    reqs.reserve(opt.random_lookup_n);

    std::unordered_set<uint64_t> uniq_key;
    uniq_key.reserve(opt.random_lookup_n * 2);

    for (size_t i = 0; i < opt.random_lookup_n; ++i) {
        for (;;) {
            uint64_t b = chosen_batches[pick_chosen(rng)];
            const uint64_t start_ino = b * inodes_per_chunk;
            const uint64_t remain = (start_ino < total_inodes) ? (total_inodes - start_ino) : 0;
            const uint64_t batch_size = std::min<uint64_t>(inodes_per_chunk, remain);
            if (batch_size == 0) {
                continue;
            }
            std::uniform_int_distribution<uint64_t> idx_dist(0, batch_size - 1);
            uint64_t idx = idx_dist(rng);
            uint64_t key = (b << 20) | idx;
            if (!uniq_key.insert(key).second) {
                continue;
            }

            const uint64_t expected_ino = start_ino + idx;
            samples[i].expected_ino = expected_ino;
            reqs.push_back({b, idx, i});
            break;
        }
    }

    std::unordered_map<uint64_t, std::vector<Req>> by_batch;
    by_batch.reserve(chosen_batches.size() * 2);
    for (const auto& r : reqs) {
        by_batch[r.batch].push_back(r);
    }
    for (auto& kv : by_batch) {
        auto& vec = kv.second;
        std::sort(vec.begin(), vec.end(), [](const Req& a, const Req& b) { return a.idx < b.idx; });
    }

    for (auto& kv : by_batch) {
        const uint64_t b = kv.first;
        auto& vec = kv.second;
        if (vec.empty()) continue;

        InodeStorage::BatchGenerationConfig cfg;
        cfg.verbose = false;
        cfg.dir_depth = 4;
        cfg.dir_fanout = 8;
        cfg.root_path = build_root_path(b);
        cfg.random_seed = static_cast<uint32_t>(b + 12345);
        cfg.starting_inode = b * inodes_per_chunk;

        TemperatureClass klass = static_cast<TemperatureClass>(b % 3);
        apply_temperature_ratio(cfg, klass);
        cfg.node_distribution = {
            {pick_node_id(b), infer_node_type(klass), 1.0},
        };

        auto nodes = cfg.node_distribution;
        if (nodes.empty()) {
            nodes.push_back({0, 0, 0.4});
            nodes.push_back({200, 1, 0.4});
            nodes.push_back({4000, 2, 0.2});
        }
        std::vector<double> node_weights;
        node_weights.reserve(nodes.size());
        for (const auto& node : nodes) {
            node_weights.push_back(node.weight > 0.0 ? node.weight : 1.0);
        }
        std::discrete_distribution<size_t> node_picker(node_weights.begin(), node_weights.end());

        std::mt19937_64 batch_rng(static_cast<uint32_t>(cfg.random_seed));
        std::uniform_int_distribution<uint16_t> block_id_dist(0, std::numeric_limits<uint16_t>::max());

        const uint64_t max_idx = vec.back().idx;
        size_t next_req = 0;

        for (uint64_t idx = 0; idx <= max_idx; ++idx) {
            TemperatureClass temp = pick_temperature(cfg, batch_rng);
            (void)nodes[node_picker(batch_rng)];
            const auto& range = range_for_temperature(cfg, temp);
            uint64_t size_bytes = pick_size(range, batch_rng);
            (void)block_id_dist(batch_rng);

            const uint64_t ino = cfg.starting_inode + idx;
            std::string path = build_path_name(ino, temp, cfg, batch_rng);

            while (next_req < vec.size() && vec[next_req].idx == idx) {
                samples[vec[next_req].out_pos].path = path;
                ++next_req;
            }

            consume_digest(batch_rng, 32);
            const size_t block_size_bytes = cfg.block_size_bytes == 0 ? (4ULL * 1024 * 1024) : cfg.block_size_bytes;
            size_t block_count = static_cast<size_t>((size_bytes + block_size_bytes - 1) / block_size_bytes);
            consume_segments(block_count, cfg.max_segments, batch_rng);
            consume_timestamps(temp, batch_rng);
        }

        if (next_req != vec.size()) {
            throw std::runtime_error("path generation mismatch: not all requests satisfied for batch=" + std::to_string(b));
        }
    }

    auto stats_before = mds.GlobalKVStats();
    const auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < samples.size(); ++i) {
        const auto& s = samples[i];
        if (s.path.empty()) {
            ++res.failed;
            continue;
        }

        const auto one0 = std::chrono::steady_clock::now();
        auto inode_ptr = mds.FindInodeByPath(s.path);
        const auto one1 = std::chrono::steady_clock::now();
        const auto cost_us = std::chrono::duration_cast<std::chrono::microseconds>(one1 - one0).count();

        bool ok = (inode_ptr && inode_ptr->inode == s.expected_ino);
        if (ok) {
            ++res.succeeded;
        } else {
            ++res.failed;
        }

        if (i < opt.random_print_limit) {
            std::cout << "[random_lookup] i=" << i
                      << " cost_us=" << cost_us
                      << " path=" << s.path
                      << " expected_ino=" << s.expected_ino
                      << " got_ino=" << (inode_ptr ? inode_ptr->inode : static_cast<uint64_t>(-1))
                      << "\n";
            if (inode_ptr) {
                PrintInodeDumpStyle(*inode_ptr, i);
            }
        }
    }
    const auto t1 = std::chrono::steady_clock::now();
    res.total_cost = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0);
    auto stats_after = mds.GlobalKVStats();

    std::cout << "[random_lookup] GlobalKVStats hits=" << stats_after.first
              << " misses=" << stats_after.second
              << " (delta_hits=" << (stats_after.first - stats_before.first)
              << " delta_misses=" << (stats_after.second - stats_before.second) << ")\n";
    return res;
}

static std::optional<Inode> probe_one_inode_from_chunk0(const MdsGlobalKVRealTesterConfig& opt) {
    const fs::path chunk0 = fs::path(opt.inode_dir) / "inode_chunk_0.bin";
    std::ifstream in(chunk0, std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "[probe] failed to open " << chunk0 << "\n";
        return std::nullopt;
    }

    std::vector<uint8_t> slot(InodeStorage::INODE_DISK_SLOT_SIZE, 0);
    for (uint64_t i = 0; i < opt.probe_slots; ++i) {
        if (!in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot.size()))) {
            break;
        }
        Inode inode;
        size_t offset = 0;
        if (!Inode::deserialize(slot.data(), offset, inode, slot.size())) {
            continue;
        }
        if (inode.filename.empty() || inode.filename.front() != '/') {
            continue;
        }
        if (inode.inode == 0 || inode.inode == static_cast<uint64_t>(-1)) {
            continue;
        }
        return inode;
    }

    std::cerr << "[probe] no valid inode found in first " << opt.probe_slots << " slots\n";
    return std::nullopt;
}

static std::optional<Inode> probe_one_directory_inode_from_chunk0(const MdsGlobalKVRealTesterConfig& opt) {
    const fs::path chunk0 = fs::path(opt.inode_dir) / "inode_chunk_0.bin";
    std::ifstream in(chunk0, std::ios::binary);
    if (!in.is_open()) {
        return std::nullopt;
    }

    std::vector<uint8_t> slot(InodeStorage::INODE_DISK_SLOT_SIZE, 0);
    for (uint64_t i = 0; i < opt.probe_slots; ++i) {
        if (!in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot.size()))) {
            break;
        }
        Inode inode;
        size_t offset = 0;
        if (!Inode::deserialize(slot.data(), offset, inode, slot.size())) {
            continue;
        }
        if (inode.filename.empty() || inode.filename.front() != '/') {
            continue;
        }
        if (inode.file_mode.fields.file_type != static_cast<uint8_t>(FileType::Directory)) {
            continue;
        }
        if (inode.inode == 0 || inode.inode == static_cast<uint64_t>(-1)) {
            continue;
        }
        return inode;
    }
    return std::nullopt;
}

class MdsGlobalKVRealTester {
public:
    using Config = MdsGlobalKVRealTesterConfig;

    explicit MdsGlobalKVRealTester(Config opt) : opt_(std::move(opt)) {}

    bool Init(std::string* err = nullptr) {
        if (!fs::exists(opt_.inode_dir) || !fs::is_directory(opt_.inode_dir)) {
            set_error(err, "inode_dir missing or not a directory: " + opt_.inode_dir);
            return false;
        }
        if (!fs::exists(opt_.bitmap_path)) {
            set_error(err, "bitmap_path missing: " + opt_.bitmap_path);
            return false;
        }
        if (!fs::exists(opt_.rocksdb_global) || !fs::is_directory(opt_.rocksdb_global)) {
            set_error(err, "rocksdb_global missing or not a directory: " + opt_.rocksdb_global);
            return false;
        }

        ensure_dir(opt_.dir_store_base);
        mds_ = std::make_unique<MdsServer>(
            opt_.inode_dir,
            opt_.bitmap_path,
            opt_.dir_store_base,
            /*create_new=*/false);

        if (!mds_->BindGlobalPathKV(opt_.rocksdb_global)) {
            set_error(err, "BindGlobalPathKV failed: " + opt_.rocksdb_global);
            return false;
        }
        return true;
    }

    RandomLookupResult RunRandomLookup(uint64_t n) {
        if (!mds_) {
            throw std::runtime_error("MdsGlobalKVRealTester not initialized");
        }
        Config run = opt_;
        run.random_lookup_n = n;

        if (run.random_lookup_warmup_n > 0) {
            Config warm = run;
            warm.random_lookup_n = run.random_lookup_warmup_n;
            warm.random_print_limit = 0;
            (void)RandomLookupNPathsViaGlobalKV(*mds_, warm);
        }
        return RandomLookupNPathsViaGlobalKV(*mds_, run);
    }

    bool RunBasicVerification() {
        auto inode = probe_one_inode_from_chunk0(opt_);
        if (!inode) {
            std::cerr << u8"探测不到可用 inode，无法验证全局 KV 的 path lookup\n";
            return false;
        }

        const std::string test_path = inode->filename;
        auto stats_before = mds_->GlobalKVStats();
        uint64_t ino = mds_->LookupIno(test_path);
        auto stats_after = mds_->GlobalKVStats();

        std::cout << "[test] path=" << test_path << " expected_ino=" << inode->inode << " got_ino=" << ino << "\n";
        std::cout << "[test] GlobalKVStats hits=" << stats_after.first << " misses=" << stats_after.second << "\n";

        if (ino != inode->inode) return false;
        if (stats_after.first < stats_before.first + 1) return false;

        auto inode_ptr = mds_->FindInodeByPath(test_path);
        if (!inode_ptr || inode_ptr->inode != inode->inode) return false;

        bool ls_root_ok = mds_->Ls("/");
        std::cout << "[test] Ls('/') ok=" << (ls_root_ok ? "true" : "false") << "\n";

        auto dir_inode = probe_one_directory_inode_from_chunk0(opt_);
        if (dir_inode) {
            auto s0 = mds_->GlobalKVStats();
            bool ok = mds_->Ls(dir_inode->filename);
            auto s1 = mds_->GlobalKVStats();
            std::cout << "[test] Ls(dir_path) path=" << dir_inode->filename
                      << " ino=" << dir_inode->inode
                      << " ok=" << (ok ? "true" : "false")
                      << " hits=" << s1.first << " misses=" << s1.second << "\n";
            if (!ok) return false;
            if (s1.first < s0.first + 1) return false;
        } else {
            std::cout << u8"[test] 在探测范围内未发现目录 inode，跳过 Ls(dir_path) 验证\n";
        }
        return true;
    }

private:
    static void set_error(std::string* err, const std::string& msg) {
        if (err) {
            *err = msg;
        }
    }

    Config opt_;
    std::unique_ptr<MdsServer> mds_;
};

std::string PromptLine(const std::string& tip) {
    std::cout << tip;
    std::string line;
    std::getline(std::cin, line);
    return line;
}

uint64_t PromptUint64(const std::string& tip, uint64_t default_value = 0) {
    while (true) {
        std::string line = PromptLine(tip);
        if (line.empty()) {
            return default_value;
        }
        try {
            return std::stoull(line);
        } catch (...) {
            std::cout << u8"输入无效，请重新输入。\n";
        }
    }
}

std::string MakeMountedPath(const std::string& input) {
    if (input.empty()) {
        return FLAGS_mount_point;
    }
    if (!FLAGS_mount_point.empty() && FLAGS_mount_point.back() == '/') {
        if (!input.empty() && input.front() == '/') {
            return FLAGS_mount_point + input.substr(1);
        }
        return FLAGS_mount_point + input;
    }
    if (!input.empty() && input.front() == '/') {
        return FLAGS_mount_point + input;
    }
    return FLAGS_mount_point + "/" + input;
}

bool MatchesStartFile(const fs::path& path, const std::string& start) {
    if (start.empty()) return true;
    if (path.string() == start) return true;
    return path.filename().string() == start;
}

void Consume(std::vector<DeviceState>& devices, uint64_t& remaining) {
    for (auto& dev : devices) {
        if (remaining == 0) return;
        uint64_t free = dev.capacity > dev.used ? dev.capacity - dev.used : 0;
        if (free == 0) continue;
        uint64_t take = free < remaining ? free : remaining;
        dev.used += take;
        remaining -= take;
    }
}

bool CanDeserializeInode(const uint8_t* data, size_t total_size) {
    size_t offset = 0;
    auto safe_read = [&](void* dest, size_t len) -> bool {
        if (offset + len > total_size) return false;
        std::memcpy(dest, data + offset, len);
        offset += len;
        return true;
    };

    Inode inode;
    if (!safe_read(&inode.location_id.raw, sizeof(inode.location_id.raw))) return false;
    if (!safe_read(&inode.block_id, sizeof(inode.block_id))) return false;
    if (!safe_read(&inode.filename_len, sizeof(inode.filename_len))) return false;
    if (!safe_read(&inode.digest_len, sizeof(inode.digest_len))) return false;
    if (!safe_read(&inode.file_mode.raw, sizeof(inode.file_mode.raw))) return false;
    if (!safe_read(&inode.file_size.raw, sizeof(inode.file_size.raw))) return false;
    if (!safe_read(&inode.inode, sizeof(inode.inode))) return false;
    if (offset + Inode::kNamespaceIdLen > total_size) return false;
    offset += Inode::kNamespaceIdLen;
    if (!safe_read(&inode.fm_time, sizeof(inode.fm_time))) return false;
    if (!safe_read(&inode.fa_time, sizeof(inode.fa_time))) return false;
    if (!safe_read(&inode.im_time, sizeof(inode.im_time))) return false;
    if (!safe_read(&inode.fc_time, sizeof(inode.fc_time))) return false;

    if (offset + inode.filename_len + inode.digest_len > total_size) return false;
    offset += inode.filename_len + inode.digest_len;

    uint8_t volume_id_len = 0;
    if (!safe_read(&volume_id_len, sizeof(volume_id_len))) return false;
    if (offset + volume_id_len > total_size) return false;
    offset += volume_id_len;

    uint32_t segment_count = 0;
    if (!safe_read(&segment_count, sizeof(segment_count))) return false;
    if (offset + static_cast<size_t>(segment_count) * sizeof(BlockSegment) > total_size) return false;
    return true;
}

uint64_t DetectSlotSize(std::ifstream& in, uint64_t file_size) {
    std::vector<uint64_t> candidates = {
        InodeStorage::INODE_DISK_SLOT_SIZE,
        1024,
        2048,
        4096
    };
    candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
    uint64_t max_candidate = 0;
    for (auto v : candidates) {
        if (v > max_candidate) max_candidate = v;
    }
    uint64_t read_size = max_candidate;
    if (file_size > 0 && file_size < read_size) {
        read_size = file_size;
    }
    if (read_size == 0) {
        return 0;
    }
    std::vector<uint8_t> buf(static_cast<size_t>(read_size));
    in.clear();
    in.seekg(0, std::ios::beg);
    in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    std::streamsize got = in.gcount();
    if (got <= 0) {
        return 0;
    }
    for (auto candidate : candidates) {
        if (candidate > static_cast<uint64_t>(got)) {
            continue;
        }
        if (CanDeserializeInode(buf.data(), static_cast<size_t>(candidate))) {
            return candidate;
        }
    }
    return 0;
}

class SystemTester {
public:
    bool Init() {
        InitSimState();
        if (!FLAGS_mount_point.empty() && !fs::exists(FLAGS_mount_point)) {
            std::cout << u8"提示：挂载点不存在或未挂载：" << FLAGS_mount_point << "\n";
        }
        return true;
    }

    void RunMenu() {
        while (true) {
            PrintMenu();
            std::string choice = PromptLine(u8"请选择操作（输入 q 退出）：");
            if (choice == "q" || choice == "Q") {
                break;
            }
            HandleMenu(choice);
        }
    }

private:
    void InitSimState() {
        InitSimNodes();
        if (!FLAGS_inode_dir.empty()) {
            ScanInodeDir();
        }
    }

    uint64_t TotalFileCount() const {
        return AddSaturating(kInitialFileCount, sim_stats_.inodes);
    }

    uint64_t TotalFileBytes() const {
        return AddSaturating(kInitialFileBytes, sim_stats_.bytes);
    }

    void InitSimNodes() {
        sim_nodes_.clear();
        sim_nodes_.reserve(FLAGS_ssd_nodes + FLAGS_hdd_nodes + FLAGS_mix_nodes);

        for (uint32_t i = 0; i < FLAGS_ssd_nodes; ++i) {
            NodeState node;
            node.node_id = "node_ssd_" + std::to_string(i);
            node.type = 0;
            for (uint32_t d = 0; d < FLAGS_ssd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
                dev.capacity = FLAGS_ssd_capacity_bytes;
                node.ssd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        for (uint32_t i = 0; i < FLAGS_hdd_nodes; ++i) {
            NodeState node;
            node.node_id = "node_hdd_" + std::to_string(i);
            node.type = 1;
            for (uint32_t d = 0; d < FLAGS_hdd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
                dev.capacity = FLAGS_hdd_capacity_bytes;
                node.hdd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        for (uint32_t i = 0; i < FLAGS_mix_nodes; ++i) {
            NodeState node;
            node.node_id = "node_mix_" + std::to_string(i);
            node.type = 2;
            for (uint32_t d = 0; d < FLAGS_ssd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_SSD_" + std::to_string(d);
                dev.capacity = FLAGS_ssd_capacity_bytes;
                node.ssd_devices.push_back(std::move(dev));
            }
            for (uint32_t d = 0; d < FLAGS_hdd_devices_per_node; ++d) {
                DeviceState dev;
                dev.device_id = node.node_id + "_HDD_" + std::to_string(d);
                dev.capacity = FLAGS_hdd_capacity_bytes;
                node.hdd_devices.push_back(std::move(dev));
            }
            sim_nodes_.push_back(std::move(node));
        }

        sim_node_index_.clear();
        sim_node_index_.reserve(sim_nodes_.size());
        for (size_t i = 0; i < sim_nodes_.size(); ++i) {
            sim_node_index_[sim_nodes_[i].node_id] = i;
        }
    }

    void ScanInodeDir() {
        bin_files_.clear();
        if (FLAGS_inode_dir.empty()) {
            return;
        }
        for (const auto& entry : fs::directory_iterator(FLAGS_inode_dir)) {
            if (!entry.is_regular_file()) continue;
            if (entry.path().extension() == ".bin") {
                bin_files_.push_back(entry.path());
            }
        }
        std::sort(bin_files_.begin(), bin_files_.end());
        if (!FLAGS_start_file.empty()) {
            bool found = false;
            for (size_t i = 0; i < bin_files_.size(); ++i) {
                if (MatchesStartFile(bin_files_[i], FLAGS_start_file)) {
                    current_bin_file_idx_ = i;
                    current_file_offset_ = FLAGS_start_index;
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cout << u8"起始文件未找到：" << FLAGS_start_file << "\n";
            }
        }
    }

    bool ApplyInodeSim(const Inode& inode, uint64_t file_size) {
        const uint8_t type = static_cast<uint8_t>(inode.location_id.fields.node_type & 0x03);
        const uint16_t node_id = inode.location_id.fields.node_id;
        std::string node_name;
        if (type == 0) {
            node_name = "node_ssd_" + std::to_string(node_id);
        } else if (type == 1) {
            node_name = "node_hdd_" + std::to_string(node_id);
        } else {
            node_name = "node_mix_" + std::to_string(node_id);
        }

        auto it = sim_node_index_.find(node_name);
        if (it == sim_node_index_.end()) {
            ++sim_stats_.missing_node;
            return false;
        }
        auto& node = sim_nodes_[it->second];
        uint64_t remaining = file_size;
        if (node.type == 0) {
            Consume(node.ssd_devices, remaining);
        } else if (node.type == 1) {
            Consume(node.hdd_devices, remaining);
        } else {
            Consume(node.ssd_devices, remaining);
            if (remaining > 0) {
                Consume(node.hdd_devices, remaining);
            }
        }
        if (remaining > 0) {
            ++sim_stats_.failed;
            return false;
        }
        return true;
    }

    bool SimBackup(uint64_t count) {
        if (bin_files_.empty()) {
            std::cout << u8"找不到 inode 批量文件，请设置 --inode_dir。\n";
            return false;
        }

        uint64_t processed = 0;
        uint64_t failed = 0;
        uint64_t batch_bytes = 0;
        auto start = std::chrono::steady_clock::now();

        while (processed < count && current_bin_file_idx_ < bin_files_.size()) {
            const auto& path = bin_files_[current_bin_file_idx_];
            std::ifstream in(path, std::ios::binary);
            if (!in.is_open()) {
                std::cout << u8"无法打开 inode 文件：" << path.string() << "\n";
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }
            in.seekg(0, std::ios::end);
            const std::streamoff total_bytes = in.tellg();
            if (total_bytes <= 0) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }

            const uint64_t slot_size = DetectSlotSize(in, static_cast<uint64_t>(total_bytes));
            if (slot_size == 0) {
                std::cout << u8"无法识别 inode 槽大小，跳过文件：" << path.string() << "\n";
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }

            const uint64_t total_slots = static_cast<uint64_t>(total_bytes) / slot_size;
            if (current_file_offset_ >= total_slots) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
                continue;
            }
            in.clear();
            in.seekg(static_cast<std::streamoff>(current_file_offset_ * slot_size), std::ios::beg);

            std::vector<uint8_t> slot(static_cast<size_t>(slot_size));
            while (processed < count && current_file_offset_ < total_slots) {
                in.read(reinterpret_cast<char*>(slot.data()), static_cast<std::streamsize>(slot_size));
                if (in.gcount() != static_cast<std::streamsize>(slot_size)) {
                    current_file_offset_ = total_slots;
                    std::cout << u8"读取到文件尾或读取失败，切换到下一个文件：" << path.filename().string() << "\n";
                    break;
                }
                size_t off = 0;
                Inode inode;
                if (!Inode::deserialize(slot.data(), off, inode, static_cast<size_t>(slot_size))) {
                    ++sim_stats_.failed;
                    ++failed;
                    ++processed;
                    ++current_file_offset_;
                    continue;
                }
                ++sim_stats_.inodes;
                const uint64_t bytes = MapFileSizeFromMeta(inode.getFileSize(), inode.inode);
                sim_stats_.bytes += bytes;
                batch_bytes += bytes;
                ApplyInodeSim(inode, bytes);
                ++processed;
                ++current_file_offset_;
            }
            if (current_file_offset_ >= total_slots) {
                ++current_bin_file_idx_;
                current_file_offset_ = 0;
            }
        }

        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        uint64_t total_files = TotalFileCount();
        uint64_t total_bytes = TotalFileBytes();
        std::cout << u8"已经成功备份 " << processed << u8" 个文件。\n";
        std::cout << u8"执行时间：" << elapsed << u8" ms。\n";
        std::cout << u8"当前已处理文件数量：" << total_files
                  << u8"，累计模拟写入：" << FormatBytes(total_bytes) << "\n";
        uint64_t batch_avg = 0;
        if (processed > 0) {
            batch_avg = batch_bytes / processed;
        }
        std::cout << u8"本次备份文件总大小：" << FormatBytes(batch_bytes) << "\n";
        std::cout << u8"本次备份平均文件大小：" << FormatBytes(batch_avg) << "\n";
        std::cout << u8"本次失败解析数量：" << failed << "\n";
        if (sim_stats_.inodes == 0 && failed > 0) {
            std::cout << u8"提示：当前文件全部解析失败，请确认 inode 批量文件版本与工具一致。\n";
        }
        return true;
    }

    void SimCountFileNum() {
        uint64_t total_files = TotalFileCount();
        if (total_files == 0) {
            std::cout << u8"当前没有已处理的文件。\n";
            return;
        }
        uint64_t total_bytes = TotalFileBytes();
        long double avg = static_cast<long double>(total_bytes) / static_cast<long double>(total_files);
        uint64_t used_nodes = 0;
        for (const auto& node : sim_nodes_) {
            uint64_t used = 0;
            for (const auto& dev : node.ssd_devices) used += dev.used;
            for (const auto& dev : node.hdd_devices) used += dev.used;
            if (used > 0) ++used_nodes;
        }
        if (total_files > 0) {
            used_nodes += kOpticalNodeCount;
        }
        std::cout << u8"系统中包含文件数量为：" << total_files << "\n";
        std::cout << u8"命名空间数量为：" << kNamespaceCount << "\n";
        std::cout << u8"平均文件大小为：" << FormatBytes(static_cast<uint64_t>(avg)) << "\n";
        std::cout << u8"文件总大小为：" << FormatBytes(total_bytes) << "\n";
        std::cout << u8"使用存储节点数量为：" << used_nodes << "\n";
    }

    void SimQueryFile(const std::string& path) {
        uint64_t size = RandomFileSizeBytes();
        double delay_ms = (static_cast<double>(size) / (100.0 * 1024 * 1024)) * 10.0;
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay_ms)));
        std::cout << u8"SimTrue=1 表示从仿真系统查询。\n";
        std::cout << u8"文件=" << path
                  << u8"，模拟大小=" << FormatBytes(size)
                  << u8"，模拟耗时=" << delay_ms << "ms\n";
    }

    void SimWriteFile(const std::string& source_path) {
        std::error_code ec;
        uint64_t size = fs::file_size(source_path, ec);
        if (ec) {
            std::cout << u8"读取源文件失败：" << ec.message() << "\n";
            return;
        }
        double seconds = static_cast<double>(size) / (50.0 * 1024 * 1024);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(seconds * 1000)));
        std::string node_id = sim_nodes_.empty() ? "none" : sim_nodes_[size % sim_nodes_.size()].node_id;
        std::cout << u8"SimTrue=1 表示仿真写入。\n";
        std::cout << u8"写入大小=" << FormatBytes(size)
                  << u8"，写入存储节点ID=" << node_id << "\n";
    }

    void RealWriteContent(const std::string& dest_path, const std::string& content) {
        std::string real_dest = MakeMountedPath(dest_path);
        int out_fd = ::open(real_dest.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (out_fd < 0) {
            std::cout << u8"[ERROR] 打开目标文件失败：" << real_dest
                      << u8"，错误=" << std::strerror(errno) << "\n";
            return;
        }
        size_t offset = 0;
        const size_t total = content.size();
        while (offset < total) {
            ssize_t n = ::write(out_fd, content.data() + offset, total - offset);
            if (n <= 0) {
                std::cout << u8"[ERROR] 写入失败，错误=" << std::strerror(errno) << "\n";
                ::close(out_fd);
                return;
            }
            offset += static_cast<size_t>(n);
        }
        ::close(out_fd);
        std::cout << u8"==== 操作结果 ====\n";
        std::cout << u8"SimTrue=0 表示真实写入。\n";
        std::cout << u8"目标文件=" << real_dest
                  << u8"，写入字节数=" << total << "\n";
    }

    void RealReadFile(const std::string& path) {
        std::string real_path = MakeMountedPath(path);
        int fd = ::open(real_path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cout << u8"[ERROR] 读取失败：" << real_path
                      << u8"，错误=" << std::strerror(errno) << "\n";
            return;
        }
        struct stat st;
        if (fstat(fd, &st) != 0) {
            std::cout << u8"[ERROR] 获取文件大小失败：" << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        const uint64_t max_read = 4ULL * 1024 * 1024;
        uint64_t to_read = static_cast<uint64_t>(st.st_size);
        bool truncated = false;
        if (to_read > max_read) {
            to_read = max_read;
            truncated = true;
        }
        std::vector<char> buf(static_cast<size_t>(to_read));
        ssize_t n = ::read(fd, buf.data(), buf.size());
        if (n < 0) {
            std::cout << u8"[ERROR] 读取失败，错误=" << std::strerror(errno) << "\n";
            ::close(fd);
            return;
        }
        ::close(fd);
        std::cout << u8"==== 操作结果 ====\n";
        std::cout << u8"SimTrue=0 表示真实读取。\n";
        std::cout << u8"文件=" << real_path << u8"，读取字节数=" << n;
        if (truncated) {
            std::cout << u8"（内容过大，已截断）";
        }
        std::cout << "\n";
        if (n > 0) {
            std::string out(buf.data(), buf.data() + n);
            std::cout << u8"内容：\n" << out << "\n";
        }
    }

    void RunGlobalKVRealTest(uint64_t lookup_n) {
        MdsGlobalKVRealTester::Config opt;
        opt.inode_dir = FLAGS_kv_inode_dir;
        if (opt.inode_dir.empty()) {
            opt.inode_dir = FLAGS_inode_dir;
        }
        opt.bitmap_path = FLAGS_kv_bitmap_path;
        opt.rocksdb_global = FLAGS_kv_rocksdb_global;
        opt.dir_store_base = FLAGS_kv_dir_store;
        opt.probe_slots = FLAGS_kv_probe_slots;
        opt.random_lookup_n = lookup_n;
        opt.random_lookup_batches = FLAGS_kv_random_lookup_batches;
        opt.random_print_limit = 0;
        opt.random_lookup_warmup_n = 0;
        opt.random_seed = FLAGS_kv_random_seed;

        if (opt.bitmap_path.empty()) {
            opt.bitmap_path = (fs::path(opt.inode_dir) / "inode_bitmap.bin").string();
        }
        if (opt.rocksdb_global.empty()) {
            opt.rocksdb_global = (fs::path(opt.inode_dir) / "rocksdb_global").string();
        }

        MdsGlobalKVRealTester tester(opt);
        std::string err;
        if (!tester.Init(&err)) {
            std::cerr << err << "\n";
            return;
        }

        auto result = tester.RunRandomLookup(opt.random_lookup_n);
        double avg_ms = 0.0;
        if (result.requested > 0) {
            avg_ms = static_cast<double>(result.total_cost.count()) / 1e6
                / static_cast<double>(result.requested);
        }
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(3) << avg_ms;
        std::cout << u8"平均时延：" << oss.str() << u8" ms\n";
    }

    void PrintMenu() {
        std::cout << "\n";
        std::cout << u8"========== ZBSystemTest 菜单 ==========\n";
        std::cout << u8"1) 批量备份（仿真）\n";
        std::cout << u8"2) 统计文件数量（仿真）\n";
        std::cout << u8"3) 统计总容量（仿真）\n";
        std::cout << u8"4) 统计节点数量（仿真）\n";
        std::cout << u8"5) 统计光盘库数量（仿真）\n";
        std::cout << u8"6) 统计光盘数量（仿真）\n";
        std::cout << u8"7) 读取文件（仿真）\n";
        std::cout << u8"8) 写入文件（仿真）\n";
        std::cout << u8"9) 真实写入文件（POSIX，输入路径+内容）\n";
        std::cout << u8"10) 真实读取文件（POSIX，输入路径）\n";
        std::cout << u8"11) 全局KV查询（真实，随机N次）\n";
        std::cout << u8"q) 退出\n";
        std::cout << u8"当前客户端挂载点：" << FLAGS_mount_point << "\n";
    }

    void CountTotalCapacityStorage() {
        uint64_t ssd = 0;
        uint64_t hdd = 0;
        for (const auto& node : sim_nodes_) {
            for (const auto& dev : node.ssd_devices) ssd += dev.capacity;
            for (const auto& dev : node.hdd_devices) hdd += dev.capacity;
        }
        long double optical = CalcOpticalCapacityBytes();
        long double total = static_cast<long double>(ssd) + static_cast<long double>(hdd) + optical;
        std::cout << u8"固态盘容量：" << FormatBytes(ssd) << "\n";
        std::cout << u8"磁盘容量：" << FormatBytes(hdd) << "\n";
        std::cout << u8"光存储容量：" << FormatBytesLongDouble(optical) << "\n";
        std::cout << u8"总容量：" << FormatBytesLongDouble(total) << "\n";
    }

    void CountStorageNodeNum() {
        uint64_t optical_nodes = kOpticalNodeCount;
        uint64_t hdd_capacity_tb = kDiskDeviceBytes / kTiB;
        std::cout << u8"磁盘节点数量：" << kDiskNodeCount
                  << u8"（每节点" << hdd_capacity_tb << u8"TB磁盘"
                  << kDiskDevicesPerNode << u8"个）\n";
        double per_100g = static_cast<double>(kDisc100GCount) / static_cast<double>(kDiscLibraryCount);
        double per_1t = static_cast<double>(kDisc1TCount) / static_cast<double>(kDiscLibraryCount);
        double per_10t = static_cast<double>(kDisc10TCount) / static_cast<double>(kDiscLibraryCount);
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2);
        oss << u8"光盘库节点数量：" << optical_nodes
            << u8"（每库100G光盘" << per_100g
            << u8"个，1TB光盘" << per_1t
            << u8"个，10TB光盘" << per_10t << u8"个）";
        std::cout << oss.str() << "\n";
    }

    void CountDiscLibNum() {
        double per_100g = static_cast<double>(kDisc100GCount) / static_cast<double>(kDiscLibraryCount);
        double per_1t = static_cast<double>(kDisc1TCount) / static_cast<double>(kDiscLibraryCount);
        double per_10t = static_cast<double>(kDisc10TCount) / static_cast<double>(kDiscLibraryCount);
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2);
        oss << u8"光盘库数量：" << kDiscLibraryCount
            << u8"（每库100G光盘" << per_100g
            << u8"个，1TB光盘" << per_1t
            << u8"个，10TB光盘" << per_10t << u8"个）";
        std::cout << oss.str() << "\n";
    }

    void CountDiscNum() {
        std::cout << u8"光盘数量：" << kDiscTotalCount
                  << u8"（100G光盘" << kDisc100GCount
                  << u8"个，1TB光盘" << kDisc1TCount
                  << u8"个，10TB光盘" << kDisc10TCount << u8"个）\n";
    }

    void HandleMenu(const std::string& choice) {
        if (choice == "1") {
            uint64_t count = PromptUint64(u8"请输入要备份的文件数量：");
            SimBackup(count);
            return;
        }
        if (choice == "2") {
            SimCountFileNum();
            return;
        }
        if (choice == "3") {
            CountTotalCapacityStorage();
            return;
        }
        if (choice == "4") {
            CountStorageNodeNum();
            return;
        }
        if (choice == "5") {
            CountDiscLibNum();
            return;
        }
        if (choice == "6") {
            CountDiscNum();
            return;
        }
        if (choice == "7") {
            std::string path = PromptLine(u8"请输入要读取的文件路径（如 /path/file）：");
            if (!path.empty()) {
                SimQueryFile(path);
            }
            return;
        }
        if (choice == "8") {
            std::string src = PromptLine(u8"请输入本地源文件路径：");
            if (!src.empty()) {
                SimWriteFile(src);
            }
            return;
        }
        if (choice == "9") {
            std::string dest = PromptLine(u8"请输入写入路径（挂载点内，如 /hello.txt）：");
            if (dest.empty()) return;
            std::string content = PromptLine(u8"请输入要写入的内容（单行）：");
            RealWriteContent(dest, content);
            return;
        }
        if (choice == "10") {
            std::string path = PromptLine(u8"请输入要读取的文件路径（挂载点内，如 /hello.txt）：");
            if (path.empty()) return;
            RealReadFile(path);
            return;
        }
        if (choice == "11") {
            uint64_t n = PromptUint64(u8"请输入随机查询次数（1-10）：");
            if (n < 1 || n > 10) {
                std::cout << u8"输入无效，范围为1-10。\n";
                return;
            }
            RunGlobalKVRealTest(n);
            return;
        }
        std::cout << u8"未知选项：" << choice << "\n";
    }

    std::vector<NodeState> sim_nodes_;
    std::unordered_map<std::string, size_t> sim_node_index_;
    Stats sim_stats_;
    std::vector<fs::path> bin_files_;
    size_t current_bin_file_idx_{0};
    uint64_t current_file_offset_{0};
};

} // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    SystemTester tester;
    if (!tester.Init()) {
        return 1;
    }

    tester.RunMenu();
    return 0;
}