#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "real_node.pb.h"

DEFINE_string(servers, "127.0.0.1:19080,127.0.0.1:19081,127.0.0.1:19082", "Comma-separated real node endpoints");
DEFINE_string(disks, "disk-01,disk-02,disk-03", "Comma-separated disk ids to test");
DEFINE_string(config_files, "", "Comma-separated config file paths, must match servers order when verify_fs is true");
DEFINE_bool(verify_fs, false, "Verify data by reading from local disk paths");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");

namespace {

namespace fs = std::filesystem;

std::vector<std::string> SplitCsv(const std::string& input) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(input);
    while (std::getline(stream, token, ',')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

std::string ToHex(uint64_t value, size_t width) {
    static const char kHex[] = "0123456789abcdef";
    std::string out(width, '0');
    for (size_t i = 0; i < width; ++i) {
        size_t shift = (width - 1 - i) * 4;
        out[i] = kHex[(value >> shift) & 0xF];
    }
    return out;
}

std::string MakeChunkId(const std::string& server, const std::string& disk) {
    std::hash<std::string> hasher;
    uint64_t h1 = static_cast<uint64_t>(hasher(server));
    uint64_t h2 = static_cast<uint64_t>(hasher(disk));
    return ToHex(h1, 16) + ToHex(h2, 16);
}

std::string Trim(std::string value) {
    auto is_space = [](unsigned char ch) { return std::isspace(ch); };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(),
                                            [&](unsigned char ch) { return !is_space(ch); }));
    value.erase(std::find_if(value.rbegin(), value.rend(),
                             [&](unsigned char ch) { return !is_space(ch); }).base(),
                value.end());
    return value;
}

std::string BuildPrefix(const std::string& chunk_id) {
    std::string prefix;
    prefix.reserve(4);
    for (char ch : chunk_id) {
        if (prefix.size() >= 4) {
            break;
        }
        if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
            if (ch >= 'A' && ch <= 'F') {
                prefix.push_back(static_cast<char>(ch - 'A' + 'a'));
            } else {
                prefix.push_back(ch);
            }
        }
    }
    while (prefix.size() < 4) {
        prefix.push_back('0');
    }
    return prefix;
}

std::string ResolveLocalPath(const std::string& root_path, const std::string& chunk_id) {
    if (root_path.empty() || chunk_id.empty()) {
        return {};
    }
    std::string prefix = BuildPrefix(chunk_id);
    std::string level1 = prefix.substr(0, 2);
    std::string level2 = prefix.substr(2, 2);
    fs::path dir_path = fs::path(root_path) / level1 / level2;
    fs::path file_path = dir_path / chunk_id;
    return file_path.string();
}

std::unordered_map<std::string, std::string> ParseDiskMap(const std::string& line_value) {
    std::unordered_map<std::string, std::string> mapping;
    std::istringstream stream(line_value);
    std::string token;
    while (std::getline(stream, token, ';')) {
        token = Trim(token);
        if (token.empty()) {
            continue;
        }
        size_t sep = token.find(':');
        if (sep == std::string::npos) {
            continue;
        }
        std::string id = Trim(token.substr(0, sep));
        std::string path = Trim(token.substr(sep + 1));
        if (!id.empty() && !path.empty()) {
            mapping[id] = path;
        }
    }
    return mapping;
}

std::unordered_map<std::string, std::string> LoadDiskMapFromDataRoot(const std::string& root) {
    std::unordered_map<std::string, std::string> mapping;
    fs::path root_path(root);
    if (!fs::exists(root_path) || !fs::is_directory(root_path)) {
        return mapping;
    }
    for (const auto& entry : fs::directory_iterator(root_path)) {
        if (!entry.is_directory()) {
            continue;
        }
        std::string mount_point = entry.path().string();
        std::string disk_id;
        fs::path id_path = entry.path() / ".disk_id";
        if (fs::exists(id_path)) {
            std::ifstream input(id_path.string());
            if (input) {
                std::getline(input, disk_id);
                disk_id = Trim(disk_id);
            }
        }
        if (disk_id.empty()) {
            disk_id = entry.path().filename().string();
        }
        mapping[disk_id] = mount_point;
    }
    return mapping;
}

bool LoadDiskMapping(const std::string& config_path, std::unordered_map<std::string, std::string>* mapping) {
    if (!mapping) {
        return false;
    }
    mapping->clear();

    std::ifstream input(config_path);
    if (!input) {
        return false;
    }

    std::string line;
    std::string data_root;
    while (std::getline(input, line)) {
        std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        std::string key = Trim(trimmed.substr(0, eq));
        std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "ZB_DISKS") {
            *mapping = ParseDiskMap(value);
        } else if (key == "DATA_ROOT") {
            data_root = value;
        }
    }

    if (mapping->empty() && !data_root.empty()) {
        *mapping = LoadDiskMapFromDataRoot(data_root);
    }

    return !mapping->empty();
}

bool StatusOk(const zb::rpc::Status& status) {
    return status.code() == zb::rpc::STATUS_OK;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<std::string> servers = SplitCsv(FLAGS_servers);
    std::vector<std::string> disks = SplitCsv(FLAGS_disks);

    if (servers.empty() || disks.empty()) {
        std::cerr << "servers or disks list is empty" << std::endl;
        return 1;
    }

    std::vector<std::string> config_files;
    std::vector<std::unordered_map<std::string, std::string>> disk_maps;
    if (FLAGS_verify_fs) {
        config_files = SplitCsv(FLAGS_config_files);
        if (config_files.size() != servers.size()) {
            std::cerr << "verify_fs requires config_files matching servers count" << std::endl;
            return 1;
        }
        disk_maps.resize(config_files.size());
        for (size_t i = 0; i < config_files.size(); ++i) {
            if (!LoadDiskMapping(config_files[i], &disk_maps[i])) {
                std::cerr << "Failed to load disk mapping from config: " << config_files[i] << std::endl;
                return 1;
            }
        }
    }

    int failures = 0;

    for (size_t server_index = 0; server_index < servers.size(); ++server_index) {
        const std::string& server = servers[server_index];
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;

        if (channel.Init(server.c_str(), &options) != 0) {
            std::cerr << "Failed to init channel to " << server << std::endl;
            ++failures;
            continue;
        }

        zb::rpc::RealNodeService_Stub stub(&channel);

        for (const auto& disk : disks) {
            const std::string chunk_id = MakeChunkId(server, disk);
            const std::string payload = "payload-" + server + "-" + disk;

            zb::rpc::WriteChunkRequest write_req;
            write_req.set_disk_id(disk);
            write_req.set_chunk_id(chunk_id);
            write_req.set_offset(0);
            write_req.set_data(payload);

            zb::rpc::WriteChunkReply write_resp;
            brpc::Controller write_cntl;
            stub.WriteChunk(&write_cntl, &write_req, &write_resp, nullptr);
            if (write_cntl.Failed()) {
                std::cerr << "WriteChunk RPC failed: server=" << server
                          << " disk=" << disk
                          << " error=" << write_cntl.ErrorText() << std::endl;
                ++failures;
                continue;
            }
            if (!StatusOk(write_resp.status())) {
                std::cerr << "WriteChunk status not ok: server=" << server
                          << " disk=" << disk
                          << " code=" << write_resp.status().code()
                          << " msg=" << write_resp.status().message() << std::endl;
                ++failures;
                continue;
            }

            zb::rpc::ReadChunkRequest read_req;
            read_req.set_disk_id(disk);
            read_req.set_chunk_id(chunk_id);
            read_req.set_offset(0);
            read_req.set_size(payload.size());

            zb::rpc::ReadChunkReply read_resp;
            brpc::Controller read_cntl;
            stub.ReadChunk(&read_cntl, &read_req, &read_resp, nullptr);
            if (read_cntl.Failed()) {
                std::cerr << "ReadChunk RPC failed: server=" << server
                          << " disk=" << disk
                          << " error=" << read_cntl.ErrorText() << std::endl;
                ++failures;
                continue;
            }
            if (!StatusOk(read_resp.status())) {
                std::cerr << "ReadChunk status not ok: server=" << server
                          << " disk=" << disk
                          << " code=" << read_resp.status().code()
                          << " msg=" << read_resp.status().message() << std::endl;
                ++failures;
                continue;
            }
            if (read_resp.data() != payload || read_resp.bytes() != payload.size()) {
                std::cerr << "ReadChunk verify failed: server=" << server
                          << " disk=" << disk
                          << " expected_bytes=" << payload.size()
                          << " got_bytes=" << read_resp.bytes()
                          << " expected_data=" << payload
                          << " got_data=" << read_resp.data() << std::endl;
                ++failures;
                continue;
            }

            if (FLAGS_verify_fs) {
                const auto& map = disk_maps[server_index];
                auto it = map.find(disk);
                if (it == map.end()) {
                    std::cerr << "Missing disk mapping in config: server=" << server
                              << " disk=" << disk << std::endl;
                    ++failures;
                    continue;
                }
                const std::string file_path = ResolveLocalPath(it->second, chunk_id);
                std::ifstream file(file_path, std::ios::binary);
                if (!file) {
                    std::cerr << "File not found on disk: " << file_path << std::endl;
                    ++failures;
                    continue;
                }
                std::string file_data(payload.size(), '\0');
                file.read(&file_data[0], static_cast<std::streamsize>(payload.size()));
                std::streamsize read_bytes = file.gcount();
                if (static_cast<size_t>(read_bytes) != payload.size() || file_data != payload) {
                    std::cerr << "Disk verify failed: " << file_path << std::endl;
                    ++failures;
                    continue;
                }
            }

            std::cout << "OK server=" << server << " disk=" << disk
                      << " bytes=" << read_resp.bytes() << std::endl;
        }
    }

    if (failures > 0) {
        std::cerr << "Test failed with " << failures << " error(s)" << std::endl;
        return 1;
    }

    std::cout << "All tests passed" << std::endl;
    return 0;
}
