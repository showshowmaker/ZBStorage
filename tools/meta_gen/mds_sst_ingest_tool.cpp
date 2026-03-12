#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace zb::meta_gen {

namespace {

bool StartsWith(const std::string& s, const std::string& prefix) {
    return s.rfind(prefix, 0) == 0;
}

void PrintUsage(std::ostream& os) {
    os << "Usage: mds_sst_ingest_tool --db_path=<path> [--manifest=<path> | --sst=<path> ...]\n"
          "Options:\n"
          "  --db_path=<path>                     Required: RocksDB path\n"
          "  --manifest=<path>                    Manifest from mds_sst_gen_tool\n"
          "  --sst=<path>                         SST file path (can repeat)\n"
          "  --move_files=<0|1>                   Ingest move mode (default: 0)\n"
          "  --snapshot_consistency=<0|1>         Snapshot consistency (default: 1)\n"
          "  --allow_global_seqno=<0|1>           Allow global seqno assignment (default: 1)\n"
          "  --help                               Show help\n";
}

bool ParseBool01(const std::string& v, bool* out) {
    if (!out) {
        return false;
    }
    if (v == "1" || v == "true" || v == "TRUE") {
        *out = true;
        return true;
    }
    if (v == "0" || v == "false" || v == "FALSE") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseManifestSsts(const std::string& manifest_path, std::vector<std::string>* out, std::string* error) {
    if (!out) {
        if (error) {
            *error = "output sst list is null";
        }
        return false;
    }
    std::ifstream ifs(manifest_path);
    if (!ifs.is_open()) {
        if (error) {
            *error = "failed to open manifest: " + manifest_path;
        }
        return false;
    }
    bool in_block = false;
    std::string line;
    while (std::getline(ifs, line)) {
        if (line == "sst_files_begin") {
            in_block = true;
            continue;
        }
        if (line == "sst_files_end") {
            in_block = false;
            break;
        }
        if (!in_block || line.empty()) {
            continue;
        }
        out->push_back(line);
    }
    if (out->empty()) {
        if (error) {
            *error = "no sst files found in manifest";
        }
        return false;
    }
    return true;
}

} // namespace

int RunMdsSstIngestTool(int argc, char** argv) {
    std::string db_path;
    std::string manifest_path;
    std::vector<std::string> sst_files;
    bool move_files = false;
    bool snapshot_consistency = true;
    bool allow_global_seqno = true;

    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i] ? argv[i] : "");
        if (arg == "--help" || arg == "-h") {
            PrintUsage(std::cout);
            return 0;
        }
        if (!StartsWith(arg, "--")) {
            std::cerr << "invalid argument: " << arg << "\n";
            return 1;
        }
        const size_t eq = arg.find('=');
        if (eq == std::string::npos) {
            std::cerr << "argument must use --key=value format: " << arg << "\n";
            return 1;
        }
        const std::string key = arg.substr(2, eq - 2);
        const std::string value = arg.substr(eq + 1);

        if (key == "db_path") {
            db_path = value;
        } else if (key == "manifest") {
            manifest_path = value;
        } else if (key == "sst") {
            sst_files.push_back(value);
        } else if (key == "move_files") {
            if (!ParseBool01(value, &move_files)) {
                std::cerr << "invalid bool value for --move_files: " << value << "\n";
                return 1;
            }
        } else if (key == "snapshot_consistency") {
            if (!ParseBool01(value, &snapshot_consistency)) {
                std::cerr << "invalid bool value for --snapshot_consistency: " << value << "\n";
                return 1;
            }
        } else if (key == "allow_global_seqno") {
            if (!ParseBool01(value, &allow_global_seqno)) {
                std::cerr << "invalid bool value for --allow_global_seqno: " << value << "\n";
                return 1;
            }
        } else {
            std::cerr << "unknown option: --" << key << "\n";
            return 1;
        }
    }

    if (db_path.empty()) {
        std::cerr << "--db_path is required\n";
        return 1;
    }

    if (!manifest_path.empty()) {
        std::string error;
        if (!ParseManifestSsts(manifest_path, &sst_files, &error)) {
            std::cerr << "manifest parse failed: " << error << "\n";
            return 2;
        }
    }

    if (sst_files.empty()) {
        std::cerr << "no sst files provided, use --manifest or --sst\n";
        return 1;
    }
    std::sort(sst_files.begin(), sst_files.end());
    sst_files.erase(std::unique(sst_files.begin(), sst_files.end()), sst_files.end());

    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* raw_db = nullptr;
    rocksdb::Status st = rocksdb::DB::Open(options, db_path, &raw_db);
    if (!st.ok() || raw_db == nullptr) {
        std::cerr << "failed to open rocksdb: " << st.ToString() << "\n";
        return 3;
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);

    rocksdb::IngestExternalFileOptions ingest_opt;
    ingest_opt.move_files = move_files;
    ingest_opt.snapshot_consistency = snapshot_consistency;
    ingest_opt.allow_global_seqno = allow_global_seqno;
    st = db->IngestExternalFile(sst_files, ingest_opt);
    if (!st.ok()) {
        std::cerr << "ingest failed: " << st.ToString() << "\n";
        return 4;
    }

    std::cout << "ingest success, file_count=" << sst_files.size() << "\n";
    return 0;
}

} // namespace zb::meta_gen

int main(int argc, char** argv) {
    return zb::meta_gen::RunMdsSstIngestTool(argc, argv);
}
