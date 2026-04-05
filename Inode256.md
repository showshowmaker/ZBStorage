# Inode 256B v3

## 1. Scope

This document describes the current inode payload format used by the project.

Current rule:

1. All new inode reads and writes use unified inode `v3`.
2. Old inode formats are no longer part of the active code path.
3. The external RPC model still uses `zb::rpc::InodeAttr`, but inode storage is always the unified `256B` binary record.

## 2. Layout

### 2.1 Field Table

| Offset | Size | Field | Type | Notes |
|---|---:|---|---|---|
| `[0,1)` | 1 | `version` | `uint8_t` | fixed to `3` |
| `[1,2)` | 1 | `inode_type` | `uint8_t` | `file/dir` |
| `[2,3)` | 1 | `storage_tier` | `uint8_t` | `none/disk/optical` |
| `[3,4)` | 1 | `flags` | `uint8_t` | state bits |
| `[4,5)` | 1 | `file_name_len` | `uint8_t` | max `64` |
| `[5,6)` | 1 | `location_format` | `uint8_t` | packed location format |
| `[6,8)` | 2 | `reserved0` | `uint16_t` | reserved |
| `[8,16)` | 8 | `inode_id` | `uint64_t` | inode id |
| `[16,24)` | 8 | `parent_inode_id` | `uint64_t` | parent inode id |
| `[24,32)` | 8 | `size_bytes` | `uint64_t` | file size; directory is non-zero |
| `[32,40)` | 8 | `atime` | `uint64_t` | access time |
| `[40,48)` | 8 | `mtime` | `uint64_t` | modify time |
| `[48,56)` | 8 | `ctime` | `uint64_t` | metadata change time |
| `[56,64)` | 8 | `version_no` | `uint64_t` | inode version |
| `[64,68)` | 4 | `mode` | `uint32_t` | POSIX mode |
| `[68,72)` | 4 | `uid` | `uint32_t` | uid |
| `[72,76)` | 4 | `gid` | `uint32_t` | gid |
| `[76,80)` | 4 | `nlink` | `uint32_t` | hard link count |
| `[80,84)` | 4 | `blksize` | `uint32_t` | preferred block size |
| `[84,88)` | 4 | `file_archive_state` | `uint32_t` | archive state |
| `[88,96)` | 8 | `blocks_512` | `uint64_t` | 512-byte block count |
| `[96,104)` | 8 | `storage_loc0` | `uint64_t` | location slot 0 |
| `[104,112)` | 8 | `storage_loc1` | `uint64_t` | location slot 1 |
| `[112,116)` | 4 | `rdev_major` | `uint32_t` | device major |
| `[116,120)` | 4 | `rdev_minor` | `uint32_t` | device minor |
| `[120,124)` | 4 | `object_unit_size` | `uint32_t` | object chunk size |
| `[124,126)` | 2 | `replica` | `uint16_t` | replica count |
| `[126,128)` | 2 | `reserved1` | `uint16_t` | reserved |
| `[128,192)` | 64 | `file_name` | `char[64]` | inode name |
| `[192,256)` | 64 | `reserved` | `uint8_t[64]` | reserved |

### 2.2 Packed Location Encoding

`storage_tier` describes the storage class. `location_format` describes how `storage_loc0/1` are interpreted.

Current definition:

1. `location_format = 0`
   `storage_loc0 = 0`, `storage_loc1 = 0`
2. `location_format = 1`
   disk layout
   `storage_loc0 = disk_node_id`
   `storage_loc1 = disk_id`
3. `location_format = 2`
   optical packed layout
   `storage_loc0 = optical_node_id`
   `storage_loc1 = (optical_disk_id << 32) | optical_image_id`

The RPC layer still exposes expanded fields:

1. `disk_node_id`
2. `disk_id`
3. `optical_node_id`
4. `optical_disk_id`
5. `optical_image_id`

The packing only applies inside the `256B` inode payload.

## 3. POSIX Defaults

New inode writers fill POSIX-like defaults consistently.

For regular files:

1. `blksize = 4096`
2. `blocks_512 = ceil(size / 512)`
3. `rdev_major = 0`
4. `rdev_minor = 0`

For directories:

1. `size_bytes = 4096`
2. `blksize = 4096`
3. `blocks_512 = 8`
4. `rdev_major = 0`
5. `rdev_minor = 0`

## 4. Code Paths

### 4.1 Core Codec

Files:

1. `src/mds/storage/UnifiedInodeRecord.h`
2. `src/mds/storage/UnifiedInodeRecord.cpp`
3. `src/mds/storage/UnifiedInodeLocation.h`

Responsibilities:

1. define the v3 layout
2. encode `UnifiedInodeRecord -> 256B`
3. decode `256B -> UnifiedInodeRecord`
4. convert `UnifiedInodeRecord <-> InodeAttr`
5. pack and unpack storage location

### 4.2 RPC Model

File:

1. `src/msg/mds.proto`

`InodeAttr` remains the RPC-facing structure and now carries:

1. core POSIX fields
2. `file_name`
3. expanded disk and optical location fields
4. `blksize`
5. `blocks_512`
6. `rdev_major`
7. `rdev_minor`

### 4.3 Write Paths

Files:

1. `src/mds/service/MdsServiceImpl.cpp`
2. `src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp`
3. `tools/meta_gen/mds_sst_generator.cpp`

Rule:

1. all new inode writes go through `AttrToUnifiedInodeRecord(...)`
2. all inode payloads are written by `EncodeUnifiedInodeRecord(...)`
3. no path should serialize old protobuf inode payloads

### 4.4 Read Paths

Files:

1. `src/mds/storage/MetaCodec.cpp`
2. `src/mds/storage/MetaStoreRouter.cpp`
3. `src/mds/archive_meta/ArchiveDataFile.cpp`
4. `src/mds/archive_meta/ArchiveImportService.cpp`
5. `src/mds/archive/OpticalArchiveManager.cpp`
6. `src/mds/masstree_meta/MasstreeInodeRecordCodec.cpp`

Rule:

1. all inode reads decode unified `v3`
2. old inode payload fallbacks are removed from the active code path

### 4.5 FUSE Mapping

File:

1. `src/client/fuse/zb_fuse_client.cpp`

`stat` mapping now includes:

1. `st_blksize`
2. `st_blocks`
3. `st_rdev`

## 5. Removed Old Inode Logic

The project no longer uses these inode paths:

1. old `InodeAttr protobuf` as inode value
2. unified inode `v2`
3. Masstree legacy inode record format based on protobuf inode payload
4. inode read-time fallback from legacy payload to unified payload
5. legacy disk and optical location column families

If an existing RocksDB still contains the old location column families, `RocksMetaStore::Open()` drops them during open.

## 6. Current Limits

1. `object_unit_size` is stored as `uint32_t`
2. `replica` is stored as `uint16_t`
3. `optical_disk_id` must fit in `uint32_t` for packed optical layout
4. `file_name_len <= 64`

## 7. Validation Focus

When validating this format, the main paths to cover are:

1. `create`
2. `mkdir`
3. `rename`
4. `getattr`
5. `path_list` template generation
6. Masstree import and rebase
7. offline SST generation
