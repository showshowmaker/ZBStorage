# Inode 256B v3 结构说明

## 1. 目标

当前工程中的统一 inode payload 采用固定 `256B` 二进制布局。

这版文档对应当前实现状态，目标是：

1. RocksDB 和 Masstree 统一使用固定 `256B` inode payload。
2. 保留 POSIX 核心语义：
   - `mode`
   - `uid`
   - `gid`
   - `nlink`
   - `size`
   - `atime`
   - `mtime`
   - `ctime`
3. 在 inode 内联存储磁盘/光盘位置，不再依赖“新写路径”单独写 location 列族。
4. 对历史数据保持兼容读取：
   - unified inode v2
   - 旧 `InodeAttr protobuf`

## 2. v3 布局

### 2.1 字段表

| 偏移区间 | 长度 | 字段 | 类型 | 说明 |
|---|---:|---|---|---|
| `[0,1)` | 1 | `version` | `uint8_t` | 当前固定为 `3` |
| `[1,2)` | 1 | `inode_type` | `uint8_t` | `file/dir` |
| `[2,3)` | 1 | `storage_tier` | `uint8_t` | `none/disk/optical` |
| `[3,4)` | 1 | `flags` | `uint8_t` | 状态位，当前保留 `virtual disk node` |
| `[4,5)` | 1 | `file_name_len` | `uint8_t` | 文件名长度，最大 `64` |
| `[5,6)` | 1 | `location_format` | `uint8_t` | 位置槽位解释方式 |
| `[6,8)` | 2 | `reserved0` | `uint16_t` | 对齐预留 |
| `[8,16)` | 8 | `inode_id` | `uint64_t` | inode 编号 |
| `[16,24)` | 8 | `parent_inode_id` | `uint64_t` | 父 inode 编号 |
| `[24,32)` | 8 | `size_bytes` | `uint64_t` | 文件大小；目录默认非 0 |
| `[32,40)` | 8 | `atime` | `uint64_t` | 访问时间 |
| `[40,48)` | 8 | `mtime` | `uint64_t` | 修改时间 |
| `[48,56)` | 8 | `ctime` | `uint64_t` | 元数据变更时间 |
| `[56,64)` | 8 | `version_no` | `uint64_t` | inode 版本号 |
| `[64,68)` | 4 | `mode` | `uint32_t` | POSIX mode |
| `[68,72)` | 4 | `uid` | `uint32_t` | 用户 ID |
| `[72,76)` | 4 | `gid` | `uint32_t` | 组 ID |
| `[76,80)` | 4 | `nlink` | `uint32_t` | 硬链接数 |
| `[80,84)` | 4 | `blksize` | `uint32_t` | 建议块大小 |
| `[84,88)` | 4 | `file_archive_state` | `uint32_t` | 归档状态 |
| `[88,96)` | 8 | `blocks_512` | `uint64_t` | 512B 计数块数 |
| `[96,104)` | 8 | `storage_loc0` | `uint64_t` | 位置主槽位 |
| `[104,112)` | 8 | `storage_loc1` | `uint64_t` | 位置辅槽位 |
| `[112,116)` | 4 | `rdev_major` | `uint32_t` | 特殊文件主设备号 |
| `[116,120)` | 4 | `rdev_minor` | `uint32_t` | 特殊文件次设备号 |
| `[120,124)` | 4 | `object_unit_size` | `uint32_t` | 对象切片大小 |
| `[124,126)` | 2 | `replica` | `uint16_t` | 副本数 |
| `[126,128)` | 2 | `reserved1` | `uint16_t` | 对齐预留 |
| `[128,192)` | 64 | `file_name` | `char[64]` | 文件名 |
| `[192,256)` | 64 | `reserved` | `uint8_t[64]` | 扩展预留 |

### 2.2 位置编码

`storage_tier` 表示位置类型，`location_format` 表示 `storage_loc0/1` 的解释方式。

当前定义：

1. `location_format = 0`
   - 无位置
   - `storage_loc0 = 0`
   - `storage_loc1 = 0`
2. `location_format = 1`
   - disk 二元组
   - `storage_loc0 = disk_node_id`
   - `storage_loc1 = disk_id`
3. `location_format = 2`
   - optical packed
   - `storage_loc0 = optical_node_id`
   - `storage_loc1 = (optical_disk_id << 32) | optical_image_id`

说明：

1. 对外 RPC 仍然使用展开后的字段：
   - `disk_node_id`
   - `disk_id`
   - `optical_node_id`
   - `optical_disk_id`
   - `optical_image_id`
2. 只是在 256B payload 内部压成统一位置区。

### 2.3 当前默认语义

新写路径统一按下面规则补 POSIX 字段：

1. 普通文件
   - `blksize = 4096`
   - `blocks_512 = ceil(size / 512)`
   - `rdev_major = 0`
   - `rdev_minor = 0`
2. 目录
   - `size_bytes = 4096`
   - `blksize = 4096`
   - `blocks_512 = 8`
   - `rdev_major = 0`
   - `rdev_minor = 0`

## 3. 兼容策略

统一 inode 编解码当前策略是：

1. 新写入只编码 `v3`
2. 读取顺序：
   - 先尝试 `v3`
   - 再尝试 `v2`
   - 最后 fallback 到旧 `InodeAttr protobuf`
3. 旧 location 列族只保留读取和删除兼容，不再作为新数据写入路径

## 4. 代码落点

### 4.1 编解码层

核心文件：

1. `src/mds/storage/UnifiedInodeRecord.h`
2. `src/mds/storage/UnifiedInodeRecord.cpp`
3. `src/mds/storage/UnifiedInodeLocation.h`

职责：

1. 定义 v3 偏移常量
2. 编码 `UnifiedInodeRecord -> 256B`
3. 解码 `256B -> UnifiedInodeRecord`
4. 兼容读取 v2
5. `UnifiedInodeRecord <-> InodeAttr` 转换
6. 位置打包和反解包

### 4.2 RPC 层

文件：

1. `src/msg/mds.proto`

`InodeAttr` 当前包含：

1. POSIX 字段
2. 文件名
3. 展开的 disk/optical 位置
4. 新增字段：
   - `blksize`
   - `blocks_512`
   - `rdev_major`
   - `rdev_minor`

### 4.3 主写路径

文件：

1. `src/mds/service/MdsServiceImpl.cpp`
2. `src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp`
3. `tools/meta_gen/mds_sst_generator.cpp`

要求：

1. 所有新 inode 写入统一走 `AttrToUnifiedInodeRecord(...)`
2. 所有新 payload 统一走 `EncodeUnifiedInodeRecord(...)`
3. 不再直接写旧 `InodeAttr protobuf`

### 4.4 主读路径

文件：

1. `src/mds/storage/MetaCodec.cpp`
2. `src/mds/storage/MetaStoreRouter.cpp`
3. `src/mds/masstree_meta/MasstreeMetaStore.cpp`
4. `src/mds/archive/OpticalArchiveManager.cpp`

要求：

1. 统一通过 `DecodeInodeAttrCompat(...)` 读取 inode
2. 历史 `InodeAttr protobuf` 仅作为兼容输入格式

### 4.5 展示层

文件：

1. `src/client/fuse/zb_fuse_client.cpp`

`stat` 映射当前补齐：

1. `st_blksize`
2. `st_blocks`
3. `st_rdev`

## 5. 已清理和保留的旧路径

### 5.1 已清理

以下路径不应再作为新写路径使用：

1. 直接把 `zb::rpc::InodeAttr` 序列化成 inode value
2. 新数据写入时单独写：
   - `disk_file_locations`
   - `optical_file_locations`

### 5.2 保留兼容

以下能力仍然保留：

1. 旧 `InodeAttr protobuf` 的兼容解码
2. 旧 location 列族的兼容读取
3. 旧 location 列族的兼容删除

## 6. 当前限制

1. `object_unit_size` 在 v3 中落为 `uint32_t`
2. `replica` 在 v3 中落为 `uint16_t`
3. `optical_disk_id` 在 packed v3 路径中要求可落入 `uint32_t`
4. `file_name_len <= 64`

## 7. 后续建议

1. 重新生成 `mds.pb.h/.cc`
2. 对 `create / mkdir / setattr / bulk import / sst generator` 做一轮端到端验证
3. 对历史数据做一轮：
   - v2 unified inode 读取验证
   - old protobuf inode 读取验证
   - 旧 location 列族 fallback 验证
