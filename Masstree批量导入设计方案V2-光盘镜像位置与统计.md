# Masstree批量导入设计方案V2-光盘镜像位置与统计

## 1. 目标

在现有 Masstree 批量导入链路基础上，补齐两类能力：

1. 文件元数据在导入时直接绑定到光盘镜像
2. 导入时同步更新集群统计视图

本版方案使用以下固定前提：

- 只考虑光盘位置，不考虑磁盘副本
- 文件元数据只记录到光盘镜像 `image_id`，不记录镜像内偏移和长度
- 文件大小按导入时生成，范围固定为 `0.5GB ~ 1.5GB`
- 文件平均大小约 `1GB`
- 文件总量目标为 `1000亿`
- 光盘节点数为 `10000`
- 每个光盘节点有 `10000` 个光盘
- `90%` 光盘容量为 `1TB`
- `10%` 光盘容量为 `10TB`
- 每个光盘镜像容量固定为 `100GB`
- 节点分配策略改为“一个节点一个节点分配”，不再轮转

## 2. 约束与边界

本版方案明确只解决两件事：

1. `Lookup/Getattr/Readdir/GetRandomMasstreeFileAttr` 能读取到导入后的元数据 `attr`
2. 元数据内部带有“属于哪个光盘镜像”的位置归属，并且导入时统计信息可被更新

本版方案不解决：

- 文件真正打开后的镜像内读取定位
- 镜像内对象偏移
- 空间回收后的重分配
- 多副本

这意味着本版位置语义是“文件属于哪个光盘镜像”，不是“文件在镜像内的精确字节范围”。

## 3. 容量测算

统一使用十进制容量：

- `1GB = 1,000,000,000 bytes`
- `1TB = 1,000,000,000,000 bytes`

集群总容量：

- 小盘：`9000万 * 1TB = 90 EB`
- 大盘：`1000万 * 10TB = 100 EB`
- 合计：`190 EB`

镜像总数：

- `1TB` 光盘每盘 `10` 个镜像
- `10TB` 光盘每盘 `100` 个镜像
- 单节点镜像数：`9000 * 10 + 1000 * 100 = 190000`
- 全集群镜像数：`10000 * 190000 = 19亿`

文件总数据量期望：

- `1000亿` 文件
- 平均大小 `1GB`
- 总数据量约 `100 EB`

结论：容量足够，但必须严格保证单镜像不超过 `100GB`。

## 4. 总体架构

Masstree 仍然保留原来的职责：

- `MTI/<namespace>/<inode20>`：inode 索引
- `MTD/<namespace>/<parent_inode20>/<name>`：dentry 索引

本版新增的核心变化是：

1. `inode_blob.bin` 不再只存 `InodeAttr`
2. `inode_blob.bin` 改为存扩展记录 `MasstreeInodeRecord`
3. 引入全局光盘镜像分配器 `MasstreeOpticalAllocator`
4. 引入全局统计存储 `MasstreeStatsStore`

整体流程变为：

1. 生成目录树元数据
2. 为每个文件生成大小
3. 为每个文件分配光盘镜像 `image_id`
4. 写扩展 inode 记录
5. 导入 Masstree
6. 更新统计信息
7. 发布 route/current

## 5. inode 元数据模型

### 5.1 新的 inode 记录

新增内部结构：

```cpp
struct MasstreeInodeRecord {
    zb::rpc::InodeAttr attr;
    bool has_optical_image;
    uint64_t optical_image_global_id;
};
```

说明：

- 目录 inode：`has_optical_image = false`
- 文件 inode：`has_optical_image = true`
- 文件大小直接写入 `attr.size`
- 不记录镜像内偏移，不记录镜像内长度

### 5.2 为什么只存 `optical_image_global_id`

如果每个文件都重复存：

- `node_id`
- `disk_id`
- `image_id`

在 `1000亿` 文件规模下，字符串重复开销过大。

因此本版只在 inode 记录中存一个紧凑整数 `optical_image_global_id`，其余信息通过固定拓扑推导：

- `node_index`
- `disk_index`
- `image_index_in_disk`
- 最终展示用的 `node_id/disk_id/image_id`

## 6. 光盘拓扑与 image_id 编码

### 6.1 固定拓扑

定义固定 optical profile：

- `optical_node_count = 10000`
- `disks_per_node = 10000`
- `small_disks_per_node = 9000`
- `large_disks_per_node = 1000`
- `small_disk_capacity_bytes = 1TB`
- `large_disk_capacity_bytes = 10TB`
- `image_capacity_bytes = 100GB`

### 6.2 节点内磁盘顺序

每个节点内磁盘顺序固定：

1. 先 `9000` 个 `1TB` 小盘
2. 再 `1000` 个 `10TB` 大盘

这样每个节点内的镜像编号空间固定可推导：

- 小盘镜像区：`0 ~ 89999`
- 大盘镜像区：`90000 ~ 189999`

### 6.3 全局 image id

定义：

```text
images_per_node = 190000
global_image_id = node_index * 190000 + image_index_in_node
```

反解规则：

1. `node_index = global_image_id / 190000`
2. `image_index_in_node = global_image_id % 190000`
3. 若 `image_index_in_node < 90000`
   - `disk_index = image_index_in_node / 10`
   - `image_index_in_disk = image_index_in_node % 10`
4. 否则
   - `large_region = image_index_in_node - 90000`
   - `disk_index = 9000 + large_region / 100`
   - `image_index_in_disk = large_region % 100`

最终可推导：

- `node_id = optical-node-<node_index>`
- `disk_id = disk-<disk_index>`
- `image_id = img-<node_index>-<disk_index>-<image_index_in_disk>`

## 7. 文件大小生成策略

文件大小在生成阶段直接确定，不依赖外部真实文件。

生成规则：

```text
size_bytes = 500,000,000 + (Hash64(namespace_id, generation_id, file_ordinal) % 1,000,000,001)
```

这样可保证：

- 下界约 `0.5GB`
- 上界约 `1.5GB`
- 平均值接近 `1GB`
- 同一批次重试可重现

生成后直接写入：

- `attr.size = size_bytes`

## 8. 光盘镜像分配算法

### 8.1 分配原则

节点分配策略改为顺序填充：

1. 先填满 `node 0`
2. 再填满 `node 1`
3. 依次向后

节点内也顺序填充：

1. 先填当前节点的第一个磁盘
2. 再填下一个磁盘
3. 每个磁盘内先填第一个镜像，再填后续镜像

### 8.2 分配器状态

新增全局状态：

```cpp
struct MasstreeOpticalClusterCursor {
    uint32_t node_index;
    uint32_t disk_index;
    uint32_t image_index_in_disk;
    uint64_t image_used_bytes;
    uint64_t total_file_count;
    uint64_t total_file_bytes;
};
```

该状态表示当前集群已用空间的全局前沿位置。

### 8.3 分配过程

对每个文件按顺序执行：

1. 生成文件大小 `size_bytes`
2. 检查当前镜像是否还能容纳该文件
3. 若不能容纳，则切到同盘下一个镜像
4. 若当前盘镜像已用尽，则切到同节点下一个盘
5. 若当前节点盘已用尽，则切到下一个节点
6. 生成当前文件的 `optical_image_global_id`
7. `image_used_bytes += size_bytes`

必须满足：

```text
image_used_bytes + size_bytes <= 100GB
```

由于单文件最大 `1.5GB`，远小于单镜像 `100GB`，因此每个文件总能完整落在单一镜像中。

### 8.4 为什么不轮转

改成“一个节点一个节点分配”后，有两个直接收益：

1. 统计视图可以用单个全局前沿压缩表示
2. 不需要同时维护 `10000` 个节点的活跃写入状态

代价是部分导入阶段负载会集中在前面的节点，但这是当前需求允许的。

## 9. 集群统计模型

### 9.1 统计目标

导入时需要更新以下信息：

- 集群磁盘节点数
- 集群光盘节点数
- 集群磁盘设备数
- 集群光盘设备数
- 每个盘的总空间
- 每个盘的已用空间
- 总文件数量
- 总文件大小
- 平均文件大小

### 9.2 统计分层

统计分两层保存：

1. generation 本地统计文件
2. RocksDB 当前汇总快照

#### 9.2.1 generation 本地统计文件

每个 generation 目录新增：

- `cluster_stats.txt`
- `allocation_summary.txt`

记录：

- `namespace_id`
- `generation_id`
- `file_count`
- `total_file_bytes`
- `avg_file_size_bytes`
- `start_cursor`
- `end_cursor`
- `first_global_image_id`
- `last_global_image_id`

#### 9.2.2 RocksDB 当前汇总快照

RocksDB 只存小规模聚合键：

- `MTS/cluster/current`
- `MTS/namespace/<namespace_id>/current`

`MTS/cluster/current` 建议字段：

- `disk_node_count`
- `optical_node_count`
- `disk_device_count`
- `optical_device_count`
- `total_capacity_bytes`
- `used_capacity_bytes`
- `free_capacity_bytes`
- `total_file_count`
- `total_file_bytes`
- `avg_file_size_bytes`
- `cursor.node_index`
- `cursor.disk_index`
- `cursor.image_index_in_disk`
- `cursor.image_used_bytes`

说明：

- 当前导入链路没有磁盘副本，因此
  - `disk_node_count = 0`
  - `disk_device_count = 0`
- optical 相关统计固定为
  - `optical_node_count = 10000`
  - `optical_device_count = 100000000`

### 9.3 每个盘的使用空间如何表示

由于本版采用全局顺序填充，所有盘的使用状态满足单调结构：

1. 前面一段节点全部写满
2. 中间最多一个“当前节点”部分写入
3. 后面所有节点均为空

对“当前节点”也同样成立：

1. 前面一段盘写满
2. 中间最多一个“当前盘”部分写入
3. 后面盘为空

因此不需要为 `1亿` 个盘逐条写 RocksDB 记录。

在查询某个盘的使用情况时，可由以下信息推导：

- 固定 optical profile
- 全局 cursor
- 节点内盘顺序规则

这就能得到：

- 该盘总空间
- 该盘是否写满
- 若为当前盘，则其部分已用空间
- 否则已用空间为 `0` 或 `total_capacity`

结论：

- “每个盘的总空间、已用空间”是可计算视图
- 不需要用海量 RocksDB 明细表保存

## 10. Manifest 扩展

`MasstreeNamespaceManifest` 需要新增字段：

- `min_file_size_bytes`
- `max_file_size_bytes`
- `avg_file_size_bytes`
- `total_file_bytes`
- `cluster_stats_path`
- `allocation_summary_path`
- `start_global_image_id`
- `end_global_image_id`
- `start_cursor_node_index`
- `start_cursor_disk_index`
- `start_cursor_image_index`
- `start_cursor_image_used_bytes`
- `end_cursor_node_index`
- `end_cursor_disk_index`
- `end_cursor_image_index`
- `end_cursor_image_used_bytes`

这样在恢复和审计时可以直接知道：

- 这批 namespace 使用了哪些镜像区间
- 导入消耗了多少容量
- 导入前后全局前沿如何变化

## 11. 对现有模块的修改点

### 11.1 `MasstreeBulkMetaGenerator`

新增职责：

- 为文件 inode 生成 `0.5GB ~ 1.5GB` 的大小
- 仍然只生成目录树和文件 inode/dentry
- 不直接分配镜像

生成结果中，文件大小要写入 `inode.records`

### 11.2 `MasstreeBulkImporter`

新增职责：

- 消费带文件大小的 `inode.records`
- 调用 `MasstreeOpticalAllocator` 生成 `optical_image_global_id`
- 将 `InodeAttr + optical_image_global_id` 编码进 `inode_blob.bin`

### 11.3 `MasstreeMetaStore`

读取 inode 时：

- 从 `inode_blob.bin` 解码 `MasstreeInodeRecord`
- 当前 RPC 仍只向外返回 `attr`

后续若要扩展“返回镜像归属”，可直接复用同一记录。

### 11.4 `MasstreeImportService`

新增职责：

- 读取并加锁全局 cluster cursor
- 在导入成功后推进全局 cursor
- 更新 `MTS/cluster/current`
- 写 generation 统计文件

发布顺序必须改为：

1. 生成
2. 分配镜像并导入
3. 写统计
4. 更新 cluster cursor
5. publish route/current

### 11.5 新增模块

建议新增：

- `MasstreeInodeRecordCodec.{h,cpp}`
- `MasstreeOpticalProfile.h`
- `MasstreeOpticalAllocator.{h,cpp}`
- `MasstreeStatsStore.{h,cpp}`

## 12. 一致性语义

导入的对外可见条件必须是：

1. inode/dentry 已成功导入 Masstree
2. `inode_blob.bin` 已完整写入
3. generation 统计文件已写入
4. RocksDB 中 cluster stats 和 global cursor 已更新
5. route/current 已发布

任一步失败：

- route 不发布
- namespace 不可见
- global cursor 不推进

### 12.1 回滚限制

本版为了保持“全局单调前沿”的统计压缩模型，不做空间回收复用。

也就是说：

- generation 回滚只回滚可见性
- 已分配镜像空间不回收
- 后续导入继续从全局 cursor 往后分配

这是本版换取简单实现和低统计开销的代价。

## 13. RPC 与配置建议

### 13.1 配置

固定集群 profile 更适合放在服务端配置，而不是每次 RPC 传参。

建议新增配置项：

- `masstree_optical_node_count = 10000`
- `masstree_optical_disks_per_node = 10000`
- `masstree_optical_small_disks_per_node = 9000`
- `masstree_optical_large_disks_per_node = 1000`
- `masstree_optical_small_disk_capacity_bytes = 1000000000000`
- `masstree_optical_large_disk_capacity_bytes = 10000000000000`
- `masstree_optical_image_capacity_bytes = 100000000000`
- `masstree_file_size_min_bytes = 500000000`
- `masstree_file_size_max_bytes = 1500000000`

### 13.2 RPC

当前 `ImportMasstreeNamespace` 可保持不变。

如果后续要暴露统计查询，再新增：

- `GetMasstreeClusterStats`
- `GetMasstreeOpticalDiskUsage`

本版不强制要求新增统计 RPC，只要求导入时正确记录与更新。

## 14. 实施 Checklist

### 14.1 数据结构

- [ ] 定义 `MasstreeInodeRecord`
- [ ] 实现 `MasstreeInodeRecordCodec`
- [ ] 定义 `MasstreeOpticalClusterCursor`
- [ ] 定义 `MasstreeClusterStatsRecord`

### 14.2 生成阶段

- [ ] 在 `MasstreeBulkMetaGenerator` 中新增文件大小生成
- [ ] 将文件大小写入 `inode.records`
- [ ] 在 manifest 中记录文件大小分布参数

### 14.3 导入阶段

- [ ] 实现 `MasstreeOpticalAllocator`
- [ ] 采用“一个节点一个节点分配”的顺序分配策略
- [ ] 保证任意镜像累计文件大小不超过 `100GB`
- [ ] 将 `optical_image_global_id` 写入 inode blob

### 14.4 统计阶段

- [ ] 生成 `cluster_stats.txt`
- [ ] 生成 `allocation_summary.txt`
- [ ] 更新 `MTS/cluster/current`
- [ ] 更新 `MTS/namespace/<namespace_id>/current`
- [ ] 在统计中维护总文件数、总字节数、平均文件大小

### 14.5 查询阶段

- [ ] 让 `MasstreeMetaStore` 能解析新 inode 记录
- [ ] 保持现有 `GetRandomMasstreeFileAttr` 仍然只返回 `attr`
- [ ] 预留后续按 `image_id` 输出位置的扩展点

### 14.6 一致性与恢复

- [ ] 导入前读取并锁定全局 cluster cursor
- [ ] 导入成功后推进全局 cursor
- [ ] 失败时不推进 cursor、不发布 route
- [ ] 启动恢复时加载 cluster stats 与当前 cursor

## 15. 推荐实施顺序

1. 先补 `MasstreeInodeRecordCodec`
2. 再补生成器里的文件大小生成
3. 然后实现 `MasstreeOpticalAllocator`
4. 再改 importer 与 meta store
5. 最后加 `MasstreeStatsStore` 和 cluster cursor 持久化

## 16. 结论

这版方案的核心变化有两点：

1. 位置记录从“可能外置到 RocksDB”收敛为“直接写进 Masstree inode blob，只保留镜像级归属”
2. 统计模型借助“按节点顺序填满”的分配方式，压缩为“固定拓扑 + 全局前沿 + 聚合快照”

这样改完以后，可以同时满足：

- 大规模元数据不落 RocksDB 明细
- 单镜像容量不超限
- 导入时能维护集群视图和总量统计
- 现有随机 attr 查询链路无需改变输出协议
