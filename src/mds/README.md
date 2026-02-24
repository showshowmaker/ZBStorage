# mds 模块说明

## 这个模块是什么
`mds` 是元数据服务器（Metadata Server）。它不存文件内容，只管理“文件到 chunk、chunk 到节点位置”的映射关系，并通过 brpc 服务给客户端。

## 它负责哪些事
- 维护目录树、inode 属性、dentry（文件名到 inode）。
- 维护文件 chunk 布局（每个 chunk 的副本位置）。
- 在写入前分配 chunk 副本位置（节点、磁盘、chunk_id）。
- 在读取时返回布局，让客户端直连数据节点读写。
- 把所有元数据持久化到 RocksDB。

## 与客户端/数据节点关系
1. 客户端做 POSIX 操作时，先向 MDS 发元数据请求（如 lookup/create/readdir/allocate）。
2. MDS 返回 inode 与 layout（副本位置列表）。
3. 客户端按 layout 直接访问 `real_node` 或 `virtual_node`。
4. 写入后客户端调用 `CommitWrite` 更新文件大小等元数据。

## 节点分配策略（当前实现）
- 支持节点类型：`REAL`、`VIRTUAL`。
- 支持节点权重：`weight` 越大，被选中概率越高。
- 支持虚拟池展开：`virtual_node_count` 可把一个 endpoint 逻辑展开为大量节点 ID（如 `vpool-v0` 到 `vpool-v99999`）。
- 支持主从分组：MDS 只从 Scheduler 视图中的 `PRIMARY` 节点分配，layout 同时携带 `SECONDARY` 元信息与 `epoch`。

## 关键配置
- `MDS_DB_PATH`：RocksDB 路径。
- `SCHEDULER_ADDR`：Scheduler 地址，配置后 MDS 会周期拉取全局节点视图。
- `SCHEDULER_REFRESH_MS`：拉取周期毫秒数。
- `CHUNK_SIZE`：默认 chunk 大小。
- `REPLICA`：默认副本数。
- `NODES`：节点列表（支持 type/weight/virtual_node_count）。
- `DISKS`：每个节点可用磁盘 ID 列表。

## 运行命令
```bash
./build/mds_server --config=config/mds.conf.mixed --port=9000
```

## 一句话总结
`mds` 是“全局命名空间和 chunk 布局的权威管理中心”。
