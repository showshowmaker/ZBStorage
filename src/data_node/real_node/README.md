# real_node 模块说明

## 这个模块是什么
`real_node` 是真正落盘的数据节点服务。它通过 brpc 对外提供 `WriteChunk/ReadChunk/GetDiskReport` 接口，负责把 chunk 写入本地磁盘并读取回来。

## 它解决什么问题
- 把 MDS 分配的 `(disk_id, chunk_id)` 变成磁盘文件路径。
- 在多块盘之间按 `disk_id` 路由。
- 使用两级哈希目录打散文件，避免单目录过多文件。
- 执行真实的磁盘 I/O（`open/pread/pwrite`）。

## 核心工作流程
1. 收到 `WriteChunk`：
   - 根据 `disk_id` 找到挂载点；
   - 根据 `chunk_id` 算出路径 `<mount>/<xx>/<yy>/<chunk_id>`；
   - 必要时创建父目录；
   - 把数据写入文件。
2. 收到 `ReadChunk`：
   - 用同样规则算路径；
   - 从文件偏移处读取指定大小。
3. 收到 `GetDiskReport`：
   - 返回每块盘容量、剩余空间、健康状态。

## 配置方式
配置文件支持二选一：
- `ZB_DISKS=disk-01:/path1;disk-02:/path2`（推荐）
- `DATA_ROOT=/path`（自动扫描）

可选接入 Scheduler：
- `NODE_ID` / `NODE_ADDRESS`
- `GROUP_ID` / `NODE_ROLE`（PRIMARY 或 SECONDARY）
- `PEER_NODE_ID` / `PEER_ADDRESS`
- `REPLICATION_ENABLED` / `REPLICATION_TIMEOUT_MS`
- `SCHEDULER_ADDR`
- `NODE_WEIGHT`
- `HEARTBEAT_INTERVAL_MS`

当启用主从时：
- PRIMARY 处理客户端写入并同步复制到 SECONDARY。
- SECONDARY 默认拒绝客户端写入（返回 NOT_LEADER），只接受复制写。
- Scheduler 切换主从后，节点会按心跳回复动态更新角色。

## 运行命令
```bash
./build/real_node_server --config=config/real_node_mp1.conf --port=19080
```

## 一句话总结
`real_node` 就是“真正读写磁盘的执行层”。
