# virtual_node 模块说明

## 这个模块是什么
`virtual_node` 是“模拟数据节点”。它对外暴露与 `real_node` 完全一致的 brpc 接口，但不做真实落盘。

## 它解决什么问题
- 在不准备大量真实机器/磁盘的情况下，模拟超大规模节点池（例如 1 个 Pod 模拟 100000 个节点）。
- 用可控参数模拟读写性能和延迟，用于压测和调度策略验证。

## 模拟规则
- `WriteChunk`：按数据大小计算耗时并 sleep，返回成功，不写磁盘。
- `ReadChunk`：按请求大小计算耗时并 sleep，返回同样长度的 `'x'` 数据。
- `GetDiskReport`：返回配置中的虚拟磁盘列表和容量。

耗时模型：
- `delay_ms = base_latency_ms + ceil(bytes / bytes_per_sec * 1000) + jitter`

## 配置项
- `NODE_ID` / `NODE_ADDRESS`：节点标识与对外地址。
- `GROUP_ID` / `NODE_ROLE`：主从组与初始角色。
- `PEER_NODE_ID` / `PEER_ADDRESS`：对端节点信息。
- `REPLICATION_ENABLED` / `REPLICATION_TIMEOUT_MS`：是否启用主从复制及超时。
- `SCHEDULER_ADDR`：Scheduler 地址（用于心跳上报）。
- `NODE_WEIGHT`：节点权重。
- `VIRTUAL_NODE_COUNT`：逻辑虚拟节点数。
- `HEARTBEAT_INTERVAL_MS`：心跳周期。
- `DISKS`：暴露给上层的磁盘 ID 列表。
- `READ_MBPS` / `WRITE_MBPS`：吞吐速率。
- `READ_BASE_LATENCY_MS` / `WRITE_BASE_LATENCY_MS`：固定延迟。
- `JITTER_MS`：随机抖动上限。
- `DISK_CAPACITY_BYTES`：上报容量。

## 运行命令
```bash
./build/virtual_node_server --config=config/virtual_node.conf.example --port=29080
```

## 一句话总结
`virtual_node` 是“接口兼容 real_node 的高规模仿真层”。
