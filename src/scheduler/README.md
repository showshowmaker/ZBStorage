# Scheduler 模块说明

`Scheduler` 是集群控制平面，职责包括：

- 接收 real node 和 virtual node 心跳
- 维护节点/磁盘健康状态全局视图
- 向 MDS 提供 `GetClusterView`，供 MDS 分配时过滤健康资源
- 维护主从分组（`group_id`）和 `epoch`
- 主节点故障时自动把从节点提升为主节点（自动 failover）
- 提供节点生命周期控制接口：`StartNode/StopNode/RebootNode`
- 提供节点管理接口：`SetNodeAdminState`（ENABLED/DRAINING/DISABLED）

## 工作机制

1. 节点周期性上报 `ReportHeartbeat`
   - 带上 `group_id`、`role(Primary/Secondary)`、`peer`、`applied_lsn`
2. Scheduler 根据超时阈值把节点标记为 `HEALTHY/SUSPECT/DEAD`
3. 对每个 `group_id` 维护主从关系，主故障时自动切主并 `epoch++`
4. MDS 定期从 Scheduler 拉取视图并替换本地节点缓存
5. MDS 只在 `HEALTHY + ADMIN_ENABLED + POWER_ON + disk_healthy` 范围内分配

## 生命周期控制

Scheduler 内置 Shell 执行器（可选）：

- `START_CMD_TEMPLATE`
- `STOP_CMD_TEMPLATE`
- `REBOOT_CMD_TEMPLATE`

可用占位符：`{node_id}` `{address}` `{force}`。
如果模板未配置，控制操作会被接受并更新期望状态，但不会触发外部命令。

## 快速运行

```bash
./build/scheduler_server --config=config/scheduler.conf.example --port=9100
```

## 快速测试控制接口

```bash
./build/scheduler_control_test --scheduler=127.0.0.1:9100 --node_id=vpool --do_reboot=true
```
