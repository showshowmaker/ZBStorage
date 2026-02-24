# ZBStorage

ZBStorage 是一个基于 `brpc + RocksDB` 的分布式文件系统原型，包含：
- **真实数据节点（Real Node）**：真实落盘读写，多盘管理，哈希目录组织。
- **虚拟数据节点（Virtual Node）**：按带宽/时延模型模拟大规模节点，单进程可模拟大量节点。
- **光盘归档节点（Optical Node）**：模拟光盘镜像刻录，提供归档副本与只读回源能力。
- **元数据服务（MDS）**：维护目录树、inode、chunk 布局，元数据持久化到 RocksDB。
- **调度器（Scheduler）**：接收节点心跳，维护全局视图，做健康判定和节点启停控制。
- **FUSE 客户端（Linux）**：把 POSIX 文件操作转为 MDS + Data Node 的分布式操作。

## 1. 主要功能

- POSIX 风格元数据接口：`lookup/getattr/readdir/create/mkdir/rename/unlink/rmdir/open/close`
- 文件按 `chunk` 切分，MDS 返回布局后客户端直连数据节点做读写
- Real Node 算法化路径：`<mount>/<2hex>/<2hex>/<chunk_id>`
- 多盘路由：按 `disk_id -> mount point` 映射读写
- 节点类型与权重：`REAL/VIRTUAL + weight`
- 主从角色与切换基础能力：节点支持 `PRIMARY/SECONDARY` 角色、同步写入和角色更新
- Scheduler 控制节点生命周期：`StartNode/StopNode/RebootNode`
- 可选光盘归档层：达到阈值后自动归档到 optical 副本，并按冷数据策略淘汰磁盘副本

## 2. 代码模块

- `src/data_node/real_node`：真实节点服务（磁盘 I/O、多盘、哈希路径、复制逻辑）
- `src/data_node/virtual_node`：虚拟节点服务（模拟带宽/时延，返回模拟数据）
- `src/data_node/optical_node`：光盘归档节点（镜像滚动写入、归档读取、主从复制）
- `src/mds`：元数据服务（RocksDB 存储、inode 与 chunk 布局分配）
- `src/scheduler`：调度与控制平面（心跳、健康状态、全局视图、启停操作）
- `src/client/fuse`：FUSE 客户端（Linux 下挂载文件系统）
- `src/client/test`：联调测试程序
- `src/msg`：`proto` 消息与 RPC 接口定义
- `config`：各模块配置样例与可运行配置
- `scripts`：一键启动脚本
- `deploy`：Docker 与 Kubernetes 部署文件

## 3. 构建

### 3.1 依赖

- CMake >= 3.16
- gcc/clang（Linux 建议），C++17（MDS/RocksDB 目标会以 C++20 编译）
- `protobuf`, `gflags`, `pthread`
- Linux 下 FUSE 客户端额外需要 `fuse3`（Windows 下不会编译 `zb_fuse_client`）
- 第三方源码：`third_party/brpc`、`third_party/rocksdb`

### 3.2 拉取子模块

```bash
git submodule update --init --recursive
```

### 3.3 编译

```bash
cmake -S . -B build
cmake --build build -j
```

可执行文件（`build/`）：
- `scheduler_server`
- `real_node_server`
- `virtual_node_server`
- `optical_node_server`
- `mds_server`
- `real_node_client`
- `real_node_multi_test`
- `virtual_node_test`
- `scheduler_control_test`
- `zb_fuse_client`（仅 Linux）

## 4. 每个模块怎么运行

建议启动顺序：`Scheduler -> Data Nodes -> MDS -> Client/FUSE`

### 4.1 Scheduler

```bash
./build/scheduler_server --config=config/scheduler.conf.example --port=9100
```

### 4.2 Real Node（3 节点示例）

```bash
bash scripts/run_three_real_nodes.sh
```

或单节点手工启动：

```bash
./build/real_node_server --config=config/real_node_mp1.conf --port=19080
```

### 4.3 Virtual Node

```bash
./build/virtual_node_server --config=config/virtual_node.conf.example --port=29080
```

### 4.4 MDS

```bash
./build/mds_server --config=config/mds.conf --port=9000
```

说明：MDS 元数据默认落在配置里的 `MDS_DB_PATH`（例如 `/tmp/zb_mds_rocks`）。
说明：若启用光盘归档，打开 `ENABLE_OPTICAL_ARCHIVE=true`，并在 `NODES`/`DISKS` 中加入 `type=OPTICAL` 节点。

### 4.5 Optical Node

```bash
./build/optical_node_server --config=config/optical_node.conf.example --port=39080
```

### 4.6 FUSE 客户端（Linux）

```bash
./build/zb_fuse_client --mds=127.0.0.1:9000 -- /mnt/zbfs -f
```

注意：`-f` 是 FUSE 参数，前面要用 `--` 和 gflags 参数隔开。

## 5. 一键混合启动（Scheduler + Real + Virtual + MDS）

```bash
bash scripts/run_mixed_with_scheduler.sh
```

启用 optical 节点的混合启动：

```bash
bash scripts/run_mixed_with_optical.sh
```

## 6. 测试

### 6.1 Real Node 多节点读写 + 可选落盘校验

```bash
./build/real_node_multi_test \
  --servers=127.0.0.1:19080,127.0.0.1:19081,127.0.0.1:19082 \
  --disks=disk-01,disk-02,disk-03 \
  --verify_fs=true \
  --config_files=config/real_node_mp1.conf,config/real_node_mp2.conf,config/real_node_mp3.conf
```

### 6.2 Virtual Node 模拟能力测试

```bash
./build/virtual_node_test --server=127.0.0.1:29080 --disk=disk-01
```

### 6.3 Scheduler 控制接口测试

```bash
./build/scheduler_control_test --scheduler=127.0.0.1:9100 --node_id=node-01 --do_reboot=true
```

### 6.4 光盘归档长时压力写入测试

```bash
./build/optical_archive_stress_test \
  --mds=127.0.0.1:9000 \
  --duration_sec=7200 \
  --file_count=256 \
  --write_size=1048576 \
  --report_interval_sec=15 \
  --require_optical=true \
  --cooldown_sec=120 \
  --require_optical_only_after_cooldown=true
```

## 7. 配置入口

- Real Node 示例：`config/real_node.conf.example`
- Virtual Node 示例：`config/virtual_node.conf.example`
- Optical Node 示例：`config/optical_node.conf.example`
- MDS 示例：`config/mds.conf.example`
- Scheduler 示例：`config/scheduler.conf.example`
- 混合部署示例：`config/mds.conf.mixed`
- Optical 归档示例：`config/mds.conf.optical`

## 8. Kubernetes 与容器部署

- Dockerfile：`deploy/Dockerfile`
- K8s 资源：
  - `deploy/k8s/real-node-daemonset.yaml`
  - `deploy/k8s/real-node-configmap.yaml`
  - `deploy/k8s/virtual-node-deployment.yaml`
  - `deploy/k8s/virtual-node-configmap.yaml`
  - `deploy/k8s/scheduler-deployment.yaml`
  - `deploy/k8s/scheduler-configmap.yaml`

如果只看模块说明文档，可直接阅读：
- `src/data_node/real_node/README.md`
- `src/data_node/virtual_node/README.md`
- `src/data_node/optical_node/README.md`
- `src/mds/README.md`
- `src/scheduler/README.md`
