按现在这版代码，直接用 `Linux` 或 `WSL2 Ubuntu` 编译运行。`bash` 脚本和 `FUSE3` 不适合原生 PowerShell 直接跑。

**1. 进入项目目录**
```bash
cd /path/to/ZBStorage
```

**2. 安装依赖**
`CMake` 目前要求 `Protobuf`、`GFLAGS`、`FUSE3` 等依赖。[CMakeLists.txt](C:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/CMakeLists.txt#L13)

Ubuntu 常用安装命令：
```bash
sudo apt update
sudo apt install -y \
  build-essential cmake ninja-build pkg-config \
  protobuf-compiler libprotobuf-dev \
  libgflags-dev \
  libfuse3-dev \
  libssl-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libsnappy-dev
```

**3. 编译**
用现成脚本即可。[build_all.sh](C:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/build_all.sh)

```bash
bash scripts/build_all.sh build
```

编译成功后，确认这些文件存在：
```bash
ls build/system_demo_tool
ls build/scheduler_server
ls build/mds_server
ls build/real_node_server
ls build/virtual_node_server
ls build/zb_fuse_client
```

**4. 启动演示环境**
启动脚本在 [start_demo_stack.sh](C:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/start_demo_stack.sh)。

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh start
```

查看状态：
```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh status
```

现在默认在线层已经按文档口径改成：
- `1` 个真实节点
- `99` 个虚拟逻辑节点
- 每节点 `24` 盘
- 每盘 `2 TB`

**5. 如果要先铺 Masstree 基线数据**
批量导入脚本在 [import_masstree_demo.sh](C:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/import_masstree_demo.sh)。

先铺 `100亿`：
```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir MASSTREE_TEMPLATE_ID=template-100m-v1 bash scripts/import_masstree_demo.sh 2
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/import_masstree_demo.sh 100
```

先铺 `1000亿`：
```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/import_masstree_demo.sh 1000
```

它的逻辑是：每个 namespace 导入 `1亿` 文件元数据，所以：
- `100` 个 namespace = `100亿`
- `1000` 个 namespace = `1000亿`

**6. 启动 demo 菜单**
脚本在 [run_system_demo.sh](C:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/run_system_demo.sh)。

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/run_system_demo.sh
```

进入菜单后，推荐这样演示：

先看 `P1`：
```txt
2 log_file=logs/p1_before.log
```

跑 `P2`：
```txt
3 file_size_mb=100 chunk_size_kb=1024 repeat=1 log_file=logs/p2.log
```

跑 `P3`：
```txt
4 file_size_mb=100 chunk_size_kb=1024 repeat=1 log_file=logs/p3.log
```

跑 `P4`：
```txt
5 namespace=demo-ns generation=gen-report-001 file_count=100000000 log_file=logs/p4.log
```

再看一次 `P1`：
```txt
2 log_file=logs/p1_after.log
```

跑 `P5`：
```txt
6 namespace=demo-ns n=1000 log_file=logs/p5.log
```

**7. 如果不想进菜单，也可以直接跑单项**
例如 `P1`：
```bash
build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=.demo_run/mnt \
  --scenario=stats \
  --log_file=logs/p1.log
```

例如 `P5`：
```bash
build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=.demo_run/mnt \
  --scenario=masstree_query \
  --masstree_namespace_id=demo-ns \
  --masstree_query_samples=1000 \
  --log_file=logs/p5.log
```

**8. 日志位置**
服务日志：
```bash
.demo_run/logs/
```

demo 指令日志：
```bash
logs/system_demo.log
```
或者你自己传的 `logs/p1.log`、`logs/p4.log`、`logs/p5.log`。

**9. 结束后关闭环境**
```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh stop
```

如果你要，我可以下一步直接给你一套“从 0 到演示完 P1~P5 的可复制命令清单”，按你要的 `100亿` 或 `1000亿` 版本各写一套。

RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh start
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh status
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/import_masstree_demo.sh 100
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/run_system_demo.sh
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh stop
