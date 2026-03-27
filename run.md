# ZBStorage 运行与演示说明

本文档按当前仓库代码整理，默认运行环境为 `Linux` 或 `WSL2 Ubuntu`。

## 1. 编译

进入项目目录：

```bash
cd /mnt/md0/Projects/wjh/ZBStorage
```

如果明确不再使用 `Ninja`，直接用 `Unix Makefiles`：

```bash
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -G "Unix Makefiles"
cmake --build build -j"$(nproc)"
```

编译成功后确认这些可执行文件存在：

```bash
ls build/system_demo_tool
ls build/scheduler_server
ls build/mds_server
ls build/real_node_server
ls build/virtual_node_server
ls build/zb_fuse_client
```

## 2. 启动演示环境

当前推荐运行目录：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir
```

启动：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh start
```

查看状态：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh status
```

停止：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh stop
```

## 3. 导入方式

### 3.1 普通导入

不使用模板，每次都完整生成并导入：

```bash
build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=masstree_import \
  --masstree_namespace_id=demo-ns \
  --masstree_generation_id=gen-001 \
  --masstree_file_count=100000000
```

### 3.2 模板导入

当前推荐使用 `legacy_records`，这是更稳的模板导入路径。

单次导入：

```bash
build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=masstree_import \
  --masstree_namespace_id=demo-ns \
  --masstree_generation_id=gen-001 \
  --masstree_file_count=100000000 \
  --masstree_template_id=template-100m-v1 \
  --masstree_template_mode=legacy_records
```

批量导入：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir \
MASSTREE_TEMPLATE_ID=template-100m-v1 \
MASSTREE_TEMPLATE_MODE=legacy_records \
bash scripts/import_masstree_demo.sh 100
```

### 3.3 快速导入

这是实验性快路径，当前不建议作为正式基线使用。

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir \
MASSTREE_TEMPLATE_ID=template-100m-v1 \
MASSTREE_TEMPLATE_MODE=page_fast \
bash scripts/import_masstree_demo.sh 2
```

## 4. 一键导入 1000 亿元数据

当前仓库已提供一键脚本，会自动：

1. 启动相关模块
2. 批量导入 `1000` 个 namespace
3. 使用模板导入 `legacy_records`
4. 完成后自动停止

前台运行：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir \
MASSTREE_TEMPLATE_ID=template-100m-v1 \
MASSTREE_TEMPLATE_MODE=legacy_records \
bash scripts/run_import_1000yi_once.sh
```

后台运行且不绑定终端：

```bash
mkdir -p /mnt/md0/wjh/zb_run_dir/logs
cd /mnt/md0/Projects/wjh/ZBStorage
nohup env \
  RUN_DIR=/mnt/md0/wjh/zb_run_dir \
  MASSTREE_TEMPLATE_ID=template-100m-v1 \
  MASSTREE_TEMPLATE_MODE=legacy_records \
  bash scripts/run_import_1000yi_once.sh \
  > /mnt/md0/wjh/zb_run_dir/logs/run_import_1000yi_once.nohup.log 2>&1 &
echo $! > /mnt/md0/wjh/zb_run_dir/logs/run_import_1000yi_once.pid
```

查看后台日志：

```bash
tail -f /mnt/md0/wjh/zb_run_dir/logs/run_import_1000yi_once.nohup.log
```

停止后台任务：

```bash
kill "$(cat /mnt/md0/wjh/zb_run_dir/logs/run_import_1000yi_once.pid)"
```

## 5. 检查 1000 亿是否导入成功

找到批量导入日志：

```bash
ls -lt /mnt/md0/wjh/zb_run_dir/logs/import_masstree_1000ns_*.log | head
```

检查最后结果：

```bash
tail -n 50 /mnt/md0/wjh/zb_run_dir/logs/import_masstree_1000ns_*.log
```

检查是否有失败：

```bash
grep -n "job_status=FAILED" /mnt/md0/wjh/zb_run_dir/logs/import_masstree_1000ns_*.log
```

统计完成数：

```bash
grep -c "job_status=COMPLETED" /mnt/md0/wjh/zb_run_dir/logs/import_masstree_1000ns_*.log
```

如果结果是 `1000`，并且最后一次导入后出现：

```txt
after_total_file_count=100000000000
check.stats.total_file_count_after=PASS
```

就说明已经成功导入 `1000亿` 元数据。

## 6. 1000 亿导入完成后的后续演示

因为一键导入脚本结束后会自动 `stop`，所以先重新启动环境：

```bash
cd /mnt/md0/Projects/wjh/ZBStorage
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh start
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/start_demo_stack.sh status
```

### 6.1 演示 P1：确认当前已有 1000 亿

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=stats \
  --tc_p1_expected_total_file_count=100000000000 \
  --log_file=logs/p1_before.log
```

重点看：

```txt
total_file_count=100000000000
check.expected.total_file_count=PASS
```

### 6.2 演示 P2：真实节点 100MB 读写

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=posix \
  --real_dir=real \
  --posix_file_size_mb=100 \
  --posix_chunk_size_kb=1024 \
  --log_file=logs/p2.log
```

### 6.3 演示 P3：虚拟节点 100MB 读写

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=posix \
  --virtual_dir=virtual \
  --posix_file_size_mb=100 \
  --posix_chunk_size_kb=1024 \
  --log_file=logs/p3.log
```

### 6.4 演示 P4：再导入 1 亿，把总量从 1000 亿变成 1001 亿

建议单独用一个新 namespace，例如 `demo-ns-report`：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=masstree_import \
  --masstree_namespace_id=demo-ns-report \
  --masstree_generation_id=gen-report-001 \
  --masstree_file_count=100000000 \
  --masstree_template_id=template-100m-v1 \
  --masstree_template_mode=legacy_records \
  --log_file=logs/p4.log
```

重点看：

```txt
job_status=COMPLETED
delta_total_file_count=100000000
check.stats.total_file_count_after=PASS
```

### 6.5 再跑一次 P1：确认现在是 1001 亿

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=stats \
  --tc_p1_expected_total_file_count=100100000000 \
  --log_file=logs/p1_after.log
```

重点看：

```txt
total_file_count=100100000000
check.expected.total_file_count=PASS
```

### 6.6 演示 P5：查询刚刚新导入的 namespace

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir build/system_demo_tool \
  --mds=127.0.0.1:9000 \
  --scheduler=127.0.0.1:9100 \
  --mount_point=/mnt/md0/wjh/zb_run_dir/mnt \
  --scenario=masstree_query \
  --masstree_namespace_id=demo-ns-report \
  --masstree_query_samples=1000 \
  --log_file=logs/p5.log
```

重点看：

```txt
query_samples=1000
query_success_count=...
avg_query_latency_us=...
```

## 7. 菜单模式演示

启动菜单：

```bash
RUN_DIR=/mnt/md0/wjh/zb_run_dir bash scripts/run_system_demo.sh
```

进入菜单后，建议按这个顺序输入：

```txt
2 tc_p1_expected_total_file_count=100000000000 log_file=logs/p1_before.log
3 file_size_mb=100 chunk_size_kb=1024 log_file=logs/p2.log
4 file_size_mb=100 chunk_size_kb=1024 log_file=logs/p3.log
5 namespace=demo-ns-report generation=gen-report-001 file_count=100000000 template_id=template-100m-v1 template_mode=legacy_records log_file=logs/p4.log
2 tc_p1_expected_total_file_count=100100000000 log_file=logs/p1_after.log
6 namespace=demo-ns-report n=10 log_file=logs/p5.log
```

## 8. 日志位置

服务日志：

```bash
/mnt/md0/wjh/zb_run_dir/logs/
```

demo 指令日志：

```bash
logs/system_demo.log
```

以及你自己传入的：

```bash
logs/p1_before.log
logs/p2.log
logs/p3.log
logs/p4.log
logs/p1_after.log
logs/p5.log
```

