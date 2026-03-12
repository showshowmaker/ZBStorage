**完整流程（Linux/Ubuntu）**

1. **编译所需程序**
```bash
cd /mnt/md0/Projects/wjh/ZBStorage

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target \
  meta_plan_tool mds_sst_gen_tool disk_meta_gen_tool optical_meta_gen_tool mds_sst_ingest_tool \
  scheduler_server mds_server real_node_server virtual_node_server optical_node_server \
  -j
```
如果要挂载 FUSE，再额外编译：
```bash
cmake --build build --target zb_fuse_client -j
```

2. **生成元数据**
使用脚本：[run_meta_gen_pipeline.sh](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\scripts\run_meta_gen_pipeline.sh)

```bash
OUT_DIR=/mnt/md0/wjh/zb_meta_out \
MIN_FREE_SPACE_TB=10 \
SPACE_CHECK_INTERVAL_SEC=30 \
ENABLE_SPACE_GUARD=1 \
bash scripts/run_meta_gen_pipeline.sh
```

生成结果在 `OUT_DIR` 下，重点看：
- `OUT_DIR/mds_sst`
- `OUT_DIR/disk_meta`
- `OUT_DIR/optical_meta`
- `OUT_DIR/generation_stats.conf`
- `OUT_DIR/logs/meta_gen_pipeline_*.log`

3. **启动系统（自动导入 MDS SST）**
使用脚本：[oneclick_cluster_from_meta.sh](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\scripts\oneclick_cluster_from_meta.sh)

```bash
OUT_DIR=/mnt/md0/wjh/zb_meta_out \
RUN_DIR=/mnt/md0/wjh/zb_meta_out/run \
CLEAR_MDS_DB_BEFORE_INGEST=1 \
ENABLE_OPTICAL_NODE=1 \
ENABLE_FUSE=0 \
REAL_NODE_COUNT=1 \
VIRTUAL_NODE_COUNT=1 \
OPTICAL_NODE_COUNT=1 \
bash scripts/oneclick_cluster_from_meta.sh start
```

4. **查看状态**
```bash
OUT_DIR=/mnt/md0/wjh/zb_meta_out \
RUN_DIR=/mnt/md0/wjh/zb_meta_out/run \
bash scripts/oneclick_cluster_from_meta.sh status
```

5. **停止系统**
```bash
OUT_DIR=/mnt/md0/wjh/zb_meta_out \
RUN_DIR=/mnt/md0/wjh/zb_meta_out/run \
bash scripts/oneclick_cluster_from_meta.sh stop
```

---

**补充**
- 首次启动建议 `CLEAR_MDS_DB_BEFORE_INGEST=1`；后续重启可用 `CLEAR_MDS_DB_BEFORE_INGEST=0`，并可加 `INGEST_MDS_SST=0` 跳过重复导入。  
- 运行期日志/PID/配置/MDS DB 在 `RUN_DIR`，不是 `OUT_DIR` 根目录。