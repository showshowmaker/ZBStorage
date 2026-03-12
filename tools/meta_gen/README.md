# `meta_gen` 工具说明

## 目标

用于大规模压测前的“参数规划”阶段，输出以下内容：

- 光盘容量三档分配（100GB/1TB/10TB）
- 命名空间目录树参数（深度、叶子目录数、每叶目录文件数）
- 文件大小分布采样估计（均值）
- 磁盘副本预算估算（按容量比例）

## 可执行程序

- `meta_plan_tool`
- `mds_sst_gen_tool`
- `mds_sst_ingest_tool`
- `disk_meta_gen_tool`
- `optical_meta_gen_tool`

## 典型用法

```bash
./meta_plan_tool \
  --total_files=1000000000000 \
  --namespace_count=1000 \
  --optical_node_count=10000 \
  --discs_per_optical_node=10000 \
  --virtual_node_count=100 \
  --virtual_disks_per_node=16 \
  --real_node_count=1 \
  --real_disks_per_node=24 \
  --disk_replica_ratio=0.5 \
  --target_optical_eb=1000 \
  --sample_count=200000 \
  --output=meta_plan_report.md
```

## 说明

- 当前默认是“严格容量模式”：给定总光盘数和目标容量，做整数求解。
- 在 `10000 * 10000` 光盘且目标 `1000EB` 的前提下，数学上只能全部使用 `10TB` 光盘。
- 若要强制混用三档容量，需要放宽约束（降低目标容量或提高光盘总数）。

## 生成 MDS SST（T07）

```bash
./mds_sst_gen_tool \
  --output_dir=./out_mds_sst \
  --total_files=1000000 \
  --namespace_count=100 \
  --branch_factor=16 \
  --max_depth=3 \
  --max_kv_per_sst=500000
```

输出目录会包含：

- `mds_meta_*.sst`：按分片生成的 SST 文件。
- `manifest.txt`：键数量、inode 数量、anchor 分布、SST 列表等统计。

## 导入到 RocksDB（T08）

```bash
./mds_sst_ingest_tool \
  --db_path=./mds_db \
  --manifest=./out_mds_sst/manifest.txt
```

## 生成磁盘节点元数据（T09）

```bash
./disk_meta_gen_tool \
  --output_dir=./out_disk_meta \
  --total_files=1000000 \
  --namespace_count=1000 \
  --disk_replica_ratio=0.5
```

输出：

- `out_disk_meta/real_nodes/real-*/file_meta.tsv`
- `out_disk_meta/virtual_nodes/virtual-*/file_meta.tsv`
- `out_disk_meta/disk_meta_manifest.txt`

## 生成光盘节点元数据（T10）

```bash
./optical_meta_gen_tool \
  --output_dir=./out_optical_meta \
  --total_files=1000000 \
  --namespace_count=1000 \
  --optical_node_count=10000 \
  --discs_per_optical_node=10000 \
  --target_optical_eb=1000
```

输出：

- `out_optical_meta/optical_manifest.tsv`
- `out_optical_meta/optical_disc_catalog.tsv`
- `out_optical_meta/optical_meta_manifest.txt`

## 一键流水线

可直接运行脚本：

```bash
bash scripts/run_meta_gen_pipeline.sh
```

也可通过环境变量覆盖参数，例如：

```bash
OUT_DIR=/data/meta_seed \
TOTAL_FILES=1000000000 \
NAMESPACE_COUNT=1000 \
TARGET_OPTICAL_EB=1000 \
bash scripts/run_meta_gen_pipeline.sh
```
