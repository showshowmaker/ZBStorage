# Masstree批量导入设计方案V1

## 1. 目标

新增一条正式的 Masstree 批量导入链路，支持按 `namespace_id` 批量导入元数据。

默认目标：

- 单批导入 `100,000,000` 个文件
- 每批导入对应一个独立 `namespace_id`
- 先生成均衡目录树元数据
- 再将生成好的元数据导入 Masstree
- 导入成功后可通过系统 `Lookup / Getattr / Readdir` 正常访问
- 导入失败不可见
- 重启后可恢复

本方案不替换现有 RocksDB 热路径，也不替换现有 archive generation 路径，而是新增一条 Masstree namespace 读写链路。

## 2. 现有可复用能力

现有 `masstree` 代码中，以下方法可以直接复用到正式方案中：

- `MasstreeWrapper::insert/search/scan/remove`
- 每线程 `thread_init` 的并行导入模式
- “Masstree 只存 key 和定位信息，正文放外部页文件”的思路
- 导入后用 `scan(key, 1, ...)` 做回读验证
- 启动时根据外部文件重放内存树的 recover 思路

现有脚本不能直接原样接入系统，原因如下：

- 当前 key 设计是压测格式，不是系统级 `inode` / `dentry` 语义
- 当前 `uid` 只是测试分组，不是系统级 `namespace_id`
- 当前日志文件布局没有 manifest、catalog、current pointer、发布语义
- 当前验证覆盖的是压测场景，不是系统级 `Lookup/Getattr/Readdir`

结论：

- 复用其方法
- 不直接复用其脚本主流程

## 3. 总体架构

新增模块：

- `MasstreeNamespaceCatalog`
- `MasstreeBulkMetaGenerator`
- `MasstreeBulkImporter`
- `MasstreeMetaStore`
- `MasstreeImportService`
- `masstree_import_tool`

职责划分：

- `MasstreeBulkMetaGenerator`
  第一步，生成均衡目录树元数据，输出 staging 文件
- `MasstreeBulkImporter`
  第二步，读取 staging 文件并批量导入 Masstree
- `MasstreeNamespaceCatalog`
  维护 `namespace_id -> generation -> current route`
- `MasstreeMetaStore`
  提供 `ResolvePath / GetInode / Readdir`
- `MasstreeImportService`
  负责生成、导入、验证、发布、恢复

## 4. 数据模型

### 4.1 Masstree 内部 key/value

定义三类核心索引：

1. inode 索引树

- key: `MTI/<namespace_id>/<inode20>`
- value: `inode_blob_offset`

2. dentry 索引树

- key: `MTD/<namespace_id>/<parent_inode20>/<name>`
- value: `packed(child_inode, type)`

3. namespace 元信息

- key: `MTN/<namespace_id>/<generation_id>`
- value: `MasstreeNamespaceManifest`

4. current 指针

- key: `MTNC/<path_prefix>`
- value: `namespace_id + generation_id`

说明：

- `inode20` / `parent_inode20` 必须使用固定宽度零填充编码，保证字符串排序与数值排序一致
- 不直接复用当前 `InodeKey` / `DentryKey` 的普通十进制拼接格式

### 4.2 外部文件

每个 generation 目录下至少包含：

- `manifest.txt`
- `inode_blob.bin`
- `dentry_data.bin`
- `verify_manifest.txt`

V1 建议：

- inode 正文放 `inode_blob.bin`
- dentry 正文不单独存完整 payload，Masstree value 直接存 `child_inode + type`
- `dentry_data.bin` 只在后续需要扩展字段时使用，V1 可选

## 5. 两步导入流程

### 5.1 第一步：生成元数据

输入：

- `namespace_id`
- `generation_id`
- `file_count`，默认 `100000000`
- `path_prefix`，默认 `/bulk/<namespace_id>`
- `max_files_per_leaf_dir`，默认 `2048`
- `max_subdirs_per_dir`，默认 `256`

输出：

- inode 记录流
- dentry 记录流
- generation manifest
- verify manifest

### 5.1.1 目录树生成策略

要求：

- 不能单目录下堆很多文件
- 不能目录层级过深
- 最终目录树尽量均衡

采用固定扇出、固定叶子容量的均衡树策略。

默认参数：

- 每个叶子目录最大文件数：`2048`
- 每个中间目录最大子目录数：`256`
- 目录深度固定为 3 层

目录结构示例：

```text
/bulk/<namespace_id>/
  d000000/
    s000000/
      f000000000
      f000000001
      ...
```

对 `1e8` 文件：

- `leaf_dir_count = ceil(1e8 / 2048) = 48829`
- `level1_dir_count = ceil(48829 / 256) = 191`

因此：

- 根目录一级子目录约 191 个
- 每个一级目录下最多 256 个二级目录
- 每个二级目录下最多 2048 个文件

### 5.1.2 inode 分配策略

每个 namespace 导入前一次性预留连续 inode 区间：

- 根目录 inode
- 中间目录 inode
- 叶子目录 inode
- 文件 inode

manifest 中记录：

- `root_inode`
- `inode_min`
- `inode_max`
- `inode_count`
- `dentry_count`

### 5.1.3 生成方式

必须采用流式生成：

- 不把 1 亿条 inode/dentry 全放内存
- 按目录分片顺序生成
- 边生成边写 staging 文件
- 边生成边更新统计和校验摘要

### 5.2 第二步：导入 Masstree

读取 staging 产物后执行导入：

1. 写 `inode_blob.bin`
2. 批量插入 inode 索引
3. 批量插入 dentry 索引
4. 做抽样验证
5. 发布 current pointer

## 6. 对现有 masstree 方法的复用方式

### 6.1 并行插入

复用现有方法：

- 每个 worker 线程执行 `thread_init`
- 每个 worker 按分片插入自己的 key 范围

分片建议：

- 以一级目录 `dXXXXXX` 为导入分片
- 每个 worker 负责若干一级目录

### 6.2 value 外置

复用现有“Masstree value 只存定位信息”的思路：

- inode 的完整属性放 `inode_blob.bin`
- Masstree 中 inode value 只存 offset
- dentry value 存 `child_inode + type`

### 6.3 读回验证

复用现有 `scan(key,1,...)` 思路：

- `Getattr(inode)` 先查 inode key，再到 blob 解码
- `Lookup(parent,name)` 直接查 dentry key
- `Readdir` 从 `MTD/<ns>/<parent_inode20>/` 前缀开始 scan

### 6.4 recover 重放

复用现有 recover 思路，但正式实现以 manifest 为准：

- 启动时扫描 active generations
- 读取 manifest
- 重放 inode / dentry 索引到内存 Masstree
- 成功后 namespace 才可见

## 7. 读路径接入

新增 `MasstreeMetaStore`，接口与现有 archive store 保持一致：

- `ResolvePath`
- `GetInode`
- `Readdir`

路由方式：

- `LookupByPath(path)` 命中 `path_prefix` 时走 Masstree namespace
- `Getattr(inode)` 通过 `inode_min / inode_max` shortlist 到 namespace
- `Readdir` 走 Masstree dentry scan

这样导入成功后，访问路径仍然统一走现有 MDS RPC，不需要单独暴露一套读 API。

## 8. 发布与恢复语义

状态机：

- `PREPARING`
- `GENERATED`
- `IMPORTING`
- `VERIFYING`
- `ACTIVE`
- `FAILED`

发布流程：

1. 生成到 `.../<namespace>/<generation>.staging/`
2. 导入 Masstree
3. 验证成功
4. staging rename 为正式 generation
5. 最后写 `current pointer`

要求：

- 导入失败的 generation 不可见
- 发布失败要支持补偿清理
- 启动时要支持恢复 current
- 后续支持按 generation 回滚

## 9. 校验方案

导入完成后必须做系统级验证：

- inode 总数一致
- dentry 总数一致
- 根目录 `Readdir` 正常
- 抽样 `10000` 个文件路径做 `Lookup`
- 抽样 `10000` 个 inode 做 `Getattr`
- 抽样 `1000` 个目录做分页 `Readdir`
- 校验分页 token 语义

verify manifest 建议记录：

- `inode_count`
- `dentry_count`
- `sample_lookup_ok`
- `sample_getattr_ok`
- `sample_readdir_ok`
- `content_hash`

## 10. 资源与容量控制

1 亿文件规模必须做资源预算。

约束要求：

- 生成阶段不得全量常驻内存
- 导入阶段必须按 batch/chunk 插入
- inode blob 必须顺序写
- namespace 导入前必须给出容量预估

导入前预估指标：

- `estimated_inode_blob_bytes`
- `estimated_dentry_index_entries`
- `estimated_masstree_bytes`
- `estimated_total_rss`

若超过阈值，应直接拒绝导入。

## 11. 接口建议

建议新增服务接口：

- `ImportMasstreeNamespace`
- `GetMasstreeImportJob`
- `ListMasstreeNamespaces`
- `RollbackMasstreeNamespace`

V1 若不做 job 化，可先做同步接口：

- `ImportMasstreeNamespace(namespace_id, generation_id, file_count, path_prefix, publish)`

返回：

- `inode_count`
- `dentry_count`
- `inode_min`
- `inode_max`
- `manifest_path`
- `verify_summary`

## 12. 默认参数

- `file_count = 100000000`
- `path_prefix = /bulk/<namespace_id>`
- `max_files_per_leaf_dir = 2048`
- `max_subdirs_per_dir = 256`
- `inode_insert_batch = 65536`
- `dentry_insert_batch = 131072`
- `verify_lookup_samples = 10000`
- `verify_readdir_samples = 1000`

## 13. 实施 Checklist

### 13.1 基础模型

- `[x]` 已定义 Masstree namespace manifest 结构
- `[x]` 已定义 `MTI/MTD/MTN/MTNC` key 编码规则
- `[x]` 已定义固定宽度 `inode20` 编码函数
- `[ ]` 定义 inode blob offset/value 编码格式
- `[x]` 已定义 dentry packed value 编码格式

### 13.2 Catalog 与路由

- `[x]` 已新增 `MasstreeNamespaceCatalog`
- `[ ]` 支持 `PutRoute/GetRoute/DeleteRoute/ListRoutes`
- `[x]` 已支持 `current generation pointer`
- `[x]` 已支持 `inode_min/inode_max` range 路由
- `[ ]` 将 Masstree route 接入 `MetaStoreRouter`

### 13.3 第一步：元数据生成器

- `[x]` 已新增 `MasstreeBulkMetaGenerator`
- `[x]` 已支持给定 `file_count` 生成均衡目录树
- `[x]` 已支持默认 3 层目录布局
- `[x]` 已支持连续 inode 区间分配
- `[x]` 已支持流式输出 inode/dentry 记录
- `[x]` 已生成 manifest 和 verify manifest

### 13.4 第二步：Masstree 导入器

- `[ ]` 新增 `MasstreeBulkImporter`
- `[x]` 已新增 `MasstreeBulkImporter`
- `[x]` 已支持写 `inode_blob.bin`
- `[x]` 已支持导入 inode 索引
- `[x]` 已支持导入 dentry 索引
- `[ ]` 支持分片并行导入
- `[ ]` 每个 worker 正确执行 `thread_init`
- `[ ]` 支持 batch/chunk 导入

### 13.5 Masstree 读存储

- `[ ]` 新增 `MasstreeMetaStore`
- `[ ]` 实现 `GetInode`
- `[ ]` 实现 `Lookup(parent, name)`
- `[ ]` 实现 `ResolvePath`
- `[ ]` 实现分页 `Readdir`
- `[ ]` 给 `Readdir` 接 generation-bound token

### 13.6 发布与恢复

- `[ ]` 新增 `MasstreeGenerationPublisher`
- `[ ]` staging 构建完成后再发布
- `[ ]` route/current pointer 最后写入
- `[ ]` 发布失败后做补偿清理
- `[ ]` 启动时扫描 active generation 并重放
- `[ ]` 缺失 current 时支持自动恢复

### 13.7 验证

- `[ ]` 抽样 `Lookup` 校验
- `[ ]` 抽样 `Getattr` 校验
- `[ ]` 抽样分页 `Readdir` 校验
- `[ ]` 校验 inode/dentry 总数
- `[ ]` 生成 verify summary

### 13.8 服务与工具

- `[ ]` 新增 `ImportMasstreeNamespace` 服务接口
- `[x]` 已新增 `masstree_meta_import_tool`
- `[x]` 已新增 `masstree_meta_generate_tool`
- `[ ]` 支持指定 `namespace_id / generation_id / file_count`
- `[ ]` 支持只生成不发布
- `[ ]` 支持导入完成后自动发布
- `[ ]` 支持查看 namespace 当前 generation

### 13.9 观测与容量控制

- `[ ]` 暴露导入耗时 metrics
- `[ ]` 暴露导入条目数 metrics
- `[ ]` 暴露恢复耗时 metrics
- `[ ]` 增加导入前容量预估
- `[ ]` 超阈值时拒绝导入

## 14. 推荐实施顺序

1. 先做 key/value 编码和 manifest 模型
2. 再做 `MasstreeNamespaceCatalog`
3. 再做元数据生成器
4. 再做导入器
5. 再做 `MasstreeMetaStore`
6. 接入 `MetaStoreRouter`
7. 最后补发布、恢复、验证和工具

## 15. V1 完成标准

满足以下条件即可认为 V1 可用：

- 能按 `namespace_id` 导入默认 1 亿文件的均衡目录树元数据
- 导入失败不可见
- 导入成功后 `Lookup/Getattr/Readdir` 可正常读取
- 重启后可恢复 namespace 可访问状态
- 支持至少一个可操作的导入工具或 RPC
