# Masstree稀疏索引改造方案V1

## 1. 目标

把当前已经接入 MDS 的 Masstree 实现，从“逐条记录索引”改成“页级稀疏索引 + 页内二分查找”。

目标效果：

- Masstree 内存中只保留稀疏页索引
- 真实 inode/dentry 记录顺序写入磁盘页文件
- 点查流程变成“Masstree 找页 -> 读页 -> 页内二分”
- `Readdir` 变成“定位首个页 -> 页内顺扫 -> 跨页续扫”
- 导入时不再把 1 亿文件对应的全部 inode/dentry 逐条灌进 Masstree
- 单 namespace 的常驻内存显著下降

## 2. 当前实现与目标实现的差异

### 2.1 当前实现

当前 MDS 接入代码在以下文件中：

- [MasstreeBulkImporter.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeBulkImporter.cpp)
- [MasstreeIndexRuntime.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeIndexRuntime.cpp)
- [MasstreeMetaStore.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeMetaStore.cpp)

现状特征：

- inode 走 `MTI/<namespace>/<inode>`，每个 inode 一条 Masstree entry
- dentry 走 `MTD/<namespace>/<parent>/<name>`，每个 dentry 一条 Masstree entry
- `inode_blob.bin` 只把 inode 正文外置了，但 inode 索引仍是逐条的
- `EnsureGenerationLoaded()` 会把 generation 重新 materialize 成内存索引

### 2.2 目标实现

参考原型路径：

- [recover.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\masstree\src\recover.cpp)
- [masstree_wrapper.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\masstree\index\masstree\masstree_wrapper.h)

目标特征：

- inode 与 dentry 都改成页级顺序段文件
- Masstree 中只保存“页最大 key -> page_id/page_offset”
- 查到页后在内存 buffer 上做二分
- generation 装载时只加载 sparse index，不重建全量逐条索引

## 3. 新的数据布局

每个 Masstree generation 目录新增或替换为以下文件：

- `inode_pages.seg`
- `dentry_pages.seg`
- `inode_sparse.idx`
- `dentry_sparse.idx`
- `manifest.txt`
- `cluster_stats.txt`
- `allocation_summary.txt`

### 3.1 inode_pages.seg

顺序页文件，每页固定大小，建议先用 `64KB`。

页内按 `inode_id` 升序排列。

每条 inode record：

- `inode_id`
- `MasstreeInodeRecord`

### 3.2 dentry_pages.seg

顺序页文件，每页固定大小，建议先用 `64KB`。

页内按 `(parent_inode, name)` 的字典序升序排列。

每条 dentry record：

- `parent_inode`
- `name_len`
- `name_bytes`
- `child_inode`
- `type`

注意：

- 排序比较时不能把 `name_len` 放在 `name_bytes` 前面参与字典序
- 页内序必须与查询使用的 lower_bound 序一致

### 3.3 inode_sparse.idx

每页 1 条稀疏索引记录：

- `max_inode_id`
- `page_id` 或 `page_offset`

### 3.4 dentry_sparse.idx

每页 1 条稀疏索引记录：

- `max_parent_inode`
- `max_name`
- `page_id` 或 `page_offset`

### 3.5 manifest.txt 需要新增字段

在 [MasstreeManifest.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeManifest.h) 中增加：

- `inode_pages_path`
- `dentry_pages_path`
- `inode_sparse_index_path`
- `dentry_sparse_index_path`
- `page_size_bytes`
- `inode_page_count`
- `dentry_page_count`
- `inode_record_count`
- `dentry_record_count`

## 4. 新的运行时模型

### 4.1 Masstree 内存中存什么

只存两棵稀疏索引树：

- inode sparse tree
- dentry sparse tree

每条 entry 的 value 是：

- `page_id`
  或
- `page_file_offset`

不再存：

- inode -> blob_offset 的逐条索引
- dentry -> child 的逐条索引

### 4.2 generation 装载时做什么

`EnsureGenerationLoaded()` 只做：

1. 读取 manifest
2. 打开 `inode_pages.seg` / `dentry_pages.seg`
3. 加载 `inode_sparse.idx` / `dentry_sparse.idx`
4. 将 sparse index 插入 Masstree runtime

不再做：

- 读取全量 inode record 并逐条插入
- 读取全量 dentry record 并逐条插入

## 5. 按文件拆分的代码改造方案

## 5.1 [MasstreeManifest.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeManifest.h) / [MasstreeManifest.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeManifest.cpp)

### 目标

让 manifest 能描述页文件和稀疏索引文件，而不是只描述 records/blob。

### 修改点

- 新增页文件路径字段
- 新增 sparse index 路径字段
- 新增页大小和页数统计字段
- 保留现有统计字段和光盘分配字段

### 兼容策略

建议新增版本字段：

- `layout_version=2`

含义：

- `1` 代表旧的逐条索引布局
- `2` 代表新的稀疏索引布局

查询侧根据 `layout_version` 决定走哪套 reader。

## 5.2 [MasstreeIndexRuntime.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeIndexRuntime.h) / [MasstreeIndexRuntime.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeIndexRuntime.cpp)

### 当前问题

接口以逐条记录为中心。

### 目标接口

保留 namespace 维度，但把接口改成页边界语义：

- `PutInodePageBoundary(namespace_id, max_inode_id, page_id, error)`
- `PutDentryPageBoundary(namespace_id, parent_inode, max_name, page_id, error)`
- `FindInodePage(namespace_id, inode_id, page_id, error)`
- `FindDentryPage(namespace_id, parent_inode, name, page_id, error)`

### 修改点

- 废弃 `PutInodeOffset`
- 废弃 `PutDentryValue`
- 废弃逐条 `GetInodeOffset`
- 废弃逐条 `GetDentryValue`
- `ScanDentryPrefix` 改成页级扫描起点定位

### 编码建议

inode sparse key：

- `MTI2/<namespace>/<max_inode_id>`

dentry sparse key：

- `MTD2/<namespace>/<parent_inode>/<max_name>`

使用新前缀是为了避免与旧布局混淆。

## 5.3 新增 [MasstreePageLayout.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageLayout.h)

### 目标

集中定义页头、record 编码、二分辅助结构。

### 建议内容

- `constexpr uint32_t kDefaultPageSize = 64 * 1024;`
- `struct InodePageHeader`
- `struct DentryPageHeader`
- `struct InodeSparseEntry`
- `struct DentrySparseEntry`
- `EncodeInodePage(...)`
- `DecodeInodePage(...)`
- `EncodeDentryPage(...)`
- `DecodeDentryPage(...)`

### 原因

不要把页格式散落在 importer 和 reader 两头，否则后面很难校验兼容性。

## 5.4 新增 [MasstreePageReader.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageReader.h) / [MasstreePageReader.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageReader.cpp)

### 目标

把“读页 + 页内二分”从 `MasstreeMetaStore` 中抽出来。

### 主要接口

- `LoadInodePage(file, page_id, page, error)`
- `LoadDentryPage(file, page_id, page, error)`
- `FindInodeInPage(page, inode_id, record, error)`
- `FindDentryInPage(page, parent_inode, name, entry, error)`
- `LowerBoundDentryInPage(page, parent_inode, start_after, index)`

### 额外建议

可加一个很小的 LRU page cache：

- `inode_page_cache`
- `dentry_page_cache`

先做 generation 内 128 页或 256 页缓存即可。

## 5.5 [MasstreeBulkImporter.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeBulkImporter.h) / [MasstreeBulkImporter.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeBulkImporter.cpp)

### 当前问题

它当前会逐条写：

- inode -> offset
- dentry -> packed value

并把每条记录都插到 Masstree。

### 目标流程

改成两阶段：

1. 构页
2. 构 sparse index

### inode 流程

1. 顺序读 `inode.records`
2. 填充当前 inode page buffer
3. 页满后：
   - 写入 `inode_pages.seg`
   - 取本页 `max_inode_id`
   - 生成一条 `InodeSparseEntry`
4. 导入完成后：
   - 顺序写 `inode_sparse.idx`
   - 将每个 `InodeSparseEntry` 插入 Masstree sparse runtime

### dentry 流程

1. 顺序读 `dentry.records`
2. 填充当前 dentry page buffer
3. 页满后：
   - 写入 `dentry_pages.seg`
   - 取本页最大 `(parent_inode, name)`
   - 生成一条 `DentrySparseEntry`
4. 导入完成后：
   - 顺序写 `dentry_sparse.idx`
   - 将每个 `DentrySparseEntry` 插入 Masstree sparse runtime

### 需要删除或废弃的逻辑

- 导入时对每条 inode 调 `PutInodeOffset`
- 导入时对每条 dentry 调 `PutDentryValue`
- 基于全量 runtime 的逐条抽样校验

### 需要新增的校验

- inode page 内部有序
- dentry page 内部有序
- 相邻 sparse entry 单调递增
- 随机抽样点查：
  - 先 sparse 定位页
  - 再页内二分验证

## 5.6 [MasstreeMetaStore.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeMetaStore.h) / [MasstreeMetaStore.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeMetaStore.cpp)

### 当前问题

读路径默认假设 runtime 里有逐条 entry。

### 目标改法

`LoadedGeneration` 中改成保存：

- manifest
- `inode_pages.seg` 文件句柄
- `dentry_pages.seg` 文件句柄
- sparse runtime
- 小型页缓存

### GetInode

新流程：

1. `FindInodePage(namespace, inode_id)`
2. `LoadInodePage(page_id)`
3. `FindInodeInPage(inode_id)`
4. 返回 `MasstreeInodeRecord.attr`

### FindDentry / ResolvePath

新流程：

1. `FindDentryPage(namespace, parent_inode, name)`
2. `LoadDentryPage(page_id)`
3. 页内二分 `(parent_inode, name)`
4. 拿到 child inode

### Readdir

新流程：

1. 用 `(parent_inode, start_after)` 做 sparse lower_bound
2. 读命中的首个页
3. 从页内 lower_bound 开始顺扫
4. 如果页不够，顺序读下一个页
5. 如果页已跨出该 parent_inode，停止

### 分页 token

token 建议仍然带：

- `generation_id`
- `parent_inode`
- `last_name`
- 可选 `page_id`

不要把 token 绑定到内存指针。

### EnsureGenerationLoaded

新流程：

1. 读 manifest
2. 打开 `inode_pages.seg` 和 `dentry_pages.seg`
3. 读取 `inode_sparse.idx`
4. 读取 `dentry_sparse.idx`
5. 将 sparse entry 插入 Masstree

要删除的逻辑：

- 从 `inode_blob.bin` 反向全量重建逐条 runtime

## 5.7 [MasstreeImportService.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeImportService.cpp)

### 目标

保持上层调用接口不变，但导入产物从旧布局切到新布局。

### 修改点

- importer result 中增加：
  - `inode_page_count`
  - `dentry_page_count`
  - `page_size_bytes`
- manifest 写入新布局字段
- stats 逻辑保持不变
- publish 前验证页文件和 sparse index 已完整落盘

### 边界

job、RPC、命名空间发布逻辑不需要重写，只需要换 importer 产物。

## 5.8 [MasstreeGenerationPublisher.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeGenerationPublisher.cpp)

### 目标

发布逻辑总体不变，但 manifest 路径修正规则要覆盖新文件。

### 修改点

- 重写 manifest 中：
  - `inode_pages_path`
  - `dentry_pages_path`
  - `inode_sparse_index_path`
  - `dentry_sparse_index_path`
- 发布后 route 仍然只需要指向 current generation manifest

## 5.9 [MasstreeBulkMetaGenerator.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeBulkMetaGenerator.cpp)

### 目标

尽量少改。

### 建议

生成器仍然输出：

- `inode.records`
- `dentry.records`

保持它是 importer 的上游有序输入，不在生成器阶段引入 page 逻辑。

原因：

- 生成器负责逻辑数据模型
- importer 负责物理存储布局

## 5.10 [MasstreeStatsStore.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreeStatsStore.cpp)

### 目标

尽量不动统计语义。

### 修改点

- namespace stats 可增加：
  - `inode_page_count`
  - `dentry_page_count`
  - `layout_version`
- cluster stats 不需要因稀疏索引改造而改变模型

## 5.11 [MdsServiceImpl.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\service\MdsServiceImpl.cpp)

### 目标

RPC 接口尽量不变。

### 修改点

- `ImportMasstreeNamespace` 仍然走 import job
- `GetRandomMasstreeFileAttr` 无需改协议
- `GetFileLocation` 无需改协议
- 只要 `MasstreeMetaStore` 能读新布局，这层基本不用动

## 6. 新增文件建议

建议新增以下文件：

- [MasstreePageLayout.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageLayout.h)
- [MasstreePageLayout.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageLayout.cpp)
- [MasstreePageReader.h](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageReader.h)
- [MasstreePageReader.cpp](c:\Users\w1j2h\Desktop\AllZB\ZBPro\ZBStorage\src\mds\masstree_meta\MasstreePageReader.cpp)

原因：

- 避免 importer 和 reader 共用一套隐式页格式
- 减少后续协议漂移

## 7. 分阶段实施计划

## 阶段 1：先改 dentry 稀疏索引

原因：

- dentry 数量大
- `Readdir` 和路径解析都依赖它
- 对内存下降最敏感

工作项：

- dentry page format
- dentry sparse idx
- dentry sparse runtime
- `FindDentry` / `Readdir` 切换

## 阶段 2：再改 inode 稀疏索引

工作项：

- inode page format
- inode sparse idx
- `GetInode` 切换

## 阶段 3：切掉旧逐条 runtime

工作项：

- 删除 `PutInodeOffset` 使用路径
- 删除 `PutDentryValue` 使用路径
- 删除全量 materialize

## 阶段 4：补压测与回归

工作项：

- 1 万文件回归
- 100 万文件回归
- 1 亿文件内存测量
- 重启后按需装载验证

## 8. 验收标准

## 功能正确性

- `ImportMasstreeNamespace` 成功
- `GetRandomMasstreeFileAttr` 成功
- `Lookup/Getattr/Readdir` 与旧布局语义一致
- `GetFileLocation` 仍能返回 image 归属

## 内存行为

- 导入时不再因为逐条 runtime 造成线性膨胀
- 单 namespace 装载后内存主要由 sparse tree 和页缓存构成
- 相比旧实现 RSS 显著下降

## 持久化行为

- generation 目录中存在 page files 和 sparse index files
- 重启后按需加载成功
- 不要求重建全量逐条内存树

## 9. 具体 checklist

- [ ] 在 `MasstreeManifest` 中新增 sparse/page 字段和 `layout_version`
- [ ] 新增 `MasstreePageLayout` 定义统一页格式
- [ ] 新增 `MasstreePageReader` 负责页读取和页内二分
- [ ] 重构 `MasstreeIndexRuntime` 为页边界索引接口
- [ ] 重构 `MasstreeBulkImporter` 为“构页 + 写 sparse idx + sparse insert”
- [ ] 让 `MasstreeMetaStore::GetInode` 改走 sparse lookup + 页内二分
- [ ] 让 `MasstreeMetaStore::FindDentry` 改走 sparse lookup + 页内二分
- [ ] 让 `MasstreeMetaStore::Readdir` 支持跨页顺扫
- [ ] 删除 `EnsureGenerationLoaded` 中的全量逐条 materialize
- [ ] 更新 `MasstreeImportService` 的 manifest/result 逻辑
- [ ] 更新 `MasstreeGenerationPublisher` 的路径重写逻辑
- [ ] 更新 `CMakeLists.txt` 加入新 page 相关源码
- [ ] 为新旧布局增加最小兼容分支或一次性迁移策略
- [ ] 增加 10k / 1M / 100M 三档回归验证

## 10. 推荐实施顺序

1. 先实现 `MasstreePageLayout` 和 `MasstreePageReader`
2. 先切 dentry sparse path
3. 再切 inode sparse path
4. 再移除旧逐条 runtime
5. 最后做压测和 memory 对比

## 11. 一句话结论

这次改造的本质不是“优化几个常数”，而是把当前已经偏掉的实现，拉回到原型那条正确路线：

- Masstree 只做稀疏页索引
- 真实记录放页文件
- 查找靠“树定位页 + 页内二分”

只有改回这条路线，单 namespace 1 亿文件时的内存占用才会回到合理区间。
