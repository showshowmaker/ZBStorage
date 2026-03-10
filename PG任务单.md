任务单（按落地顺序，含可并行组A详细设计）

T00 基线与开关
进度（2026-03-09）：✅ 已完成（代码已落地，待依赖环境补齐后统一编译回归）。
改动文件：`mds_server.cpp`、`MdsConfig.h`、`MdsConfig.cpp`。
实现要点：增加 `ENABLE_PG_LAYOUT`、`PG_VIEW_EPOCH`、`LAYOUT_MODE` 配置开关，默认关闭新路径。
验收标准：不开开关时行为与当前一致；开关打开后走新代码分支。

---

可并行组A：T01/T02/T03/T04（本文件已细化到可实现级）

统一设计前提（组A共用）
1. 保留旧路径：`inode -> chunk -> replicas`，新路径只在 `ENABLE_PG_LAYOUT=true` 生效。
2. 不改现有线上 key 前缀语义（`I/ D/ C/ RC/ ...`），只新增前缀。
3. 组A只完成“放置与元数据基础设施”，不改客户端协议语义（协议扩展在 T05）。
4. 所有新能力都必须携带 `epoch`，用于视图一致性与失效重试。

T01 定义 PG 视图与放置接口
进度（2026-03-09）：✅ 已完成（PGManager + mds_server 接线已落地）。
改动文件（新增）：`src/mds/allocator/PGManager.h`、`src/mds/allocator/PGManager.cpp`。
协同改动：`src/mds/server/mds_server.cpp`（初始化与刷新接线）、`CMakeLists.txt`（加入新源文件）。

设计目标
1. 把“对象 -> PG -> 副本组”映射独立出来，避免分配器直接随机挑节点。
2. 映射结果在同一 `epoch` 下稳定，可复现；`epoch` 切换后允许变化。
3. 读路径可无锁或近似无锁查询，写路径可按快照替换。

核心数据模型
1. `PgReplicaMember`：`node_id/address/disk_id/group_id/epoch/primary-secondary/sync_ready/node_type`。
2. `PgReplicaSet`：`pg_id`、`replica_count`、`members[]`、`view_epoch`。
3. `PlacementView`：`epoch`、`pg_count`、`replica`、`pg_to_members`（定长数组或 map）。

接口定义（设计约束）
1. `uint32_t ObjectToPg(std::string_view object_id, uint64_t epoch_hint = 0) const;`
2. `bool ResolvePg(uint32_t pg_id, uint64_t epoch, PgReplicaSet* out, std::string* error) const;`
3. `uint64_t CurrentEpoch() const;`
4. `bool ReplaceView(const PlacementView& view, std::string* error);`
5. `bool RebuildFromNodes(const std::vector<NodeInfo>& nodes, uint32_t pg_count, uint32_t replica, uint64_t new_epoch, std::string* error);`

放置算法
1. `object_id -> pg_id`：`hash64(object_id) % pg_count`，哈希固定为稳定实现（如 FNV-1a 64），跨进程一致。
2. `pg_id -> 副本组`：对可分配节点集合做 Rendezvous Hash（或一致性排序），取前 `replica` 个不同 `group_id` 节点。
3. 节点不可分配（`allocatable=false` 或无健康盘）时不进入候选集。
4. `disk_id` 选择复用当前 `NodeStateCache` 现有选盘策略，保证与旧分配器行为一致。

并发与可见性
1. 读路径：查询当前视图快照，不阻塞写线程。
2. 写路径：构建新视图后原子替换，旧视图可被并发读完后释放。
3. 失败策略：`ResolvePg` 失败要返回明确错误（`epoch not found`、`pg out of range`、`replica insufficient`）。

可观测性
1. 日志必须打印：`object_id`（可裁剪）、`pg_id`、`epoch`、`replica_nodes`。
2. 计数器建议：`pg_resolve_ok/pg_resolve_fail/pg_view_replace`。

验收标准
1. 同一 `object_id` 在同 `epoch` 下 `ObjectToPg` 结果稳定。
2. 切换到新 `epoch` 后，允许部分 PG 副本组变化。
3. 节点缩容/故障后，`ResolvePg` 不返回不可分配节点。

T02 改造分配器为 PG 驱动
进度（2026-03-09）：✅ 已完成（ChunkAllocator 新增 PG 分配接口并接开关）。
改动文件：`src/mds/allocator/ChunkAllocator.h`、`src/mds/allocator/ChunkAllocator.cpp`。
协同改动：`src/mds/server/mds_server.cpp`（注入 `PGManager*`）、`src/mds/service/MdsServiceImpl.cpp`（后续调用切换点预留）。

设计目标
1. 保留旧接口兼容老路径。
2. 增加新接口按 PG 结果分配副本，不再直接随机选节点。
3. 让分配层输出标准 `ReplicaLocation`，对上层透明。

接口变更（向后兼容）
1. 保留：`AllocateChunk(replica, chunk_id, out_replicas)`。
2. 新增：`AllocateObjectByPg(object_id, epoch, replica, out_replicas, error)`。
3. 新增可选开关：`SetPgEnabled(bool)` 或构造注入 `enable_pg_layout`。

行为定义
1. `ENABLE_PG_LAYOUT=false`：全部走旧 `PickNodes` 路径。
2. `ENABLE_PG_LAYOUT=true`：优先走 `AllocateObjectByPg`，内部调用 `ObjectToPg + ResolvePg`。
3. `ResolvePg` 成功时，填充 `ReplicaLocation` 的 `node_id/address/disk_id/group_id/epoch` 与主从字段。
4. `ResolvePg` 失败时，不静默降级；返回错误给上层，由上层决定重试或失败。

约束与边界
1. 返回副本数必须等于请求副本数，否则失败。
2. 同一分配结果中禁止重复 `group_id`（容灾域隔离）。
3. 无可用 PG 视图时，错误信息必须包含 `epoch` 和 `object_id`。

验收标准
1. 新接口不调用随机选节点逻辑。
2. 同 `object_id + epoch` 分配结果稳定。
3. 旧接口行为保持不变，旧回归测试通过。

T03 元数据键空间扩展（layout root + layout object + pg view）
进度（2026-03-09）：✅ 已完成（`LR/LO/PV` key 与 parse helper 已落地）。
改动文件：`src/mds/storage/MetaSchema.h`。

设计目标
1. 增加布局入口、布局对象、PG 视图三个命名空间。
2. 通过前缀隔离新旧元数据，便于灰度与回滚。

新增 key 设计
1. `LayoutRootKey(inode_id)` -> `LR/<inode_id>`。
2. `LayoutRootPrefix()` -> `LR/`。
3. `LayoutObjectKey(layout_obj_id)` -> `LO/<layout_obj_id>`。
4. `LayoutObjectPrefix()` -> `LO/`。
5. `PgViewKey(epoch, pg_id)` -> `PV/<epoch>/<pg_id>`。
6. `PgViewEpochKey()` -> `PV/current_epoch`。
7. `PgViewPrefix(epoch)` -> `PV/<epoch>/`（用于视图批量加载）。

命名规则
1. 前缀统一大写短码，保持与现有 `I/C/D/RC` 风格一致。
2. 函数只做纯字符串拼装，不耦合 RocksDB。
3. 若需要反解，提供 `ParseLayoutRootKey/ParsePgViewKey`。

兼容策略
1. 旧 key 全部保留，不改现有函数签名。
2. 新旧路径并存阶段，读路径先看 `LR/`，miss 后回退 `C/`（回退策略具体在 T06/T18）。

验收标准
1. 新增 key 函数覆盖 inode 入口、布局对象、PG 视图三类数据。
2. key 格式稳定、可单元测试验证。

T04 元数据编解码扩展
进度（2026-03-09）：✅ 已完成（LayoutRoot/LayoutNode/PgView 编解码 + envelope/crc 已落地）。
改动文件：`src/mds/storage/MetaCodec.h`、`src/mds/storage/MetaCodec.cpp`。

设计目标
1. 增加布局相关记录的编解码能力。
2. 保持旧 `InodeAttr/ChunkMeta/UInt64` 编解码不变。
3. 为后续崩溃恢复和数据校验预留版本与校验位。

新增记录类型（本地元数据对象）
1. `LayoutRootRecord`：`inode_id/layout_root_id/layout_version/file_size/epoch/update_ts`。
2. `LayoutExtentRecord`：`logical_offset/length/object_id/object_offset/object_length/object_version`。
3. `LayoutNodeRecord`：`node_id/level/repeated extents/repeated child_layout_ids`。
4. `PgViewRecord`（可选）：`epoch/pg_id/repeated members`（供 T01 落盘恢复）。

编码格式设计
1. 统一 envelope：`magic + format_version + payload_len + payload + crc32`。
2. `payload` 使用 protobuf 二进制序列化（内部消息），保证扩展字段兼容。
3. decode 失败时区分：版本不支持、CRC 错误、payload 解析失败。

接口建议
1. `EncodeLayoutRoot/DecodeLayoutRoot`
2. `EncodeLayoutNode/DecodeLayoutNode`
3. `EncodePgView/DecodePgView`（若 T01 选择落盘恢复）
4. 旧接口签名不动，避免影响现有调用方。

鲁棒性要求
1. 解码必须做长度和 CRC 校验，禁止直接信任输入。
2. 对损坏数据返回 `false`，不抛异常，不崩溃。

验收标准
1. 编码-解码回环一致。
2. 损坏数据（截断、篡改 CRC）能被识别。
3. 与旧 inode/chunk 元数据完全兼容。

---

组A并行拆分建议（开发排期）
1. 线程A：T01（PGManager）+ mds_server 接线。
2. 线程B：T02（ChunkAllocator PG接口）+ 旧接口回归。
3. 线程C：T03（MetaSchema 新 key）。
4. 线程D：T04（MetaCodec 新记录编解码）。
5. 合并顺序：先合 T03/T04，再合 T01，最后合 T02（减少接口冲突）。

组A统一验收口径
1. 功能正确：对象定位结果稳定、key 和 codec 可回环、错误可诊断。
2. 兼容可回退：开关关闭后仍走旧路径，老测试不过滤。
3. 可观测：日志能定位 `inode/layout_root/object_id/pg_id/epoch`。

---

后续任务（保持原计划）

T05 协议扩展（MDS <-> client）
进度（2026-03-09）：✅ 已完成（`GetLayoutRoot/ResolveLayout/CommitLayoutRoot` 已扩展到 proto）。
改动文件：`mds.proto`。
实现要点：新增 `GetLayoutRoot`、`ResolveLayout`、`CommitLayoutRoot`（或在现有 `GetLayout/CommitWrite` 增加新字段并兼容）。
验收标准：proto 生成通过；旧 RPC 仍可用。

T06 MDS 服务接口接入新协议
进度（2026-03-09）：✅ 已完成（`LoadLayoutRoot/StoreLayoutRootAtomic/ResolveExtents/BuildReadPlan` 已落地）。
改动文件：`MdsServiceImpl.h`、`MdsServiceImpl.cpp`。
实现要点：增加 `LoadLayoutRoot`、`StoreLayoutRootAtomic`、`ResolveExtents`、`BuildReadPlan`。
验收标准：MDS 返回的是“布局入口/解析结果”，不是完整物理落点写死表。

T07 写路径切换为 COW + 原子切根
进度（2026-03-09）：✅ 已完成（写入 pending 事务 + commit 发布 chunk + 原子切根）。
改动文件：`MdsServiceImpl.cpp`。
实现要点：`AllocateWrite` 只给写入计划；写入后 `CommitLayoutRoot` 原子更新 inode 当前根。
验收标准：切根前只能读旧版本，切根后读新版本；中途失败不污染可见版本。

T08 客户端读路径改为“布局解析 + PG 定位”
进度（2026-03-09）：✅ 已完成（读路径默认走 `ResolveLayout`，增加 inode 级布局缓存；仅在旧协议 MethodNotFound 时回退 `GetLayout`，读副本失败自动切换）。
改动文件：`zb_fuse_client.cpp`。
实现要点：先取 `layout_root`，再解析 extent 拿 `object_id`，再 `object_id -> pg_id -> replicas` 读。
验收标准：MDS 不在数据热路径；读重试支持副本切换。

T09 客户端写路径接入新提交流程
进度（2026-03-09）：✅ 已完成（写/截断路径默认走 `GetLayoutRoot + CommitLayoutRoot`；`CommitWrite` 仅在旧协议 MethodNotFound 时兼容回退，避免冲突场景误降级）。
改动文件：`zb_fuse_client.cpp`。
实现要点：单写者租约、先写新对象、后提交新 `layout_root`。
验收标准：并发写冲突返回明确错误；成功提交可立即可见。

T10 客户端 PG 视图缓存与 epoch 失效处理
进度（2026-03-09）：✅ 已完成（客户端增加 inode->epoch 缓存，`STALE_EPOCH` 自动刷新并重试）。
改动文件：`zb_fuse_client.cpp`。
实现要点：本地缓存 pg view，遇到 `STALE_EPOCH` 自动刷新并重试。
验收标准：PG 视图变更时客户端可自愈，不需重启。

T11 数据节点对象抽象层
进度（2026-03-09）：✅ 已完成（新增 `ObjectStore` 抽象 + real/virtual chunk 适配器）。
改动文件（新增）：`ObjectStore.h`、`RealChunkObjectStore.cpp`、`VirtualChunkObjectStore.cpp`。
实现要点：定义 `PutObject/GetObject/DeleteObject`，屏蔽 real/virtual/optical 存储差异。
验收标准：real/virtual 可先用现有 chunk 存储做适配实现。

T12 real node 接入对象接口
进度（2026-03-09）：✅ 已完成（服务层已增加 `PutObject/GetObject/DeleteObject` 的 chunk 适配实现）。
改动文件：`StorageServiceImpl.h`、`StorageServiceImpl.cpp`。
实现要点：新增 object RPC 处理或 chunk->object 适配。
验收标准：读写对象与现有副本机制兼容。

T13 virtual node 接入对象接口
进度（2026-03-09）：✅ 已完成（与 real node 对齐，已增加对象接口适配实现）。
改动文件：`VirtualStorageServiceImpl.h`、`VirtualStorageServiceImpl.cpp`。
实现要点：与 real node 语义一致，失败修复队列继续可用。
验收标准：virtual 在新路径读写语义与 real 对齐。

T14 optical 节点只作为介质层/缓存层
进度（2026-03-09）：✅ 已完成（`BuildReadPlan` 只返回 DISK+READY 副本，客户端读写路径也仅访问 DISK tier；optical 仅用于内部归档/召回模拟）。
改动文件：`OpticalStorageServiceImpl.cpp`、`ImageStore.cpp`。
实现要点：保留整盘加载与刻录模拟；禁止客户端直读 optical tier。
验收标准：对外可见副本仍为 DISK tier；光盘路径仅内部迁移/归档使用。

T15 小文件策略（内联/外置阈值）
进度（2026-03-09）：⏸ 暂缓（按当前要求关闭小文件优化，大小文件统一走同一套布局/对象逻辑，后续需要时再单独开启优化分支）。
改动文件：`MdsServiceImpl.cpp`、`MdsConfig.cpp`。
实现要点：小于阈值内联到 inode；超过阈值升级为布局对象。
验收标准：小文件对象数量明显下降；升级过程无可见性错误。

T16 孤儿对象与旧版本 GC
进度（2026-03-09）：✅ 已完成（新增 `GcManager`，实现 layout object 可达性扫描与延迟回收，并接入 mds_server 定时线程）。
改动文件（新增）：`GcManager.h`、`GcManager.cpp`。
实现要点：基于“可达 layout_root 集”做标记清理；延迟回收旧版本对象。
验收标准：崩溃后可回收未提交对象；不误删当前版本对象。

T17 布局对象可靠性提升
进度（2026-03-09）：✅ 已完成（提交 `layout_root` 时原子写入 `LO + LOR` 多副本；读路径按 `scrub_on_load` 做解码校验、镜像回填与缺失重建；GC 增加 `LOR` 清理）。
改动文件：`OpticalArchiveManager.cpp`、`MdsServiceImpl.cpp`。
实现要点：布局对象使用更高副本级别与巡检优先级。
验收标准：注入单副本损坏后，布局仍可恢复解析。

T18 兼容迁移工具（旧 chunk 元数据 -> layout_root）
进度（2026-03-09）：✅ 已完成（新增 `migrate_chunk_to_layout` 离线迁移工具，支持断点续跑与幂等）。
改动文件（新增）：`migrate_chunk_to_layout.cpp`。
实现要点：离线迁移、可中断续跑、幂等。
验收标准：迁移前后文件校验一致；可分批灰度。

T19 一键测试脚本升级
进度（2026-03-09）：✅ 已完成（`run_full_e2e_suite.sh` 已覆盖 `layout_commit_conflict_test`、`layout_cow_rw_via_fuse`、`migrate_chunk_to_layout_dry_run`、`pg_migration_sim_test`、`layout_gc_reclaim_orphan_object`，并新增对应开关）。
改动文件：`run_full_e2e_suite.sh`、`test.md`。
实现要点：覆盖新读写流程、PG 迁移、epoch 失效重试、COW 可见性、GC 回收。
验收标准：新增用例稳定通过，并保留旧路径回归测试。
