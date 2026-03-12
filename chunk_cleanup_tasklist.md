# Chunk 残留清理任务清单

## 目标
- 最终形态：对外与对内统一使用 `object/layout/PG` 语义。
- 兼容策略：分阶段保留 `chunk_id` 兼容字段，逐步下线旧 RPC/旧 key/旧工具。

## 阶段与任务

### 阶段 A：控制面对象化（进行中）
1. `A1` 归档候选链路 object_id 优先（real/virtual/report -> mds）  
状态：`DONE`
- real/virtual 候选结构新增 `object_id`，并保留 `chunk_id` 兼容。
- 上报 MDS 时统一使用 `ArchiveObjectId()`，同时填充 `chunk_id/object_id`。

2. `A2` MDS 归档读取对象归属索引化（`OO/<object_id>`）  
状态：`DONE`
- 写入/回收/缓存路径补写 `OO`。
- 归档主链路优先查 `OO`，仅在缺失时兼容回退旧索引。

3. `A3` 在线读路径去 `RC->C` 回退  
状态：`DONE`
- optical 读计划严格依赖 `layout + AS + AOL`。

### 阶段 B：协议与服务接口去 chunk 化（待做）
1. `B1` `real_node.proto` 新增 object-only RPC（`WriteObject/ReadObject/DeleteObject`）并迁移调用方。
2. `B2` MDS/Client 不再发送 `WriteChunk/ReadChunk`；仅 object RPC。
3. `B3` optical 节点接口调整为镜像级读写 + 文件级索引访问，不再暴露 chunk API。

### 阶段 C：元数据与存储键去 chunk 化（待做）
1. `C1` `MetaSchema` 去别名：`ObjectKey/ObjectPrefix/ReverseObjectKey` 不再映射到 `C/RC`。
2. `C2` 新增对象元数据独立命名空间并迁移历史数据。
3. `C3` GC 停止扫 `C/RC`，改为新对象命名空间清理。

### 阶段 D：数据节点内部去 chunk 化（待做）
1. `D1` `ArchiveChunkMetaStore` 重命名为 `ArchiveObjectMetaStore`（含 WAL/snapshot 兼容升级）。
2. `D2` real/virtual 内部变量、函数、日志统一 object 命名。
3. `D3` 复制修复队列 key 从 `chunk_id` 迁移为 `object_id`。

### 阶段 E：客户端、测试、脚本去 chunk 化（待做）
1. `E1` FUSE 去 `READ_PLAN_SOURCE_CHUNK_FALLBACK` 分支与指标。
2. `E2` 测试用例改 object 命名与 object RPC。
3. `E3` 脚本参数重命名（`*_CHUNKS_*` -> `*_OBJECTS_*`），保留短期兼容映射。

## 本轮已完成改动（对应 A1）
- `src/data_node/real_node/service/ArchiveChunkMetaStore.h`
- `src/data_node/real_node/service/ArchiveChunkMetaStore.cpp`
- `src/data_node/real_node/service/StorageServiceImpl.h`
- `src/data_node/real_node/service/StorageServiceImpl.cpp`
- `src/data_node/real_node/server/real_node_server.cpp`
- `src/data_node/virtual_node/service/VirtualStorageServiceImpl.h`
- `src/data_node/virtual_node/service/VirtualStorageServiceImpl.cpp`
- `src/data_node/virtual_node/server/virtual_node_server.cpp`


## Progress Updates (ASCII)
- 2026-03-11: `B1` marked `DONE`.
- Scope:
  - Expanded `WriteObject/ReadObject` protobuf messages to carry optical archive context (`archive_op_id`, file identity/attrs, image fields).
  - Migrated MDS-side replica IO callers from `stub.WriteChunk/ReadChunk/DeleteChunk` to `stub.WriteObject/ReadObject/DeleteObject`.
  - Added object-RPC adapters in optical brpc service so object RPC can drive existing optical chunk/image internals.
- Compatibility:
  - Legacy chunk RPC definitions and handlers are still kept for transition; caller side is object-first.
- 2026-03-11: Continued de-chunk work (B2/E3 partial).
- Changes:
  - MDS no longer emits `READ_PLAN_SOURCE_CHUNK_FALLBACK`; read plans now expose `LAYOUT/OPTICAL` only.
  - FUSE client removed chunk-fallback counters/branch.
  - Added object-first config aliases:
    - `ARCHIVE_TRACK_MAX_OBJECTS` (real/virtual node config parser).
    - `ARCHIVE_MAX_OBJECTS_PER_ROUND` (mds config parser).
  - Startup/test scripts switched to object-first parameter names with legacy env fallback.
  - CLI/test arg migration: `real_node_client` and `virtual_node_test` support `--object_id` (keep `--chunk_id` as legacy alias).
  - `layout_consistency_check` terminology and counters switched from chunk-meta to object-meta.
- 2026-03-11: Continued de-chunk work (runtime semantics + naming).
- Changes:
  - Added `is_replication` to `WriteObjectRequest` and wired through object write path (proto -> brpc -> object store -> real/virtual service).
  - Removed MDS/client usage of `READ_PLAN_SOURCE_CHUNK_FALLBACK` in runtime behavior.
  - Updated config/script surface to object-first keys (`*_OBJECTS_*`) with legacy `*_CHUNKS_*` env fallback.
  - Updated `real_node_multi_test` local naming from chunk_id to object_id.
- 2026-03-11: Renamed read-plan enum value in `mds.proto` to `READ_PLAN_SOURCE_LEGACY_FALLBACK` (value=2 kept for compatibility).
- 2026-03-11: Continued service-internal de-chunk naming cleanup.
- Changes:
  - Real/virtual service internals renamed from `TrackChunkAccess/RemoveChunkTracking` to `TrackObjectAccess/RemoveObjectTracking`.
  - `UpdateArchiveState` parameter naming migrated from `chunk_id` to `object_id` in real/virtual service interfaces and implementations.
  - Introduced allocator naming bridge: `using ObjectAllocator = ChunkAllocator`, and switched MDS service/server construction sites to `ObjectAllocator` type.
- 2026-03-11: Continued de-chunk naming refactor in core modules.
- Changes:
  - MDS now uses `ObjectAllocator` type alias at construction/injection sites (backed by existing allocator implementation).
  - Real/virtual storage service internal APIs renamed to object-centric names (`TrackObjectAccess`, `RemoveObjectTracking`).
  - Real/virtual `UpdateArchiveState` argument naming changed to `object_id`.
  - Object store adapter classes renamed to `RealObjectStore` / `VirtualObjectStore`, with legacy aliases preserved.

- 2026-03-11: Continued de-chunk work (B2/D2 in-progress).
- Changes:
  - Introduced object-first internal storage-node messages in [src/msg/storage_node_messages.h] (Write/Read/Delete Object), with legacy chunk type aliases kept for compatibility.
  - Real/virtual/optical service implementations now expose object-first core methods (WriteObject/ReadObject/DeleteObject); chunk methods are compatibility wrappers only.
  - Real/virtual internals switched key paths to ArchiveObjectId() resolution for IO/replication/repair key generation.
  - Virtual node in-memory maps renamed to object-centric identifiers (object_data_, object_home_disk_, etc.).
  - Optical node cache/idempotency maps renamed to object-centric identifiers and object-id lookup path.
  - Real/virtual server startup switched to object-first setter (SetArchiveTrackingMaxObjects).
- Compatibility:
  - Legacy chunk RPC handlers and fields remain available at protocol boundary; internal execution path is object-first.

- 2026-03-11: Added object-first adapters in object stores and path resolver naming cleanup (LocalPathResolver params now object-centric).
- 2026-03-11: Added ArchiveObjectMetaStore/ArchiveObjectMeta aliases and switched real/virtual service member types to object aliases.
- 2026-03-11: ImageStore now exposes object-first API wrappers (WriteObject/ReadObject/DeleteObject), and optical service path switched to these object methods.
- 2026-03-11: module_io_smoke_test switched real/virtual/optical IO checks to object RPC (WriteObject/ReadObject/DeleteObject); case labels renamed to *_object_rw.
- 2026-03-11: Archive meta store gained object-named wrappers (SetMaxObjects/TrackObjectAccess/RemoveObject/UpdateObjectArchiveState) and real/virtual service call sites migrated to wrappers.
- 2026-03-11: ImageStore switched to object-first concrete implementations (WriteObject/ReadObject/DeleteObject in .cpp); chunk APIs are now compatibility wrappers in header.
- 2026-03-11: Added object-named wrappers in ArchiveBatchStager and migrated OpticalArchiveManager stager call sites (StageObject/ContainsObject/ReadObjectData/MarkObjectDone/RemoveObject).

- 2026-03-11: Continued de-chunk cleanup (D1/B2 internal naming pass).
- Changes:
  - Fixed ArchiveObjectMetaStore post-refactor mismatch (Init now consistently uses max_objects; eviction bound renamed to max_objects_).
  - ArchiveBatchStager internals renamed object-first (StagedArchiveObject, object_key, objects_, object_order_, ObjectFilePathLocked), while keeping StagedArchiveChunk/chunk-method wrappers for compatibility.
  - OpticalArchiveManager staging path renamed from file-chunk task structs to file-object task structs and switched sealed-batch consume path to staged.object_key.
  - Allocator core class switched to object-first (ObjectAllocator primary type; ChunkAllocator kept as alias for compatibility).

- 2026-03-11: Continued de-chunk cleanup (B1/B2 protocol cutover).
- Changes:
  - Removed legacy chunk RPC definitions from src/msg/real_node.proto (WriteChunk/ReadChunk/DeleteChunk + request/reply messages).
  - Removed chunk RPC handlers from all three node brpc services: real, virtual, optical.
  - Removed chunk-wrapper methods from StorageServiceImpl/VirtualStorageServiceImpl/OpticalStorageServiceImpl and ImageStore public API.
  - Removed chunk request/reply type aliases in src/msg/storage_node_messages.h.
- Result:
  - Source tree no longer has runtime WriteChunk/ReadChunk/DeleteChunk call paths; remaining chunk terms are mainly compatibility fields (chunk_id, chunk_size, ile_chunk_index) and docs/tasks.

- 2026-03-11: Continued de-chunk cleanup (MDS internal naming pass).
- Changes:
  - MdsServiceImpl removed obsolete used_chunk_fallback plumbing from BuildReadPlanWithPolicy and its call sites.
  - Renamed internal helper/function names to object semantics: ResolveExtentObjectIndex, SelectReadableDiskObjectReplica, HasReadyDiskObjectReplica, FindReadyOpticalObjectReplica.
  - Read/layout internal variables in BuildReadPlanFromLayout, ResolveExtents, and file-cache/owner-index paths migrated from chunk-centric names to object-unit/object-index naming.
  - Updated MDS runtime error text from chunk wording to object/layout wording for active read paths.
  - Removed deprecated layout_read_fallback_c config/read-option wiring (MdsConfig + mds_server + MdsServiceImpl state).
  - Real/virtual archive tracking internals renamed from rchive_tracking_max_chunks_ to rchive_tracking_max_objects_ (compat method alias kept).

- 2026-03-11: Continued de-chunk cleanup (B2/D2 compatibility shrink).
- Changes:
  - Removed unused chunk compatibility APIs from `ArchiveBatchStager` (`StageChunk/ReadChunkData/MarkChunkDone/RemoveChunk/ContainsChunk`, `StagedArchiveChunk` alias).
  - Migrated `OpticalArchiveManager` to object-first lease/stage calls (`UpdateObjectLease`) and replaced scattered `set_chunk_id + set_object_id` writes with object-id helper setters.
  - Migrated `MdsServiceImpl` disk-replica append paths (recall/file-cache) to object-first helper (`EnsureReplicaObjectId`) so chunk-id fallback is centralized.
  - `UpdateArchiveStateRequest` sender path now uses `object_id` only (no direct chunk-id writes in runtime call path).

- 2026-03-11: Continued de-chunk cleanup (B2/D2 runtime path narrowing).
- Changes:
  - Real/virtual archive candidate reports now fill `object_id` only (no direct `candidate.chunk_id` writes in server report loop).
  - `ArchiveCandidateQueue` stopped forcing `chunk_id` backfill when object id exists; queue internals renamed `is_new_object`.
  - `ArchiveBatchStager` manifest replay no longer populates `candidate.chunk_id`; uses `object_id` as single source.
  - Optical `ImageStore` internal metadata renamed object-first:
    - `ImageFileExtent.chunk_id` -> `object_id`
    - `ChunkRecord/chunks` -> `ObjectRecord/objects`
    - simulated payload seed label switched from `chunk-*` to `object-*`.

- 2026-03-11: Started hard cut toward object-only runtime path (breaking compatibility intentionally).
- Changes:
  - Removed `chunk_id` from internal object request/response structs in `src/msg/storage_node_messages.h`; `ArchiveObjectId()` now returns `object_id` directly.
  - Removed `chunk_id` fields from real/virtual/archive candidate in-memory structs (`StorageServiceImpl.h`, `VirtualStorageServiceImpl.h`, `ArchiveChunkMetaStore.h`, `ArchiveCandidateQueue.h`).
  - Removed object/chunk dual-read fallback from node brpc `UpdateArchiveState` handlers (real/virtual/optical now read only `request.object_id`).
  - MDS runtime helpers switched to object-only id resolution:
    - `MdsServiceImpl.cpp`: `ReplicaObjectId/ArchiveObjectId` no longer fallback to `chunk_id`.
    - `OpticalArchiveManager.cpp`: replica/lease request id helpers no longer set/read `chunk_id`.
    - `ArchiveLeaseManager.cpp`: lease key resolution/object validation is `object_id` only; removed legacy chunk-id fill logic.
  - Allocator stopped populating replica `chunk_id`; removed unused `AllocateChunk` compatibility API.
  - Client runtime helper switched to object-only replica id read (`zb_fuse_client.cpp`, `optical_archive_stress_test.cpp`).
  - Removed legacy `--chunk_id` CLI alias from `real_node_client` and `virtual_node_test`.

- 2026-03-11: Continued hard cut (protocol + naming migration deeper).
- Changes:
  - `mds.proto` migrated to object-first layout schema:
    - `ChunkMeta` -> `ObjectMeta`
    - `FileLayout.chunks/chunk_size` -> `objects/object_unit_size`
    - `InodeAttr.chunk_size` -> `object_unit_size`
    - `LayoutRoot.chunk_size` -> `object_unit_size`
    - `LayoutExtent.chunk_index` -> `object_index`
    - removed archive/replica `chunk_id` protocol fields (reserved old tags).
  - `real_node.proto` `UpdateArchiveStateRequest` removed `chunk_id` field (reserved old tag).
  - Runtime call sites updated to new proto accessors (`object_unit_size/objects/object_index/ObjectMeta`).
  - Removed `MetaCodec` chunk-named APIs (`EncodeChunkMeta/DecodeChunkMeta`), kept object-only encode/decode path.
  - Renamed core source files to object naming:
    - `src/mds/allocator/ChunkAllocator.*` -> `src/mds/allocator/ObjectAllocator.*`
    - `src/data_node/real_node/service/ArchiveChunkMetaStore.*` -> `.../ArchiveObjectMetaStore.*`
  - Updated includes/CMake references to renamed files.
- 2026-03-11: Continued de-chunk cleanup (protocol + naming hardening).
- Changes:
  - Renamed optical archive context field from `file_chunk_index` to `file_object_index` across proto, internal messages, optical image metadata encode/decode, replication requests, and smoke tests.
  - Renamed archive budget/limits to object-first naming:
    - real/virtual config field: `archive_track_max_objects`
    - mds config field: `archive_max_objects_per_round`
    - archive manager option: `max_objects_per_round`
    - kept legacy config keys (`*_CHUNKS_*`) accepted by parsers for compatibility.
  - Client/tooling naming cleanup:
    - `zb_fuse_client`: `default_object_unit_size` and object-first local vars.
    - `optical_archive_stress_test`: object-first flags/counters/messages.
    - `layout_consistency_check` and migration tool internals switched to object naming.
  - Migration tool target/file renamed to remove chunk wording:
    - `src/mds/tools/migrate_chunk_to_layout.cpp` -> `src/mds/tools/migrate_legacy_to_layout.cpp`
    - CMake target renamed to `migrate_legacy_to_layout`.
  - Removed leftover compatibility API names (`SetArchiveTrackingMaxChunks`) and residual local variable names (`full_chunk_overwrite`, `chunk_key`).
- Verification:
  - `rg -n "chunk" src tests CMakeLists.txt config scripts --glob '!**/*.md'` returns no matches.
- 2026-03-11: Pure-PG hard cut completed for runtime paths.
- Changes:
  - MDS removed legacy/cutover runtime branches (`from_legacy`, `LC/*`, `legacy:*` fallback) and now requires concrete `LR/LO` roots.
  - File create path now initializes empty layout root/object (`LR/<inode>`, `LO/<layout_root_id>`) at creation time.
  - MDS config/server are PG-only: removed `ENABLE_PG_LAYOUT`, `LAYOUT_MODE`, and layout-only mode switches; PG placement is always used.
  - Object allocator is PG-only (`AllocateObject` always resolves via PGManager, no node-cache fallback).
  - GC removed legacy-key cleanup branch and now only handles layout-object reachability GC.
  - Removed `migrate_legacy_to_layout` build target and deleted legacy migration source file.
  - Protocol cleanup: removed `from_legacy` fields from `GetLayoutRootReply`/`ResolveLayoutReply`; read-plan source value `2` kept as reserved.
  - FUSE client adapted to new `GetLayoutRoot` signature without legacy flag.
- Verification:
  - `rg -n "chunk|CHUNK" src tests config scripts CMakeLists.txt --glob '!**/*.md'` => no matches.
  - `rg -n "legacy:|from_legacy|LayoutCutover|cutover|ENABLE_PG_LAYOUT|LAYOUT_MODE|CHUNK_SIZE|ARCHIVE_MAX_CHUNKS_PER_ROUND|ARCHIVE_TRACK_MAX_CHUNKS" src tests config scripts CMakeLists.txt --glob '!**/*.md'` => no matches.
- 2026-03-11: Final residue cleanup done.
- Changes:
  - Renamed object store source files:
    - `src/data_node/common/RealChunkObjectStore.cpp` -> `src/data_node/common/RealObjectStore.cpp`
    - `src/data_node/common/VirtualChunkObjectStore.cpp` -> `src/data_node/common/VirtualObjectStore.cpp`
  - Updated CMake source list to new object store filenames.
  - Removed compatibility type aliases `RealChunkObjectStore` / `VirtualChunkObjectStore` from `ObjectStore.h`.
  - `MetaSchema` object keyspace renamed from `C/` and `RC/` to `O/` and `RO/`.
  - Rewrote core module READMEs (`src/mds`, `src/data_node/real_node`, `src/data_node/virtual_node`, `src/data_node/optical_node`) to object/PG terminology.
- Verification:
  - `rg -n "chunk|Chunk|CHUNK|RealChunkObjectStore|VirtualChunkObjectStore" src tests config scripts CMakeLists.txt --glob '!**/*.md'` => no matches.

- 2026-03-11: Continued hard cleanup on MDS write-path API (legacy layout commit RPC removal).
- Changes:
  - Removed legacy MDS RPCs from protocol/service surface:
    - `AllocateWrite`, `CommitWrite`, `GetLayoutRoot`, `ResolveLayout`, `CommitLayoutRoot`.
  - Removed related protobuf message types from `src/msg/mds.proto`.
  - Deleted corresponding handlers and obsolete pending-write commit pipeline in `MdsServiceImpl`:
    - removed pending transaction state/mutex/counters and helper functions that only served removed RPCs.
  - Kept active runtime path focused on:
    - `GetFileAnchor` + `GetLayout` for MDS side read planning.
    - node-side `GetFileMeta/CommitFileMeta/DeleteFileMeta` for file metadata updates.

- 2026-03-11: Continued sequential cleanup (MDS layout residual purge + write-tx durability hardening).
- Changes:
  - Removed unused MDS layout fallback helpers and dead APIs:
    - deleted `LoadLayoutRoot/BuildLayoutNodeFromObjects/LoadHealthyLayoutNode/StoreLayoutNodeWithReplicas/ValidateLayoutObjectOnLoad`.
    - deleted `BuildReadPlan*/ResolveExtents/SelectReadableDiskObjectReplica` dead wrappers.
    - removed obsolete `SeedObjectForCowWrite`.
    - removed now-unused layout-object option plumbing (`SetLayoutObjectOptions`, config parse, server wiring).
  - Strengthened node-side file-write idempotency persistence (real + virtual):
    - added `CommitFileMetaInternal(..., txid)` path used by `CommitFileWrite`.
    - persisted per-inode last successful `txid` in `file_meta.tsv` (new optional column).
    - retrying same `txid` now returns previous committed meta instead of version mismatch.
    - `DeleteFileMeta` now cleans associated persisted txid state.

- 2026-03-11: Continued API surface cleanup (remove unused file-meta RPCs).
- Changes:
  - Removed unused protocol RPCs from `real_node.proto`:
    - `GetFileMeta`
    - `CommitFileMeta`
  - Removed corresponding brpc handlers from real/virtual/optical services.
  - Removed public service impl methods `GetFileMeta` / `CommitFileMeta`; write commit now only uses internal `CommitFileMetaInternal` via `CommitFileWrite`.
  - Removed unused internal message structs `GetFileMetaRequest/GetFileMetaReply` from `storage_node_messages.h`.

- 2026-03-11: Continued naming convergence (file-meta internal apply path).
- Changes:
  - Renamed internal file-meta commit messages in `storage_node_messages.h`:
    - `CommitFileMetaRequest` -> `ApplyFileMetaRequest`
    - `CommitFileMetaReply` -> `ApplyFileMetaReply`
  - Renamed real/virtual internal helper:
    - `CommitFileMetaInternal` -> `ApplyFileMetaInternal`
  - Updated `CommitFileWrite` internals to use `ApplyFileMeta*` request/reply structures.

- 2026-03-11: Continued client naming cleanup (remove fallback wording).
- Changes:
  - In `zb_fuse_client.cpp`, renamed cached metadata helper and variables from `fallback_*` to `hint_*`:
    - `GetCachedInodeMetaFallback` -> `GetCachedInodeMetaHint`
    - `fallback_file_size/object_unit_size` -> `hint_file_size/object_unit_size`
  - Semantics unchanged: still uses local cached inode metadata as read/write planning hints.

- 2026-03-11: Continued metadata simplification on MDS service path.
- Changes:
  - Simplified `DeleteInodeData` in `MdsServiceImpl`:
    - no longer decodes/rewrites object/layout records to chase reverse indexes during unlink.
    - now only performs lightweight inode-scoped key cleanup (`FA/LR/LOP` and direct `O/<inode>/...` keys).
  - Removed dead MDS recall/cache implementation that depended on full object metadata in MDS:
    - dropped `RecallTask` and related helper functions (`CollectRecallTasksByImage`, `RecallTasksToDisk`, `CacheWholeFileToDisk`, etc.).
    - removed unused `GenerateObjectId`, `ReadObjectFromReplica`, `WriteObjectToReplica` in `MdsServiceImpl`.
  - Removed layout GC runtime integration from MDS startup/config:
    - deleted `ENABLE_LAYOUT_GC` and `LAYOUT_GC_*` config parsing/fields.
    - removed `GcManager` thread wiring in `mds_server.cpp`.

- 2026-03-11: Continued metadata simplification (unlink cleanup fully delegated to anchor node).
- Changes:
  - Extended `DeleteFileMetaRequest` (proto + internal message) with:
    - `disk_id`
    - `purge_objects`
  - MDS unlink path no longer issues per-object delete RPC loops.
    - `MdsServiceImpl::Unlink` now sends one `DeleteFileMeta(purge_objects=true)` to file anchor.
  - Real node `DeleteFileMeta` now supports node-side purge:
    - computes object id range from local file meta (`inode_id/file_size/object_unit_size`),
    - deletes local objects first, then removes file meta record.
  - Virtual node `DeleteFileMeta` now supports node-side purge with in-memory object map cleanup and usage/accounting updates.
  - Updated brpc adapters (real/virtual) to map new request fields.

- 2026-03-11: Continued metadata-path performance optimization (reduced MDS round trips on open/create).
- Changes:
  - Extended MDS replies:
    - `OpenReply` now carries `file_anchor`.
    - `CreateReply` now carries `file_anchor`.
  - MDS `Open` now returns file anchor (load existing anchor or lazily select/persist one).
  - FUSE client now consumes anchor from `Open/Create` replies and updates local cache directly.
  - `FuseOpen/FuseCreate` only fallback to `GetFileAnchor` when reply anchor is missing, reducing one common-path MDS RPC.

- 2026-03-11: Continued read-plan simplification (MDS delegates layout slicing to anchor node).
- Changes:
  - `MdsServiceImpl::BuildReadPlanFromAnchor` no longer infers object ranges from MDS inode size.
  - `GetLayout` path now calls anchor node `ResolveFileRead(inode, offset, size, disk_id)` and builds `FileLayout` from returned slices.
  - Effect: read-plan generation uses node-side file metadata as source of truth, reducing MDS-side detailed metadata dependence.

- 2026-03-11: Removed dead optical-plan branch in MDS `GetLayout`.
- Changes:
  - Deleted `BuildOpticalReadPlan` helper and related branch logic in `MdsServiceImpl::GetLayout`.
  - `GetLayout` now has a single path: anchor-node `ResolveFileRead` -> synthesize `FileLayout`.
  - `optical_plan` field is now explicitly cleared in reply to avoid stale payload.

- 2026-03-11: Reduced archive online dependence on reverse indexes (`RO/OO`) in owner resolution path.
- Changes:
  - `OpticalArchiveManager` now first parses stable object id format `obj-<inode>-<index>`:
    - `ResolveObjectOwner` prefers direct parse, then optional backfill `OO/<object_id>`.
    - `FindObjectKeyByObjectId` prefers direct `O/<inode>/<index>` existence check before `RO/<object_id>` fallback.
  - Effect: common-path archive scheduling/lookup no longer requires reverse-index hit for stable ids.

- 2026-03-11: Further simplified unlink metadata cleanup.
- Changes:
  - `DeleteInodeData` now only removes file-anchor key (`FA/<inode>`).
  - No inline cleanup of `LR/LO/O/RO/LCO` in unlink path; aligns with anchor-only online metadata model.

- 2026-03-11: Removed reverse-index repair/fallback pipeline from optical archive manager.
- Changes:
  - Deleted `ProcessReverseObjectRepairTasks` / `EnqueueReverseObjectRepair`.
  - Removed `ReconcileArchiveStates` tail call to reverse-index repair worker.
  - `ResolveObjectOwner` now:
    - accepts stable id parse (`obj-<inode>-<index>`), or
    - reads existing `OO/<object_id>`, otherwise fail fast (no `RO` fallback).
  - `FindObjectKeyByObjectId` now:
    - accepts stable id -> direct `O/<inode>/<index>` lookup,
    - otherwise fail fast (no `RO` lookup / no repair queueing).
  - Effect: archive online path no longer depends on `RO/*` keyspace or repair scans.

- 2026-03-11: Stopped maintaining reverse-object index in online archive path.
- Changes:
  - Removed `ReverseObjectKey(object_id)` writes from:
    - `PersistOpticalReplica`
    - `ArchiveByCandidate`
  - Removed dead reverse-repair key helpers from `MetaSchema`:
    - `ArchiveReverseObjectRepairKey`
    - `ArchiveReverseRepairPrefix`
    - `ArchiveReverseObjectRepairPrefix`
  - Result: `RO/*` is now offline-tool-only legacy surface (no online producer/consumer in archive runtime).

- 2026-03-11: Cleaned build surface for removed layout-GC runtime path.
- Changes:
  - Removed `src/mds/gc/GcManager.cpp` from `mds_server` target sources.
  - Removed `layout_gc_probe` executable target from `CMakeLists.txt`.
  - Runtime/control-plane build no longer links layout-GC implementation.

- 2026-03-11: Removed remaining `RO` code references and checks.
- Changes:
  - Dropped `ReverseObjectKey` from `MetaSchema`.
  - `layout_consistency_check` no longer exposes `--require_reverse_index` and no longer validates reverse-index keys.
  - Result: source tree no longer has active `RO/*` key usage; reverse-index path is fully retired.

- 2026-03-11: Deleted retired layout-GC source files.
- Changes:
  - Removed:
    - `src/mds/gc/GcManager.h`
    - `src/mds/gc/GcManager.cpp`
    - `src/mds/tools/layout_gc_probe.cpp`
  - `rg -n "GcManager|layout_gc_probe"` now has no source references.

- 2026-03-11: Removed legacy layout consistency checker tool.
- Changes:
  - Removed `layout_consistency_check` target from `CMakeLists.txt`.
  - Deleted `src/mds/tools/layout_consistency_check.cpp`.
  - Build/test surface no longer includes LR/LO consistency probing toolchain.

- 2026-03-11: Removed unused layout-key helpers from `MetaSchema`.
- Changes:
  - Deleted unused key helpers:
    - `LayoutRootKey/LayoutRootPrefix`
    - `LayoutCommitOpKey/LayoutCommitOpPrefix`
    - `LayoutObjectKey/LayoutObjectPrefix`
    - `LayoutObjectReplicaKey/LayoutObjectReplicaPrefix/LayoutObjectReplicaGlobalPrefix`
    - `LayoutGcSeenKey/LayoutGcSeenPrefix`
  - Deleted unused parse helpers:
    - `ParseLayoutRootKey`
    - `ParseLayoutObjectReplicaKey`
  - Result: schema helpers now focus on active anchor/object/PG keyspaces.

- 2026-03-11: Continued cleanup (metadata surface reduction).
- Changes:
  - Removed unused MDS GetLayout reply fields: optical_plan / plan_source and related enum/message definitions in src/msg/mds.proto.
  - Removed dead MDS implementation writes for removed fields in MdsServiceImpl::GetLayout.
  - Removed legacy text WAL replay compatibility path from ArchiveObjectMetaStore; WAL is now strict magic+len+crc binary only.
  - Removed unused MetaCodec layout-tree encode/decode surface (LayoutRoot/LayoutNode/LayoutExtent records and codecs).

- 2026-03-11: ArchiveObjectMetaStore WAL path tightened further.
- Changes:
  - Removed now-dead wal_has_magic_ state; WAL parser/writer always enforces magic-header format.
  - Keep behavior: incompatible WAL is truncated, valid WAL replays with tail truncation on corruption.

- 2026-03-11: Continued cleanup (removed legacy MDS read-plan RPC surface).
- Changes:
  - Removed GetLayout RPC and related FileLayout/GetLayoutRequest/GetLayoutReply messages from src/msg/mds.proto.
  - Removed MdsServiceImpl::GetLayout and internal BuildReadPlanFromAnchor/FillAnchorReplica helpers.
  - Updated optical_archive_stress_test to sample resolve-read slices via anchor (GetFileAnchor + ResolveFileRead) instead of GetLayout.
  - Marked optical-observation flags as deprecated/ignored in simplified metadata mode.

