#pragma once

#include <brpc/channel.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../allocator/ObjectAllocator.h"
#include "../archive/ArchiveCandidateQueue.h"
#include "../archive/ArchiveLeaseManager.h"
#include "../storage/RocksMetaStore.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "mds.pb.h"

namespace zb::mds {

class MdsServiceImpl : public zb::rpc::MdsService {
public:
    struct LayoutObjectOptions {
        uint32_t replica_count{3};
        bool scrub_on_load{true};
    };

    MdsServiceImpl(RocksMetaStore* store,
                   ObjectAllocator* allocator,
                   uint64_t default_object_unit_size,
                   ArchiveCandidateQueue* candidate_queue = nullptr,
                   ArchiveLeaseManager* lease_manager = nullptr);

    void SetLayoutObjectOptions(LayoutObjectOptions options);
    void SetSimplifiedAnchorMetadataMode(bool enabled);

    void Lookup(google::protobuf::RpcController* cntl_base,
                const zb::rpc::LookupRequest* request,
                zb::rpc::LookupReply* response,
                google::protobuf::Closure* done) override;

    void Getattr(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::GetattrRequest* request,
                 zb::rpc::GetattrReply* response,
                 google::protobuf::Closure* done) override;

    void Open(google::protobuf::RpcController* cntl_base,
              const zb::rpc::OpenRequest* request,
              zb::rpc::OpenReply* response,
              google::protobuf::Closure* done) override;

    void Close(google::protobuf::RpcController* cntl_base,
               const zb::rpc::CloseRequest* request,
               zb::rpc::CloseReply* response,
               google::protobuf::Closure* done) override;

    void Create(google::protobuf::RpcController* cntl_base,
                const zb::rpc::CreateRequest* request,
                zb::rpc::CreateReply* response,
                google::protobuf::Closure* done) override;

    void Mkdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::MkdirRequest* request,
               zb::rpc::MkdirReply* response,
               google::protobuf::Closure* done) override;

    void Readdir(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::ReaddirRequest* request,
                 zb::rpc::ReaddirReply* response,
                 google::protobuf::Closure* done) override;

    void Rename(google::protobuf::RpcController* cntl_base,
                const zb::rpc::RenameRequest* request,
                zb::rpc::RenameReply* response,
                google::protobuf::Closure* done) override;

    void Unlink(google::protobuf::RpcController* cntl_base,
                const zb::rpc::UnlinkRequest* request,
                zb::rpc::UnlinkReply* response,
                google::protobuf::Closure* done) override;

    void Rmdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::RmdirRequest* request,
               zb::rpc::RmdirReply* response,
               google::protobuf::Closure* done) override;

    void AllocateWrite(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::AllocateWriteRequest* request,
                       zb::rpc::AllocateWriteReply* response,
                       google::protobuf::Closure* done) override;

    void GetLayout(google::protobuf::RpcController* cntl_base,
                   const zb::rpc::GetLayoutRequest* request,
                   zb::rpc::GetLayoutReply* response,
                   google::protobuf::Closure* done) override;

    void CommitWrite(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::CommitWriteRequest* request,
                     zb::rpc::CommitWriteReply* response,
                     google::protobuf::Closure* done) override;
    void GetLayoutRoot(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::GetLayoutRootRequest* request,
                       zb::rpc::GetLayoutRootReply* response,
                       google::protobuf::Closure* done) override;
    void ResolveLayout(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::ResolveLayoutRequest* request,
                       zb::rpc::ResolveLayoutReply* response,
                       google::protobuf::Closure* done) override;
    void CommitLayoutRoot(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::CommitLayoutRootRequest* request,
                          zb::rpc::CommitLayoutRootReply* response,
                          google::protobuf::Closure* done) override;

    void ReportNodeStatus(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReportNodeStatusRequest* request,
                          zb::rpc::ReportNodeStatusReply* response,
                          google::protobuf::Closure* done) override;
    void ReportArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::ReportArchiveCandidatesRequest* request,
                                 zb::rpc::ReportArchiveCandidatesReply* response,
                                 google::protobuf::Closure* done) override;
    void ClaimArchiveTask(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ClaimArchiveTaskRequest* request,
                          zb::rpc::ClaimArchiveTaskReply* response,
                          google::protobuf::Closure* done) override;
    void RenewArchiveLease(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::RenewArchiveLeaseRequest* request,
                           zb::rpc::RenewArchiveLeaseReply* response,
                           google::protobuf::Closure* done) override;
    void CommitArchiveTask(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::CommitArchiveTaskRequest* request,
                           zb::rpc::CommitArchiveTaskReply* response,
                           google::protobuf::Closure* done) override;

private:
    struct RecallTask {
        std::string object_key;
        zb::rpc::ReplicaLocation optical_replica;
    };
    struct PendingWriteTransaction {
        uint64_t inode_id{0};
        std::string op_id;
        uint64_t base_layout_version{0};
        uint64_t pending_layout_version{0};
        uint64_t create_ts_ms{0};
        LayoutNodeRecord pending_layout_node;
    };

    bool EnsureRoot(std::string* error);
    bool ResolvePath(const std::string& path, uint64_t* inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool ResolveParent(const std::string& path, uint64_t* parent_inode, std::string* name, std::string* error);
    bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error);
    bool PutDentry(uint64_t parent_inode, const std::string& name, uint64_t inode_id, std::string* error);
    bool DeleteDentry(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DentryExists(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DeleteInodeData(uint64_t inode_id, std::string* error);
    bool LoadLayoutRoot(uint64_t inode_id,
                        LayoutRootRecord* root,
                        std::string* error);
    bool StoreLayoutRootAtomic(uint64_t inode_id,
                               const LayoutRootRecord& root,
                               uint64_t expected_layout_version,
                               bool update_inode_size,
                               uint64_t new_size,
                               const std::string* commit_op_id,
                               const LayoutNodeRecord* layout_node_override,
                               LayoutRootRecord* committed,
                               std::string* error);
    bool LoadCommittedLayoutRootByOpId(uint64_t inode_id,
                                       const std::string& op_id,
                                       LayoutRootRecord* committed,
                                       std::string* error);
    bool BuildLayoutNodeFromObjects(uint64_t inode_id,
                                    const std::string& layout_obj_id,
                                    uint64_t object_version,
                                    LayoutNodeRecord* node,
                                    std::string* error);
    bool LoadHealthyLayoutNode(uint64_t inode_id,
                               const std::string& layout_obj_id,
                               uint64_t object_version,
                               LayoutNodeRecord* node,
                               bool* recovered,
                               std::string* error);
    bool StoreLayoutNodeWithReplicas(const std::string& layout_obj_id,
                                     const LayoutNodeRecord& node,
                                     rocksdb::WriteBatch* batch,
                                     std::string* error);
    bool ValidateLayoutObjectOnLoad(uint64_t inode_id,
                                    const LayoutRootRecord& root,
                                    std::string* error);
    bool BuildReadPlan(uint64_t inode_id,
                       uint64_t offset,
                       uint64_t size,
                       zb::rpc::FileLayout* layout,
                       std::string* error);
    bool BuildReadPlanFromLayout(uint64_t inode_id,
                                 uint64_t offset,
                                 uint64_t size,
                                 zb::rpc::FileLayout* layout,
                                 std::string* error);
    bool BuildReadPlanWithPolicy(uint64_t inode_id,
                                 uint64_t offset,
                                 uint64_t size,
                                 zb::rpc::FileLayout* layout,
                                 std::string* error);
    bool BuildOpticalReadPlan(uint64_t inode_id,
                              uint64_t offset,
                              uint64_t size,
                              zb::rpc::OpticalReadPlan* plan,
                              bool* optical_only,
                              std::string* error);
    bool ResolveExtents(const zb::rpc::FileLayout& read_plan,
                        uint64_t request_offset,
                        uint64_t request_size,
                        uint64_t object_version,
                        std::vector<LayoutExtentRecord>* extents,
                        std::string* error);
    bool SelectReadableDiskObjectReplica(const zb::rpc::ObjectMeta& object_meta,
                                         zb::rpc::ReplicaLocation* source) const;
    bool ResolveObjectReplicas(uint32_t replica_count,
                               const std::string& object_id,
                               uint64_t placement_epoch,
                               std::vector<zb::rpc::ReplicaLocation>* replicas,
                               std::string* error) const;
    bool SelectFileAnchor(uint64_t inode_id,
                          const zb::rpc::InodeAttr& attr,
                          zb::rpc::ReplicaLocation* anchor,
                          std::string* error) const;
    bool LoadFileAnchor(uint64_t inode_id, zb::rpc::ReplicaLocation* anchor, std::string* error) const;
    bool SaveFileAnchor(uint64_t inode_id, const zb::rpc::ReplicaLocation& anchor, rocksdb::WriteBatch* batch) const;
    bool BuildReadPlanFromAnchor(uint64_t inode_id,
                                 uint64_t offset,
                                 uint64_t size,
                                 zb::rpc::FileLayout* layout,
                                 std::string* error);
    static std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index);
    static void FillAnchorReplica(zb::rpc::ReplicaLocation* replica,
                                  const zb::rpc::ReplicaLocation& anchor,
                                  const std::string& object_id);
    bool SeedObjectForCowWrite(const LayoutExtentRecord& old_extent,
                               const std::vector<zb::rpc::ReplicaLocation>& new_replicas,
                               uint64_t object_size,
                               std::string* error);
    bool ConsumePendingWriteForCommit(uint64_t inode_id,
                                      const std::string& op_id,
                                      uint64_t expected_base_layout_version,
                                      uint64_t new_file_size,
                                      LayoutRootRecord* committed_root,
                                      std::string* error);
    bool HasReadyDiskObjectReplica(const zb::rpc::ObjectMeta& object_meta) const;
    bool FindReadyOpticalObjectReplica(const zb::rpc::ObjectMeta& object_meta,
                                       zb::rpc::ReplicaLocation* optical) const;
    bool CollectRecallTasksByImage(const std::string& seed_object_key,
                                   const zb::rpc::ObjectMeta& seed_meta,
                                   const zb::rpc::ReplicaLocation& optical_seed,
                                   std::vector<RecallTask>* tasks,
                                   std::string* error);
    bool RecallTasksToDisk(const std::vector<RecallTask>& tasks, std::string* error);
    bool CacheWholeFileToDisk(uint64_t inode_id, std::string* error);
    bool EnsureObjectReadableFromDisk(const std::string& object_key,
                                      zb::rpc::ObjectMeta* object_meta,
                                      std::unordered_set<std::string>* recalled_images,
                                      bool* recalled_from_optical,
                                      std::string* error);
    bool ReadObjectFromReplica(const zb::rpc::ReplicaLocation& source,
                               uint64_t read_size,
                               std::string* data,
                               std::string* error);
    bool WriteObjectToReplica(const zb::rpc::ReplicaLocation& target,
                              const std::string& object_id,
                              const std::string& data,
                              std::string* error);
    brpc::Channel* GetDataChannel(const std::string& address, std::string* error);

    uint64_t AllocateInodeId(std::string* error);
    uint64_t AllocateHandleId(std::string* error);
    static std::string GenerateObjectId();
    static uint64_t NowSeconds();
    static uint64_t NowMilliseconds();
    static void FillStatus(zb::rpc::MdsStatus* status, zb::rpc::MdsStatusCode code, const std::string& message);

    RocksMetaStore* store_{};
    ObjectAllocator* allocator_{};
    uint64_t default_object_unit_size_{0};
    ArchiveCandidateQueue* candidate_queue_{};
    ArchiveLeaseManager* lease_manager_{};
    mutable std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
    mutable std::mutex pending_write_mu_;
    std::unordered_map<uint64_t, PendingWriteTransaction> pending_writes_;
    mutable std::mutex layout_object_mu_;
    LayoutObjectOptions layout_object_options_;
    bool simplified_anchor_metadata_mode_{true};
    std::atomic<uint64_t> layout_read_hit_total_{0};
    std::atomic<uint64_t> layout_commit_conflict_total_{0};
    std::atomic<uint64_t> layout_commit_retry_total_{0};
    std::atomic<uint64_t> pg_resolve_fail_total_{0};
};

} // namespace zb::mds
