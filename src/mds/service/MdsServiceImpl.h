#pragma once

#include <brpc/channel.h>

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
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
    MdsServiceImpl(RocksMetaStore* store,
                   ObjectAllocator* allocator,
                   uint64_t default_object_unit_size,
                   ArchiveCandidateQueue* candidate_queue = nullptr,
                   ArchiveLeaseManager* lease_manager = nullptr);

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
    void GetFileAnchor(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::GetFileAnchorRequest* request,
                       zb::rpc::GetFileAnchorReply* response,
                       google::protobuf::Closure* done) override;
    void UpdateInodeStat(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::UpdateInodeStatRequest* request,
                         zb::rpc::UpdateInodeStatReply* response,
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
    void SetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::SetPathPlacementPolicyRequest* request,
                                zb::rpc::SetPathPlacementPolicyReply* response,
                                google::protobuf::Closure* done) override;
    void DeletePathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::DeletePathPlacementPolicyRequest* request,
                                   zb::rpc::DeletePathPlacementPolicyReply* response,
                                   google::protobuf::Closure* done) override;
    void GetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::GetPathPlacementPolicyRequest* request,
                                zb::rpc::GetPathPlacementPolicyReply* response,
                                google::protobuf::Closure* done) override;

private:
    bool EnsureRoot(std::string* error);
    bool ResolvePath(const std::string& path, uint64_t* inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool ResolveParent(const std::string& path, uint64_t* parent_inode, std::string* name, std::string* error);
    bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error);
    bool PutDentry(uint64_t parent_inode, const std::string& name, uint64_t inode_id, std::string* error);
    bool DeleteDentry(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DentryExists(uint64_t parent_inode, const std::string& name, std::string* error);
    bool DeleteInodeData(uint64_t inode_id, std::string* error);
    bool ResolveObjectReplicas(uint32_t replica_count,
                               const std::string& object_id,
                               uint64_t placement_epoch,
                               std::vector<zb::rpc::ReplicaLocation>* replicas,
                               std::string* error) const;
    bool SelectFileAnchor(uint64_t inode_id,
                          const zb::rpc::InodeAttr& attr,
                          zb::rpc::ReplicaLocation* anchor,
                          std::string* error) const;
    bool SelectFileAnchorWithPreference(uint64_t inode_id,
                                        const zb::rpc::InodeAttr& attr,
                                        NodeType preferred_type,
                                        bool strict_type,
                                        zb::rpc::ReplicaLocation* anchor,
                                        std::string* error) const;
    bool MatchPathPlacementPolicy(const std::string& path,
                                  zb::rpc::PathPlacementPolicyRecord* policy,
                                  std::string* matched_prefix,
                                  std::string* error) const;
    bool SavePathPlacementPolicy(const zb::rpc::PathPlacementPolicyRecord& policy, std::string* error);
    bool DeletePathPlacementPolicyByPrefix(const std::string& path_prefix, std::string* error);
    bool LoadFileAnchorSet(uint64_t inode_id, zb::rpc::FileAnchorSet* anchors, std::string* error) const;
    bool SaveFileAnchorSet(uint64_t inode_id, const zb::rpc::FileAnchorSet& anchors, rocksdb::WriteBatch* batch) const;
    static zb::rpc::FileAnchorSet BuildFileAnchorSetFromSingle(const zb::rpc::ReplicaLocation& anchor);
    static bool SelectPrimaryAnchor(const zb::rpc::FileAnchorSet& anchors, zb::rpc::ReplicaLocation* anchor);
    static bool SelectDiskAnchor(const zb::rpc::FileAnchorSet& anchors, zb::rpc::ReplicaLocation* anchor);
    bool LoadFileAnchor(uint64_t inode_id, zb::rpc::ReplicaLocation* anchor, std::string* error) const;
    bool SaveFileAnchor(uint64_t inode_id, const zb::rpc::ReplicaLocation& anchor, rocksdb::WriteBatch* batch) const;
    static std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index);
    static void StripReplicaAddresses(zb::rpc::ReplicaLocation* replica);
    bool ResolveNodeAddress(const std::string& node_id, std::string* address, std::string* error) const;
    bool DeleteFileMetaOnAnchor(const zb::rpc::ReplicaLocation& anchor,
                                uint64_t inode_id,
                                bool purge_objects,
                                std::string* error);
    brpc::Channel* GetDataChannel(const std::string& address, std::string* error);

    uint64_t AllocateInodeId(std::string* error);
    uint64_t AllocateHandleId(std::string* error);
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
};

} // namespace zb::mds
