归档跨系统仍非原子，存在“部分成功”窗口（高）
OpticalArchiveManager.cpp:1062 先写光盘，
OpticalArchiveManager.cpp:1109 再写 MDS 元数据，
OpticalArchiveManager.cpp:1132 最后 commit lease。
后两步任一步失败，都会产生状态漂移（光盘已写但元数据/lease未完全同步）。

归档路径仍有全量扫描，规模上去后会明显拖慢（高）
OpticalArchiveManager.cpp:196
OpticalArchiveManager.cpp:577
OpticalArchiveManager.cpp:1426
这些都在用迭代器扫前缀/状态集合，万亿级元数据场景会成为瓶颈。

复制写入仍是“本地先成功、复制失败就报错”，副本可能短时不一致（高）
真实节点：StorageServiceImpl.cpp:151、StorageServiceImpl.cpp:171
虚拟节点：VirtualStorageServiceImpl.cpp:166、VirtualStorageServiceImpl.cpp:198

路径放置策略“非 strict”时实际上没生效为“偏好 real”，而是退回普通分配（中）
MdsServiceImpl.cpp:435 只有 strict 才走 real-only，
MdsServiceImpl.cpp:1395 非 strict 直接走常规路径，没有 real 偏好逻辑。

大规模元数据生成后，带磁盘副本的文件“元数据存在但对象数据不存在”，读可能失败（中）
生成器仅写 file_meta 记录：disk_meta_generator.cpp:157
真实节点读对象依赖实际对象文件：StorageServiceImpl.cpp:204

光盘 manifest 记录缺少 node_id，跨节点重建时定位有歧义（中）
optical_meta_generator.cpp:163 使用 image_id=img-odiskX，
optical_meta_generator.cpp:169 仅写 disc_id，未写 optical node_id。

FUSE 热路径对 MDS 调用仍偏多，影响你要测的 MDS 极限性能（中）
每次读/写/truncate 都会取 anchor set：
zb_fuse_client.cpp:1663、zb_fuse_client.cpp:1795、zb_fuse_client.cpp:1906

构建/运维层面还有可用性问题（低）
当前目录下构建目标直接失败（mds_server.vcxproj 不存在，构建目录与生成器不匹配）；
脚本也主要是 Linux Bash 流程：run_meta_gen_pipeline.sh:1、run_meta_gen_pipeline.sh:111。