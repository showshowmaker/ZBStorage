1.默认不启动主从相关的配置。
2.关于真实节点部分，做如下更改：
启动一个磁盘节点时，通过两个参数来配置磁盘个数，一个参数是路径目录，一个参数是磁盘个数，比如比如给了一个路径/a/b/c和一个个数4，就在这个路径下创建4个文件夹disk0、disk1、disk2、disk3，用这四个文件夹来模拟4个磁盘，而不再关注挂载之类的细节。
3.现在按照这些要求来修改节点的启动相关的配置，只启动一个真实节点，有24个盘，每个盘的大小为2TB。虚拟节点配置和真实节点一个，有100个虚拟节点。启动10000个光盘节点，每个光盘节点包含10000个光盘，每个光盘的大小为1TB。


cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
RUN_DIR=/mnt/md0/wjh/zb_test_dir bash scripts/run_full_e2e_suite.sh
RUN_DIR=runtime/e2e_run_01       bash scripts/run_full_e2e_suite.sh
ls -lt /mnt/md0/wjh/zb_test_dir/report
cat /mnt/md0/wjh/zb_test_dir/report/e2e_report_*.md
