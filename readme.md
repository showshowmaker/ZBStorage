NAMESPACE_PREFIX=test-ns scripts/import_masstree_demo.sh 2
scripts/run_system_demo.sh

<!-- scripts/start_demo_stack.sh start -->
RUN_DIR=/mnt/md0/wjh/zb_data MOUNT_POINT=/mnt/md0/wjh/mp bash scripts/start_demo_stack.sh start
RUN_DIR=/mnt/md0/wjh/zb_data bash scripts/start_demo_stack.sh stop
NAMESPACE_PREFIX=test-ns scripts/import_masstree_demo.sh 2
scripts/run_system_demo.sh


scripts/build_all.sh build
JOBS=16 BUILD_TYPE=Debug scripts/build_all.sh reconfigure
