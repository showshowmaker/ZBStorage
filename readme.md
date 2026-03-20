scripts/import_masstree_demo.sh <file_count> [namespace_id]
scripts/run_system_demo.sh

scripts/start_demo_stack.sh start
scripts/import_masstree_demo.sh 100000
scripts/run_system_demo.sh


scripts/build_all.sh build
JOBS=16 BUILD_TYPE=Debug scripts/build_all.sh reconfigure
