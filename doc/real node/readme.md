
测试多个real node的功能
./build/real_node_multi_test \
  --servers=127.0.0.1:19080,127.0.0.1:19081,127.0.0.1:19082 \
  --disks=disk-01,disk-02,disk-03 \
  --verify_fs=true \
  --config_files=config/real_node_mp1.conf,config/real_node_mp2.conf,config/real_node_mp3.conf
