# Virtual node simulator

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

## Run one virtual node pod/process

```bash
./build/virtual_node_server --config=config/virtual_node.conf.example --port=29080
```

## Quick simulator test

```bash
./build/virtual_node_test --server=127.0.0.1:29080 --disk=disk-01 --write_size=4096 --read_size=4096
```

## MDS config example (one endpoint simulates 100000 virtual nodes)

```ini
MDS_DB_PATH=/tmp/zb_mds_rocks
CHUNK_SIZE=4194304
REPLICA=2
NODES=vpool@127.0.0.1:29080,type=VIRTUAL,weight=10,virtual_node_count=100000;node-01@127.0.0.1:19080,type=REAL,weight=1
DISKS=vpool:disk-01,disk-02,disk-03;node-01:disk-01,disk-02,disk-03
```

`vpool` expands to `vpool-v0 ... vpool-v99999` logically in MDS allocation, but all traffic goes to `127.0.0.1:29080`.
