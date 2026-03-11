# optical_node module

## Role
`optical_node` manages optical discs and disc images for archive workloads.
It provides disc-level archive/read operations and simulates optical media timing.

## Core responsibilities
- Manage disc inventory and usage status.
- Persist and read disc image metadata.
- Simulate burn and load latency based on configured bandwidth.
- Serve archive read/write RPCs used by MDS archive workflows.

## Run
```bash
./build/optical_node_server --config=config/optical_node.conf.example --port=39080
```
