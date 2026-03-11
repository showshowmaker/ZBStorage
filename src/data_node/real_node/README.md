# real_node module

## Role
`real_node` is the physical data node. It serves object I/O RPCs and writes object payload to local disks.

## Core responsibilities
- Map `(disk_id, object_id)` to local filesystem path.
- Execute real disk I/O for object write/read/delete.
- Report disk capacity and free space.
- Optionally participate in replication and heartbeat.

## Request flow
1. Receive `WriteObject`: resolve path, create parent directories, write payload.
2. Receive `ReadObject`: resolve path and read bytes by offset/size.
3. Receive `DeleteObject`: remove object file.
4. Receive `GetDiskReport`: return disk usage and health.

## Run
```bash
./build/real_node_server --config=config/real_node.conf --port=19080
```
