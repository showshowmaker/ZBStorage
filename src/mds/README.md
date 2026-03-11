# mds module

## Role
`mds` is the metadata authority. It manages namespace, inode attributes, layout roots, layout objects, and PG view state.
It does not store user payload data.

## Core responsibilities
- Maintain namespace metadata: `inode`, `dentry`, permissions, file size, timestamps.
- Maintain layout metadata: layout root and layout objects (extent -> object mapping).
- Maintain PG view metadata and route policy used by object allocation.
- Serve metadata RPCs for client read/write open, allocation, commit, and lookup.
- Persist metadata in RocksDB.

## Data path model
1. Client asks MDS for namespace and layout metadata.
2. Client resolves extents to object IDs via layout.
3. Client talks directly to data nodes for object read/write.
4. Client commits write result back to MDS; MDS updates layout root atomically.

## Key config
- `MDS_DB_PATH`
- `SCHEDULER_ADDR`
- `SCHEDULER_REFRESH_MS`
- `OBJECT_UNIT_SIZE`
- `REPLICA`

## Run
```bash
./build/mds_server --config=config/mds.conf --port=9000
```
