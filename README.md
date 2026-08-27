# Deep Storage Engine

A from-scratch implementation of a distributed database engine in Python, built to understand how systems like RocksDB, LevelDB, and Raft-based databases actually work under the hood.

## What's inside

**Module 1 — Storage Engine (LSM-tree)**
- `wal/` — Write-Ahead Log for crash-safe durability
- `memtable/` — SkipList-backed in-memory store
- `sstable/` — immutable sorted files on disk
- `compaction/` — merges SSTables, resolves duplicates and deletes
- `db.py` — single entry point (`put`, `get`, `delete`)

**Module 2 — Replication & Consensus (Raft)** *(in progress)*
- `state.py` — persistent node state
- `node.py` — leader election, log replication, heartbeats

## Architecture

```
put(key, value)
    │
    ▼
   WAL        → durable immediately
    │
    ▼
 Memtable     → in-memory, sorted
    │
    ▼ (when full)
  SSTable     → immutable, sorted, on disk
    │
    ▼ (periodically)
 Compaction   → merges files, drops old versions
```

## Status

- [x] Module 1 — Storage Engine
- [ ] Module 2 — Raft (election + replication logic done, networking layer in progress)
- [ ] Module 3 — Partitioning & Distributed Transactions
- [ ] Module 4 — Query Optimizer Internals

## References

- ARIES (Mohan et al., 1992)
- Raft paper (Ongaro & Ousterhout, 2014) — raft.github.io
- RocksDB, LevelDB, PostgreSQL, SQLite source code
