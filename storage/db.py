# storage/db.py
import os
from storage.memtable.memtable import Memtable
from storage.sstable.sstable import SSTable
from storage.compaction.compaction import Compaction, TOMBSTONE

class DB:
    def __init__(self, data_dir: str, memtable_size: int = 1024 * 1024):
        self.data_dir      = data_dir
        self.memtable_size = memtable_size
        self.sst           = SSTable()
        self.compaction    = Compaction()
        self.sstable_paths = []  # oldest first, newest last
        self.sstable_counter = 0

        os.makedirs(data_dir, exist_ok=True)

        # discover existing SSTables
        existing = sorted([
            f for f in os.listdir(data_dir)
            if f.startswith("sstable_") and f.endswith(".sst")
        ])
        for f in existing:
            self.sstable_paths.append(os.path.join(data_dir, f))
            self.sstable_counter = max(
                self.sstable_counter,
                int(f.replace("sstable_", "").replace(".sst", ""))
            )

        # recover memtable from WAL
        wal_path = os.path.join(data_dir, "wal.log")
        self.memtable = Memtable(wal_path, memtable_size)
        self.memtable.recover()

    def put(self, key: str, value: bytes) -> None:
        self.memtable.put(key, value)
        if self.memtable.is_full():
            self._flush()

    def delete(self, key: str) -> None:
        self.memtable.put(key, TOMBSTONE)
        if self.memtable.is_full():
            self._flush()

    def get(self, key: str) -> bytes:
        # 1. check memtable first
        value = self.memtable.get(key)
        if value is not None:
            return None if value == TOMBSTONE else value

        # 2. check SSTables newest to oldest
        for path in reversed(self.sstable_paths):
            value = self.sst.read(key, path)
            if value is not None:
                return None if value == TOMBSTONE else value

        return None

    def _flush(self) -> None:
        self.sstable_counter += 1
        path = os.path.join(self.data_dir, f"sstable_{self.sstable_counter}.sst")
        self.sst.write(self.memtable, path)
        self.sstable_paths.append(path)

        # reset memtable with fresh WAL
        wal_path = os.path.join(self.data_dir, "wal.log")
        os.remove(wal_path) if os.path.exists(wal_path) else None
        self.memtable = Memtable(wal_path, self.memtable_size)

        # compact if too many SSTables
        if len(self.sstable_paths) >= 4:
            self._compact()

    def _compact(self) -> None:
        self.sstable_counter += 1
        output_path = os.path.join(
            self.data_dir, f"sstable_{self.sstable_counter}.sst"
        )
        self.compaction.compact(self.sstable_paths, output_path)

        # remove old SSTables
        for path in self.sstable_paths:
            os.remove(path)

        self.sstable_paths = [output_path]

if __name__ == "__main__":
    import os, shutil
    from storage.db import DB

    # clean slate
    shutil.rmtree("testdb", ignore_errors=True)

    db = DB("testdb")

    db.put("apple",  b"100")
    db.put("banana", b"200")
    db.put("cherry", b"300")

    print(db.get("banana"))   # → b"200"
    print(db.get("grape"))    # → None

    db.put("banana", b"999")
    print(db.get("banana"))   # → b"999"

    db.delete("apple")
    print(db.get("apple"))    # → None

    # simulate restart
    db2 = DB("testdb")
    print(db2.get("banana"))  # → b"999"
    print(db2.get("cherry"))  # → b"300"
    print(db2.get("apple"))   # → None