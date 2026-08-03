from storage.wal.wal import WAL
from storage.memtable.skiplist import SkipList
import struct
class Memtable:
    def __init__(self, wal_path: str, size_limit: int):
        self.wal = WAL(wal_path)
        self.skiplist = SkipList()
        self.size_limit = size_limit
        self.size = 0

    def put(self, key: str, value: bytes) -> None:
        # 1. write to WAL first
        payload = struct.pack("<I", len(key)) + key.encode() + struct.pack("<I", len(value)) + value
        self.wal.append(payload)

        # 2. insert into SkipList
        self.skiplist.insert(key, value)
        # 3. update size
        self.size += len(payload)

    def recover(self):
        # replay WAL and insert into SkipList
        for payload in self.wal.replay():
            key_len = struct.unpack("<I", payload[:4])[0]
            key = payload[4:4 + key_len].decode()
            value_len = struct.unpack("<I", payload[4 + key_len:8 + key_len])[0]
            value = payload[8 + key_len:8 + key_len + value_len]
            self.skiplist.insert(key, value)
            self.size += len(payload)

    def get(self, key: str) -> bytes:
        # search in SkipList
        return self.skiplist.search(key)

    def is_full(self) -> bool:
        # return True if size >= size_limit
        return self.size >= self.size_limit

    def flush(self):
        # iterate all keys in order
        # yield (key, value) pairs for SSTable
        for key, value in self.skiplist:
            yield key, value

if __name__ == "__main__":
    # 1. write some data
    m = Memtable("wal.log", size_limit=1024*1024)
    m.put("apple", b"100")
    m.put("banana", b"200")
    m.put("cherry", b"300")

    print(m.get("banana"))   # → b"200"

    # 2. simulate crash — create a new Memtable from same WAL
    m2 = Memtable("wal.log", size_limit=1024*1024)
    print(m2.get("banana"))  # → None  (SkipList is empty)

    m2.recover()
    print(m2.get("banana"))  # → b"200"  (rebuilt from WAL)