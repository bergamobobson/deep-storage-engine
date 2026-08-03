import heapq
import struct
from storage.sstable.sstable import SSTable

TOMBSTONE = b"__TOMBSTONE__"


class Compaction:
    def compact(self, input_paths: list[str], output_path: str) -> None:
        """
        input_paths: list of SSTable paths, oldest first, newest last
        """
        sst     = SSTable()
        heap    = []
        iters   = []

        # one iterator per SSTable, track file_id for recency
        for file_id, path in enumerate(input_paths):
            it = sst.iter_entries(path)
            iters.append((file_id, it))
            try:
                key, value = next(it)
                heapq.heappush(heap, (key, -file_id, value, file_id, it))
            except StopIteration:
                pass

        merged = []

        while heap:
            key, neg_file_id, value, file_id, it = heapq.heappop(heap)

            # collect all versions of this key
            duplicates = [(neg_file_id, value, file_id, it)]
            while heap and heap[0][0] == key:
                _, neg_id, val, fid, iterator = heapq.heappop(heap)
                duplicates.append((neg_id, val, fid, iterator))

            # newest = highest file_id = most negative neg_file_id
            newest = min(duplicates, key=lambda x: x[0])
            _, newest_value, _, _ = newest

            # skip tombstones
            if newest_value != TOMBSTONE:
                merged.append((key, newest_value))

            # advance every iterator that had this key
            for _, _, fid, iterator in duplicates:
                try:
                    next_key, next_value = next(iterator)
                    heapq.heappush(heap, (next_key, -fid, next_value, fid, iterator))
                except StopIteration:
                    pass

        # write merged output
        sst.write_from_iter(iter(merged), output_path)

if __name__ == "__main__":
    from storage.sstable.sstable import SSTable
    from storage.memtable.memtable import Memtable
    from storage.compaction.compaction import Compaction, TOMBSTONE
    import os

    # clean up
    for f in ["wal1.log", "wal2.log", "t1.sst", "t2.sst", "merged.sst"]:
        os.remove(f) if os.path.exists(f) else None

    sst = SSTable()

    # SSTable 1 — older
    m1 = Memtable("wal1.log", size_limit=1024*1024)
    m1.put("apple",  b"100")
    m1.put("banana", b"200")
    m1.put("cherry", b"300")
    sst.write(m1, "t1.sst")

    # SSTable 2 — newer
    m2 = Memtable("wal2.log", size_limit=1024*1024)
    m2.put("banana", b"999")   # override
    m2.put("date",   b"400")
    m2.put("banana", TOMBSTONE)  # delete banana
    sst.write(m2, "t2.sst")

    # compact
    c = Compaction()
    c.compact(["t1.sst", "t2.sst"], "merged.sst")

    # read merged
    print(sst.read("apple",  "merged.sst"))   # → b"100"
    print(sst.read("banana", "merged.sst"))   # → None (tombstone)
    print(sst.read("cherry", "merged.sst"))   # → b"300"
    print(sst.read("date",   "merged.sst"))   # → b"400"