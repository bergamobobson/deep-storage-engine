import struct


class SSTable:
    def write(self, memtable, path: str):
        index = []

        with open(path, "wb") as f:
            # Data block
            for key, value in memtable.flush():
                offset = f.tell()

                key_bytes = key.encode()
                value_bytes = value if isinstance(value, bytes) else value.encode()

                f.write(struct.pack("<I", len(key_bytes)))
                f.write(key_bytes)

                f.write(struct.pack("<I", len(value_bytes)))
                f.write(value_bytes)

                index.append((key, offset))

            # Index block
            index_offset = f.tell()

            for key, offset in index:
                key_bytes = key.encode()

                f.write(struct.pack("<I", len(key_bytes)))
                f.write(key_bytes)
                f.write(struct.pack("<Q", offset))

            # Footer
            f.write(struct.pack("<Q", index_offset))
            f.write(struct.pack("<I", len(index)))

    def read(self, key: str, path: str):
        with open(path, "rb") as f:
            # Footer
            f.seek(-(8 + 4), 2)

            index_offset = struct.unpack("<Q", f.read(8))[0]
            num_entries = struct.unpack("<I", f.read(4))[0]

            # Read index
            f.seek(index_offset)

            data_offset = None

            for _ in range(num_entries):
                key_len = struct.unpack("<I", f.read(4))[0]
                index_key = f.read(key_len).decode()
                offset = struct.unpack("<Q", f.read(8))[0]

                if index_key == key:
                    data_offset = offset
                    break

            if data_offset is None:
                return None

            # Read data record
            f.seek(data_offset)

            key_len = struct.unpack("<I", f.read(4))[0]
            f.read(key_len)  # Skip key

            value_len = struct.unpack("<I", f.read(4))[0]
            value = f.read(value_len)

            return value


if __name__ == "__main__":
    import os
    os.remove("wal.log") if os.path.exists("wal.log") else None
    os.remove("table.sst") if os.path.exists("table.sst") else None

    from storage.memtable.memtable import Memtable

    m = Memtable("wal.log", size_limit=1024*1024)
    m.put("apple", b"100")
    m.put("banana", b"200")
    m.put("cherry", b"300")

    sst = SSTable()
    sst.write(m, "table.sst")

    print(sst.read("banana", "table.sst"))
    print(sst.read("apple", "table.sst"))
    print(sst.read("grape", "table.sst"))