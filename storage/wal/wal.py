# wal.py
import os
import struct
import zlib
import typing

BLOCK_SIZE =  32768 # block of 32KB
HEADER_SIZE = 7 # 4 bytes for checksum, 2 bytes for length, 1 byte for type

# types of log records to see if they fit in a BLOCK_SIZE
FULL = 1
FIRST = 2
MIDDLE = 3
LAST = 4




class WAL:
    def __init__(self, path: str):
        self.file_path: str = path
        self.file: typing.BinaryIO = open(self.file_path, "a+b")
        self._HEADER_FORMAT = "<IHB"  # format for struct packing/unpacking: CRC32 (4 bytes), length (2 bytes), type (1 byte)

    def append(self, payload: bytes) -> int:
        lsn: int = self.file.tell() # get the current position in the file
        
        crc: int = zlib.crc32(payload) & 0xffffffff # calculate the checksum of the payload, make sure it's a 32-bit unsigned integer
        header: bytes = struct.pack(self._HEADER_FORMAT, crc, len(payload), FULL)

        self.file.write(header)
        self.file.write(payload)

        self.file.flush()                    # buffer Python → OS
        os.fsync(self.file.fileno())         # OS → disque

        return lsn # return the log sequence number (position in the file)
    
    def replay(self):
        self.file.seek(0)  # retour au début

        # read the log records one by one
        while True:
            header = self.file.read(HEADER_SIZE)

            if len(header) < HEADER_SIZE:  # fin du fichier
                return

            crc, length, type_ = struct.unpack(self._HEADER_FORMAT, header)
            payload = self.file.read(length)

            if zlib.crc32(payload) & 0xffffffff != crc:  # CRC invalide
                return

            yield payload # yield the payload to the caller
    
    def truncate(self, up_to_lsn: int) -> None:
        # 1. read the remaining data from the current file position to the end
        self.file.seek(up_to_lsn)
        remaining = self.file.read()

        # 2. write the remaining data to a temporary file
        tmp_path = self.file_path + ".tmp"
        with open(tmp_path, "wb") as tmp:
            tmp.write(remaining)
            tmp.flush()
            os.fsync(tmp.fileno())

        # 3. close the original file
        self.file.close()

        # 4. replace the original
        os.rename(tmp_path, self.file_path)

        # 5. reopen
        self.file = open(self.file_path, "a+b")
