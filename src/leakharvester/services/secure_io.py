import struct
from pathlib import Path
from typing import Iterator, Optional, IO, TYPE_CHECKING

if TYPE_CHECKING:
    import zstandard as zstd
    import nacl.pwhash
    import nacl.secret
    import nacl.utils
    import pyarrow as pa

# File Format Constants
MAGIC_BYTES = b'LH01'
FLAG_ENCRYPTED = 0x01
SALT_SIZE = 16 # nacl.pwhash.argon2id.SALTBYTES is 16
CHUNK_SIZE = 1024 * 1024 * 4  # 4MB Buffer for Encryption

class LHError(Exception):
    pass

class CryptoEngine:
    def __init__(self, password: Optional[str] = None):
        self.password = password.encode() if password else None
        self.box: Optional["nacl.secret.SecretBox"] = None

    def derive_key_and_init(self, salt: bytes) -> None:
        """Derives a 32-byte key using Argon2id and initializes the SecretBox."""
        if not self.password:
            raise LHError("Password required for key derivation")
        
        import nacl.pwhash
        import nacl.secret
        
        # Sensitive profile: m=1GB (approx), t=4, p=1 (libsodium defaults usually)
        # This meets the prompt's high security requirement.
        key = nacl.pwhash.argon2id.kdf(
            nacl.secret.SecretBox.KEY_SIZE,
            self.password,
            salt,
            opslimit=nacl.pwhash.argon2id.OPSLIMIT_SENSITIVE,
            memlimit=nacl.pwhash.argon2id.MEMLIMIT_SENSITIVE
        )
        self.box = nacl.secret.SecretBox(key)

    def encrypt(self, data: bytes) -> bytes:
        if not self.box: return data
        return self.box.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        if not self.box: return data
        return self.box.decrypt(data)

class EncryptedFileWriter:
    """
    A file-like object that buffers writes, encrypts them in chunks, 
    and writes [Length][EncryptedData] to the underlying file.
    """
    def __init__(self, f: IO[bytes], crypto: CryptoEngine, encrypted: bool):
        self.f = f
        self.crypto = crypto
        self.encrypted = encrypted
        self.buffer = bytearray()

    def write(self, b: bytes) -> int:
        self.buffer.extend(b)
        while len(self.buffer) >= CHUNK_SIZE:
            chunk = self.buffer[:CHUNK_SIZE]
            self._flush_chunk(bytes(chunk))
            self.buffer = self.buffer[CHUNK_SIZE:]
        return len(b)

    def _flush_chunk(self, data: bytes):
        if not data: return
        
        if self.encrypted:
            # Encrypt adds overhead (nonce + mac)
            blob = self.crypto.encrypt(data)
        else:
            blob = data
            
        # Write 4-byte length header + blob
        self.f.write(struct.pack('<I', len(blob)))
        self.f.write(blob)

    def flush(self):
        if self.buffer:
            self._flush_chunk(bytes(self.buffer))
            self.buffer = bytearray()
        self.f.flush()

    def close(self):
        self.flush()

class DecryptedFileReader:
    """
    A file-like object that reads [Length][EncryptedData] chunks from disk,
    decrypts them, and provides a continuous read stream.
    """
    def __init__(self, f: IO[bytes], crypto: CryptoEngine, encrypted: bool):
        self.f = f
        self.crypto = crypto
        self.encrypted = encrypted
        self.buffer = b''

    def read(self, size: int = -1) -> bytes:
        # If size is -1, read everything (careful with memory!)
        if size == -1:
            out = bytearray(self.buffer)
            self.buffer = b''
            while True:
                chunk = self._read_next_chunk()
                if not chunk: break
                out.extend(chunk)
            return bytes(out)

        while len(self.buffer) < size:
            chunk = self._read_next_chunk()
            if not chunk: break
            self.buffer += chunk
            
        result = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return result

    def _read_next_chunk(self) -> bytes:
        # Read Length Header
        len_bytes = self.f.read(4)
        if not len_bytes: return b''
        chunk_len = struct.unpack('<I', len_bytes)[0]
        
        # Read Data
        data = self.f.read(chunk_len)
        if len(data) != chunk_len:
            raise LHError("Unexpected EOF: Corrupt file")
            
        if self.encrypted:
            return self.crypto.decrypt(data)
        return data

class SecureIO:
    @staticmethod
    def export_data(
        output_path: Path,
        arrow_stream: Iterator["pa.RecordBatch"],
        schema: "pa.Schema",
        password: Optional[str] = None,
        compression_level: int = 3
    ) -> None:
        """Pipeline: Arrow -> ZSTD -> Crypto -> File"""
        import zstandard as zstd
        import nacl.utils
        import pyarrow as pa
        
        crypto = CryptoEngine(password)
        flags = 0x00
        salt = b''
        
        if password:
            flags |= FLAG_ENCRYPTED
            salt = nacl.utils.random(SALT_SIZE)
            crypto.derive_key_and_init(salt)

        with open(output_path, 'wb') as f:
            # Header
            f.write(MAGIC_BYTES)
            f.write(struct.pack('B', flags))
            if flags & FLAG_ENCRYPTED:
                f.write(salt)

            # Pipeline Construction
            crypto_writer = EncryptedFileWriter(f, crypto, bool(flags & FLAG_ENCRYPTED))
            
            cctx = zstd.ZstdCompressor(level=compression_level, threads=-1)
            # zstd_writer will write compressed bytes to crypto_writer
            # closefd=False because we manually manage the file
            zstd_writer = cctx.stream_writer(crypto_writer, closefd=False)
            
            # Arrow IPC Writer writes to zstd_writer
            with pa.ipc.new_stream(zstd_writer, schema) as ipc_writer:
                for batch in arrow_stream:
                    ipc_writer.write_batch(batch)
            
            # Cleanup in reverse order
            # ipc_writer closes automatically in context manager (writes footer)
            zstd_writer.close()   # Flushes ZSTD frame
            crypto_writer.close() # Flushes remaining encryption buffer

    @staticmethod
    def import_data(
        input_path: Path,
        password: Optional[str] = None
    ) -> Iterator["pa.RecordBatch"]:
        """Pipeline: File -> Crypto -> ZSTD -> Arrow"""
        if not input_path.exists():
            raise FileNotFoundError(f"File not found: {input_path}")

        # We need to keep the file open during iteration. 
        # Ideally, we yield a generator that closes the file when exhausted.
        # But for CLI usage, a context manager in the caller is better.
        # Here we will open it and rely on the generator to keep it alive? 
        # No, safe way: The generator function opens the file.

        # Verify Header first
        with open(input_path, 'rb') as f:
            magic = f.read(4)
            if magic != MAGIC_BYTES:
                raise LHError("Invalid file format")
            flags = struct.unpack('B', f.read(1))[0]
            is_encrypted = bool(flags & FLAG_ENCRYPTED)
            
            if is_encrypted and not password:
                raise LHError("Password required")
        
        # Generator
        return SecureIO._stream_generator(input_path, password)

    @staticmethod
    def _stream_generator(input_path: Path, password: Optional[str]) -> Iterator["pa.RecordBatch"]:
        import zstandard as zstd
        import pyarrow as pa
        import nacl.pwhash
        
        with open(input_path, 'rb') as f:
            # Skip Header (Already validated)
            f.read(5) 
            crypto = CryptoEngine(password)
            flags = 0x00 # Re-read flags needed? No, we know offset.
            
            # Re-read flags to be sure of encryption state (race condition check essentially)
            f.seek(4)
            flags = struct.unpack('B', f.read(1))[0]
            if flags & FLAG_ENCRYPTED:
                salt = f.read(SALT_SIZE)
                crypto.derive_key_and_init(salt)

            crypto_reader = DecryptedFileReader(f, crypto, bool(flags & FLAG_ENCRYPTED))
            
            dctx = zstd.ZstdDecompressor()
            # Decompress from crypto stream
            zstd_reader = dctx.stream_reader(crypto_reader, read_across_frames=True)
            
            try:
                with pa.ipc.open_stream(zstd_reader) as ipc_reader:
                    for batch in ipc_reader:
                        yield batch
            except Exception as e:
                raise LHError(f"Stream error: {e}")