import pytest
import pyarrow as pa
import zstandard as zstd
import nacl.exceptions
from pathlib import Path
from leakharvester.services.secure_io import SecureIO, LHError, MAGIC_BYTES

# Helper to create a dummy Arrow Batch
def create_dummy_batch(num_rows=1000):
    emails = pa.array([f"user{i}@example.com" for i in range(num_rows)])
    passwords = pa.array([f"pass{i}" for i in range(num_rows)])
    
    schema = pa.schema([
        ("email", pa.string()),
        ("password", pa.string())
    ])
    
    batch = pa.RecordBatch.from_arrays([emails, passwords], schema=schema)
    return batch, schema

def test_export_import_plaintext(tmp_path):
    output_file = tmp_path / "plaintext.lh"
    batch, schema = create_dummy_batch(100)
    
    # 1. Export
    SecureIO.export_data(
        output_file,
        iter([batch]),
        schema,
        password=None,
        compression_level=1
    )
    
    assert output_file.exists()
    
    # 2. Verify Header (Magic + Flags)
    with open(output_file, "rb") as f:
        magic = f.read(4)
        flags = f.read(1)[0]
        assert magic == MAGIC_BYTES
        assert flags & 0x01 == 0 # Not Encrypted
    
    # 3. Import
    imported_batches = list(SecureIO.import_data(output_file, password=None))
    assert len(imported_batches) == 1
    
    imported_table = pa.Table.from_batches(imported_batches)
    assert imported_table.num_rows == 100
    assert imported_table["email"][0].as_py() == "user0@example.com"

def test_export_import_encrypted(tmp_path):
    output_file = tmp_path / "encrypted.lh"
    batch, schema = create_dummy_batch(500)
    password = "supersecretpassword"
    
    # 1. Export
    SecureIO.export_data(
        output_file,
        iter([batch]),
        schema,
        password=password,
        compression_level=1
    )
    
    # 2. Verify Header
    with open(output_file, "rb") as f:
        magic = f.read(4)
        flags = f.read(1)[0]
        assert magic == MAGIC_BYTES
        assert flags & 0x01 == 1 # Encrypted Flag
    
    # 3. Import (Success)
    imported_batches = list(SecureIO.import_data(output_file, password=password))
    assert len(imported_batches) == 1
    assert imported_batches[0].num_rows == 500

def test_import_wrong_password(tmp_path):
    output_file = tmp_path / "encrypted_fail.lh"
    batch, schema = create_dummy_batch(10)
    
    SecureIO.export_data(output_file, iter([batch]), schema, password="correct", compression_level=1)
    
    # SecureIO uses PyNaCl SecretBox. On decryption failure, it usually raises CryptoError.
    # Our wrapper might let that bubble up or catch it.
    # The current implementation lets it bubble from crypto.decrypt.
    
    with pytest.raises(Exception): # sodium.exceptions.CryptoError usually
        list(SecureIO.import_data(output_file, password="wrong"))

def test_import_missing_password(tmp_path):
    output_file = tmp_path / "encrypted_no_pass.lh"
    batch, schema = create_dummy_batch(10)
    SecureIO.export_data(output_file, iter([batch]), schema, password="correct", compression_level=1)
    
    # The header check in import_data should raise "Password required" LHError
    with pytest.raises(LHError, match="Password required"):
        list(SecureIO.import_data(output_file, password=None))

def test_streaming_large_data(tmp_path):
    """Verifies that we can process multiple batches continuously."""
    output_file = tmp_path / "stream.lh"
    batch1, schema = create_dummy_batch(100)
    batch2, _ = create_dummy_batch(100)
    
    SecureIO.export_data(
        output_file,
        iter([batch1, batch2]),
        schema,
        password=None,
        compression_level=1
    )
    
    batches = list(SecureIO.import_data(output_file))
    total_rows = sum(b.num_rows for b in batches)
    assert total_rows == 200

def test_invalid_file_format(tmp_path):
    bad_file = tmp_path / "bad.txt"
    bad_file.write_bytes(b"NOT_MAGIC_BYTES")
    
    with pytest.raises(LHError, match="Invalid file format"):
        list(SecureIO.import_data(bad_file))
