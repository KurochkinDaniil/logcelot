"""Unit tests for file helpers."""
import pytest
from io import BytesIO
from fastapi import UploadFile, HTTPException

from src.api.file_helpers import (
    _has_allowed_ext,
    _read_limited,
    MAX_UPLOAD_BYTES,
    READ_CHUNK_BYTES,
)


class TestHasAllowedExt:
    """Test _has_allowed_ext function."""

    def test_no_filename_returns_true(self):
        """Test that None filename is allowed."""
        assert _has_allowed_ext(None, {".log", ".txt"})

    def test_empty_filename_returns_true(self):
        """Test that empty filename is allowed."""
        assert _has_allowed_ext("", {".log", ".txt"})

    def test_allowed_extension_lowercase(self):
        """Test that lowercase allowed extension is matched."""
        assert _has_allowed_ext("test.log", {".log", ".txt"})

    def test_allowed_extension_uppercase(self):
        """Test that uppercase extension is matched (case insensitive)."""
        assert _has_allowed_ext("test.LOG", {".log", ".txt"})

    def test_disallowed_extension(self):
        """Test that disallowed extension is not matched."""
        assert not _has_allowed_ext("test.csv", {".log", ".txt"})

    def test_multiple_dots_in_filename(self):
        """Test filename with multiple dots."""
        assert _has_allowed_ext("my.test.log", {".log"})


@pytest.mark.asyncio
class TestReadLimited:
    """Test _read_limited function."""

    async def test_read_small_file(self):
        """Test reading a small file."""
        content = b"test content"
        file = UploadFile(filename="test.txt", file=BytesIO(content))
        
        result = await _read_limited(file, max_bytes=1024)
        
        assert result == content

    async def test_read_empty_file(self):
        """Test reading an empty file."""
        file = UploadFile(filename="empty.txt", file=BytesIO(b""))
        
        result = await _read_limited(file, max_bytes=1024)
        
        assert result == b""

    async def test_read_file_exceeds_limit(self):
        """Test that large file raises HTTPException."""
        # Create file larger than limit
        large_content = b"x" * 1000
        file = UploadFile(filename="large.txt", file=BytesIO(large_content))
        
        with pytest.raises(HTTPException) as exc_info:
            await _read_limited(file, max_bytes=100)
        
        assert exc_info.value.status_code == 413
        assert "File too large" in str(exc_info.value.detail)

    async def test_read_exactly_at_limit(self):
        """Test reading file exactly at the limit."""
        content = b"x" * 100
        file = UploadFile(filename="exact.txt", file=BytesIO(content))
        
        result = await _read_limited(file, max_bytes=100)
        
        assert result == content
        assert len(result) == 100

    async def test_read_multiple_chunks(self):
        """Test reading file that spans multiple chunks."""
        # Create file larger than READ_CHUNK_BYTES
        content = b"a" * (READ_CHUNK_BYTES + 1000)
        file = UploadFile(filename="multi.txt", file=BytesIO(content))
        
        result = await _read_limited(file, max_bytes=MAX_UPLOAD_BYTES)
        
        assert result == content
        assert len(result) == READ_CHUNK_BYTES + 1000
