from fastapi import UploadFile
from typing import Union
from fastapi import HTTPException, status


MAX_UPLOAD_BYTES = 50 * 1024 * 1024
READ_CHUNK_BYTES = 1024 * 1024

ALLOWED_TEXT_CONTENT_TYPES = {
    "text/plain",
    "application/octet-stream",  # часто приходит для файлов без расширения
}

ALLOWED_JSONL_CONTENT_TYPES = {
    "application/json",
    "text/plain",
    "application/octet-stream",
}

def _has_allowed_ext(filename: Union[str, None], allowed: set[str]) -> bool:
    if not filename:
        return True  # файл может быть без имени/расширения
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in allowed)

async def _read_limited(file: UploadFile, max_bytes: int) -> bytes:
    data = b""
    total = 0
    while True:
        chunk = await file.read(READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail={"error": "File too large", "max_bytes": max_bytes},
            )
        data += chunk
    return data
