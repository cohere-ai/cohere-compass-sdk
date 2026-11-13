"""Private-file filesystem primitives for the file-backed upload job store."""

import asyncio
import os
import tempfile
from pathlib import Path

PRIVATE_DIR_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


async def ensure_private_dir(path: Path) -> None:
    """Create *path* and its parents if needed, then force mode 0700."""
    await asyncio.to_thread(_ensure_private_dir, path)


def ensure_private_dir_sync(path: Path) -> None:
    """Create *path* and its parents if needed, then force mode 0700, blocking."""
    _ensure_private_dir(path)


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(mode=PRIVATE_DIR_MODE, parents=True, exist_ok=True)
    path.chmod(PRIVATE_DIR_MODE)


async def atomic_write_bytes(path: Path, data: bytes, *, mode: int = PRIVATE_FILE_MODE) -> None:
    """Write *data* to *path* atomically via a temp file: fsync, replace, chmod."""
    await asyncio.to_thread(_atomic_write_bytes, path, data, mode)


def _atomic_write_bytes(path: Path, data: bytes, mode: int) -> None:
    """Write through a temporary file, which is removed if it never replaced *path*."""
    temp_path: Path | None = None
    try:
        descriptor, temp_name = tempfile.mkstemp(dir=path.parent)
        temp_path = Path(temp_name)
        os.chmod(temp_path, mode)
        with os.fdopen(descriptor, "wb") as temp_file:
            temp_file.write(data)
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
        os.chmod(path, mode)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


async def read_bytes(path: Path) -> bytes:
    """Read all of *path* off the event loop."""
    return await asyncio.to_thread(path.read_bytes)


async def path_exists(path: Path) -> bool:
    """Whether *path* exists, as an awaited stat."""
    return await asyncio.to_thread(path.exists)


async def is_file(path: Path) -> bool:
    """Whether *path* exists and is a regular file, as an awaited stat."""
    return await asyncio.to_thread(path.is_file)


async def remove(path: Path) -> None:
    """Unlink *path*; propagates `FileNotFoundError` like `Path.unlink`."""
    await asyncio.to_thread(path.unlink)


async def glob(directory: Path, pattern: str) -> list[Path]:
    """List *directory* entries matching *pattern*, materialised off the loop."""
    return await asyncio.to_thread(lambda: sorted(directory.glob(pattern)))
