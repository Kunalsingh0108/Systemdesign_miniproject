"""S3-compatible object storage (MinIO) with a filesystem fallback.

Object keys always follow ``data/batches/YYYY/MM/DD/batch-<id>.jsonl.gz``.
The fallback writes to ``settings.data_dir/objects`` with the same key layout.
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from app.config.settings import settings
from app.utils.logging import get_logger

log = get_logger("traceflow.object_store")


class StorageUnavailable(RuntimeError):
    pass


class ObjectStore:
    backend = "base"

    def __init__(self) -> None:
        self._fail_until = 0.0

    def fail_for(self, seconds: float) -> None:
        self._fail_until = time.time() + seconds

    @property
    def failure_active(self) -> bool:
        return time.time() < self._fail_until

    def _guard(self) -> None:
        if self.failure_active:
            raise StorageUnavailable("object storage is unavailable (simulated outage)")

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def put_object(self, key: str, data: bytes) -> None: ...
    async def get_object(self, key: str) -> bytes: ...
    async def object_exists(self, key: str) -> bool: ...
    async def health(self) -> bool: ...
    async def list_objects(self, prefix: str = "") -> list[dict[str, Any]]: ...


class MinioObjectStore(ObjectStore):
    backend = "minio"

    def __init__(self) -> None:
        super().__init__()
        self._client: Any = None

    async def start(self) -> None:
        from minio import Minio

        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )

        def _ensure_bucket() -> None:
            if not self._client.bucket_exists(settings.minio_bucket):
                self._client.make_bucket(settings.minio_bucket)

        await asyncio.to_thread(_ensure_bucket)
        log.info("minio object store ready", extra={"bucket": settings.minio_bucket})

    async def close(self) -> None:
        self._client = None

    async def put_object(self, key: str, data: bytes) -> None:
        self._guard()
        import io

        def _put() -> None:
            self._client.put_object(
                settings.minio_bucket,
                key,
                io.BytesIO(data),
                length=len(data),
                content_type="application/gzip",
            )

        try:
            await asyncio.to_thread(_put)
        except Exception as exc:
            raise StorageUnavailable(str(exc)) from exc

    async def get_object(self, key: str) -> bytes:
        self._guard()

        def _get() -> bytes:
            response = self._client.get_object(settings.minio_bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        try:
            return await asyncio.to_thread(_get)
        except Exception as exc:
            raise StorageUnavailable(str(exc)) from exc

    async def object_exists(self, key: str) -> bool:
        self._guard()
        try:
            await asyncio.to_thread(self._client.stat_object, settings.minio_bucket, key)
            return True
        except Exception:
            return False

    async def health(self) -> bool:
        if self._client is None or self.failure_active:
            return False
        try:
            await asyncio.to_thread(self._client.bucket_exists, settings.minio_bucket)
            return True
        except Exception:
            return False

    async def list_objects(self, prefix: str = "") -> list[dict[str, Any]]:
        self._guard()

        def _list() -> list[dict[str, Any]]:
            out = []
            for obj in self._client.list_objects(settings.minio_bucket, prefix=prefix, recursive=True):
                out.append(
                    {
                        "key": obj.object_name,
                        "size": obj.size,
                        "last_modified": obj.last_modified.timestamp() if obj.last_modified else None,
                    }
                )
            return out

        return await asyncio.to_thread(_list)


class FilesystemObjectStore(ObjectStore):
    """Local fallback that mimics the bucket layout on disk."""

    backend = "filesystem"

    def __init__(self, root: str | None = None) -> None:
        super().__init__()
        self.root = Path(root or Path(settings.data_dir) / "objects").resolve()

    async def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        log.info("filesystem object store ready", extra={"root": str(self.root)})

    async def close(self) -> None:
        return None

    def _path(self, key: str) -> Path:
        safe = key.replace("\\", "/").lstrip("/")
        path = (self.root / safe).resolve()
        if self.root.resolve() not in path.parents and path != self.root.resolve():
            raise ValueError("invalid object key")
        return path

    async def put_object(self, key: str, data: bytes) -> None:
        self._guard()
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        await asyncio.to_thread(tmp.write_bytes, data)
        await asyncio.to_thread(tmp.replace, path)

    async def get_object(self, key: str) -> bytes:
        self._guard()
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(key)
        return await asyncio.to_thread(path.read_bytes)

    async def object_exists(self, key: str) -> bool:
        self._guard()
        return self._path(key).exists()

    async def health(self) -> bool:
        return self.root.exists() and not self.failure_active

    async def list_objects(self, prefix: str = "") -> list[dict[str, Any]]:
        self._guard()
        base = self._path(prefix) if prefix else self.root
        out: list[dict[str, Any]] = []
        if base.is_file():
            files = [base]
        else:
            files = [p for p in base.rglob("*") if p.is_file() and not p.name.endswith(".tmp")]
        for path in files:
            stat = path.stat()
            out.append(
                {
                    "key": str(path.relative_to(self.root)).replace("\\", "/"),
                    "size": stat.st_size,
                    "last_modified": stat.st_mtime,
                }
            )
        return sorted(out, key=lambda o: o["key"])


async def create_object_store() -> ObjectStore:
    backend = settings.object_store_backend.lower()
    if backend in {"minio", "auto"}:
        store = MinioObjectStore()
        try:
            await store.start()
            return store
        except Exception as exc:
            if backend == "minio":
                log.error("minio unavailable", extra={"error": str(exc)})
                raise
            log.warning("minio unavailable, using filesystem object store", extra={"error": str(exc)})
    store = FilesystemObjectStore()
    await store.start()
    return store
