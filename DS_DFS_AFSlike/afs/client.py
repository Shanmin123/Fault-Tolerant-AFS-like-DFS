# afs/client.py
import base64
import json
from pathlib import Path
from typing import Dict, Optional
from rpc.client import RPCClient


class LocalCache:
    """
    Local cache manager under ./cache directory.
    Stores:
      - Cached files
      - .meta.json for version tracking
    """
    def __init__(self, root: Optional[Path] = None):
        self.root = (root or Path("./cache")).resolve()
        self.meta_file = self.root / ".meta.json"
        self.meta: Dict[str, Dict[str, int]] = {}
        self.root.mkdir(parents=True, exist_ok=True)
        if self.meta_file.exists():
            try:
                self.meta = json.loads(self.meta_file.read_text(encoding="utf-8"))
            except Exception:
                self.meta = {}

    def _save(self):
        self.meta_file.write_text(
            json.dumps(self.meta, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8"
        )

    def fs_path(self, path: str) -> Path:
        """Map logical path to local filesystem path."""
        if not path.startswith("/"):
            path = "/" + path
        p = (self.root / path.lstrip("/")).resolve()
        if not str(p).startswith(str(self.root)):
            raise ValueError("path escapes cache root")
        return p

    def get_version(self, path: str) -> int:
        if not path.startswith("/"):
            path = "/" + path
        return int(self.meta.get(path, {}).get("version", 0))

    def set_version(self, path: str, version: int) -> None:
        if not path.startswith("/"):
            path = "/" + path
        entry = self.meta.get(path, {})
        entry["version"] = int(version)
        self.meta[path] = entry
        self._save()


class AFSClient:
    """
    Thin wrapper on top of RPCClient to provide AFS operations.
    - open_sync_read(path): keep cache up-to-date
    - put(path): upload changes using optimistic concurrency
    """

    def __init__(self, rpc: RPCClient, cache: Optional[LocalCache] = None):
        self.rpc = rpc
        self.cache = cache or LocalCache()

    async def open_sync_read(self, path: str) -> Dict[str, object]:
        """Open → TestAuth → (maybe) GetFile"""
        r1 = await self.rpc.call("Open", {"path": path})
        if r1["code"] != 0:
            return {"ok": False, "err": r1["err"]}

        server_ver = int(r1["data"]["version"])
        size = int(r1["data"]["size"])
        client_ver = self.cache.get_version(path)

        r2 = await self.rpc.call("TestAuth", {"path": path, "client_version": client_ver})
        if r2["code"] != 0:
            return {"ok": False, "err": r2["err"]}

        downloaded = False
        if r2["data"]["changed"]:
            r3 = await self.rpc.call("GetFile", {"path": path})
            if r3["code"] != 0:
                return {"ok": False, "err": r3["err"]}
            b = base64.b64decode(r3["data"]["bytes"].encode("ascii"))
            fp = self.cache.fs_path(path)
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_bytes(b)
            self.cache.set_version(path, int(r3["data"]["version"]))
            downloaded = True
        else:
            self.cache.set_version(path, server_ver)

        return {"ok": True, "version": self.cache.get_version(path),
                "size": size, "downloaded": downloaded}

    async def put(self, path: str) -> Dict[str, object]:
        """Upload local file from cache directory."""
        fp = self.cache.fs_path(path)
        if not fp.exists():
            return {"ok": False, "err": f"local file not found: {fp}"}

        data = fp.read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        base_v = self.cache.get_version(path)

        r = await self.rpc.call("PutFile", {"path": path, "bytes": b64, "base_version": base_v})
        if r["code"] != 0:
            return {"ok": False, "err": r["err"]}

        new_v = int(r["data"]["new_version"])
        self.cache.set_version(path, new_v)
        return {"ok": True, "new_version": new_v}
