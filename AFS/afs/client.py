# afs/client.py
import base64
import json
from pathlib import Path
from typing import Dict, Optional
from AFS.rpc.client import RPCClient
import uuid

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
    - open(path, mode) -> fd
    - create(path) -> fd
    - read(fd, size) -> bytes
    - write(fd, data) -> int
    - close(fd) -> None
    """

    def __init__(self, rpc: RPCClient, cache: Optional[LocalCache] = None):
        self.rpc = rpc
        self.cache = cache or LocalCache()
        self._next_fd = 1
        self._open_files: Dict[int, Dict] = {}

    def _normalize_path(self, path: str) -> str:
        #path start with /
        if not path.startswith("/"):
            path = "/"+path
        return path
    
    async def open(self, path: str, mode: str="r") -> int:
        """open a file and return a descripter"""
        path = self._normalize_path(path)
        local_path = self.cache.fs_path(path)
        fetch = True
        if mode == "r" or mode == "r+":
            client_ver = self.cache.get_version(path)
            if client_ver > 0 and local_path.exists():
                try:
                    r = await self.rpc.call("TestAuth", {
                        "path": path,
                        "client_version": client_ver
                    })
                    if r["code"] == 0 and not r["data"]["changed"]:
                        fetch = False
                except Exception as e:
                    print(f"TestAuth failed: {e}")
        if fetch:
            try:
                r1 = await self.rpc.call("Open", {"path": path})
                if r1["code"] != 0:
                    raise FileNotFoundError(f"Cannot open {path}: {r1['err']}")
                server_ver = int(r1["data"]["version"])
                r2 = await self.rpc.call("GetFile", {"path": path})
                if r2["code"] != 0:
                    raise IOError(f"Cannot fetch {path}: {r2['err']}")
                content = base64.b64decode(r2["data"]["bytes"].encode("ascii"))
                #write to local cache
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(content)
                self.cache.set_version(path, server_ver)
                print(f"[client] Fetched {path} from server ({len(content)} bytes)")
            except Exception as e:
                raise IOError(f"Failed to open {path}: {e}")
        #open file with API
        mode_map = {'r': 'rb', 'w': 'wb', 'r+': 'r+b', 'a': 'ab'}
        posix_mode = mode_map.get(mode, 'rb')
        try:
            file_obj = open(local_path, posix_mode)
        except FileNotFoundError:
            if mode == 'w':
                local_path.parent.mkdir(parents=True, exist_ok=True)
                file_obj = open(local_path, posix_mode)
            else:
                raise
        #allocate file descriptor
        fd = self._next_fd
        self._next_fd += 1
        self._open_files[fd] = {
            'path': path,
            'file_obj': file_obj,
            'modified': mode in ['w', 'r+', 'a'],
            'version': self.cache.get_version(path),
            'write_mode': mode in ['w', 'r+', 'a']
        }
        return fd
    
    async def create(self, path: str) -> int:
        """create a new file and return file descriptor"""
        path = self._normalize_path(path)
        local_path = self.cache.fs_path(path)
        op_id = f"create-{path}-{uuid.uuid4()}"
        try:
            #pass op_id
            r = await self.rpc.call("Create", {"path": path}, op_id=op_id)
            if r["code"] != 0:
                raise FileExistsError(f"Cannot create {path}: {r['err']}")
            server_ver = int(r["data"]["version"])
            #create local file
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(b"")
            self.cache.set_version(path, server_ver)
            print(f"[client] Created {path}")
        except Exception as e:
            raise IOError(f"Failed to create {path}: {e}")
        file_obj = open(local_path, 'w+b')
        fd = self._next_fd
        self._next_fd += 1
        self._open_files[fd] = {
            'path': path,
            'file_obj': file_obj,
            'modified': True,
            'version': server_ver,
            'write_mode': True
        }
        return fd
    
    def read(self, fd: int, size: int = -1) -> bytes:
        """read from local cached file"""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        file_obj = self._open_files[fd]['file_obj']
        return file_obj.read(size)

    def write(self, fd: int, data: bytes) -> int:
        """write to local cached file"""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        file_info = self._open_files[fd]
        if not file_info['write_mode']:
            raise IOError(f"{fd} not opened for writing")
        file_obj = file_info['file_obj']
        result = file_obj.write(data)
        file_info['modified'] = True
        return result

    def seek(self, fd: int, offset: int, whence: int = 0) -> int:
        """Seek to position in file."""
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        file_obj = self._open_files[fd]['file_obj']
        return file_obj.seek(offset, whence)

    async def close(self, fd: int) -> None:
        """close file descriptor """
        if fd not in self._open_files:
            raise ValueError(f"Invalid file descriptor: {fd}")
        file_info = self._open_files[fd]
        path = file_info['path']
        file_obj = file_info['file_obj']
        modified = file_info['modified']
        base_version = file_info['version']
        file_obj.close()
        if modified:
            try:
                local_path = self.cache.fs_path(path)
                content = local_path.read_bytes()
                b64_content = base64.b64encode(content).decode("ascii")
                r = await self.rpc.call("PutFile", {
                    "path": path,
                    "bytes": b64_content,
                    "base_version": base_version
                })
                if r["code"] != 0:
                    print(f"[client] Warning: Failed to flush {path}: {r['err']}")
                else:
                    new_ver = int(r["data"]["new_version"])
                    self.cache.set_version(path, new_ver)
                    print(f"[client] Flushed {path} to server ({len(content)} bytes, v{new_ver})")
            except Exception as e:
                print(f"[client] Error flushing {path}: {e}")
        del self._open_files[fd]



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
