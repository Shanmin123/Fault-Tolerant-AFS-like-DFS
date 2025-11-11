# afs/handlers.py
import asyncio, base64, hashlib, json
from pathlib import Path
from typing import Dict

_DATA_DIR = Path("./srv_data").resolve()
_META_FILE = _DATA_DIR / ".meta.json"

_meta: Dict[str, Dict[str, object]] = {}
_locks: Dict[str, asyncio.Lock] = {}

def _load_meta() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    if _META_FILE.exists():
        try:
            global _meta
            _meta = json.loads(_META_FILE.read_text(encoding="utf-8"))
        except Exception:
            _meta = {}

async def _save_meta() -> None:
    tmp = _META_FILE.with_suffix(".json.tmp")
    text = json.dumps(_meta, ensure_ascii=False, separators=(",", ":"))
    await asyncio.to_thread(tmp.write_text, text, encoding="utf-8")
    await asyncio.to_thread(tmp.replace, _META_FILE)

def _norm(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    p = (_DATA_DIR / path.lstrip("/")).resolve()
    if not str(p).startswith(str(_DATA_DIR.resolve())):
        raise ValueError("path escapes data dir")
    return path

def _fs_path(norm: str) -> Path:
    return (_DATA_DIR / norm.lstrip("/")).resolve()

def _sha256(b: bytes) -> str:
    h = hashlib.sha256(); h.update(b); return h.hexdigest()

def _lock_for(norm: str) -> asyncio.Lock:
    if norm not in _locks:
        _locks[norm] = asyncio.Lock()
    return _locks[norm]

_load_meta()

async def Open(path: str, flags: int = 0):
    norm = _norm(path)
    fp = _fs_path(norm)
    if norm not in _meta:
        if await asyncio.to_thread(fp.exists):
            b = await asyncio.to_thread(fp.read_bytes)
            _meta[norm] = {"version": 1, "size": len(b), "sha256": _sha256(b)}
        else:
            await asyncio.to_thread(fp.parent.mkdir, parents=True, exist_ok=True)
            _meta[norm] = {"version": 0, "size": 0, "sha256": _sha256(b"")}
        await _save_meta()
    e = _meta[norm]
    return {"handle": 1, "version": int(e["version"]), "size": int(e["size"])}

#ADD: Create operation for new files
async def Create(path: str):
    norm = _norm(path)
    fp = _fs_path(norm)
    if norm in _meta and await asyncio.to_thread(fp.exists):
        raise FileExistsError(f"File exists:{norm}")
    #create empty file
    await asyncio.to_thread(fp.parent.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread(fp.write_bytes, b"")
    #initialize metadata
    _meta[norm] = {"version": 1,
                   "size": 0,
                   "sha256": _sha256(b"")}
    await _save_meta() 
    return {"handle": 1, "version": 1, "size": 0}

async def TestAuth(path: str, client_version: int):
    norm = _norm(path)
    e = _meta.get(norm)
    sv = int(e["version"]) if e else 0
    return {"changed": sv != client_version, "server_version": sv}

async def GetFile(path: str):
    norm = _norm(path)
    fp = _fs_path(norm)
    if not await asyncio.to_thread(fp.exists):
        raise FileNotFoundError(f"no such file: {norm}")
    b = await asyncio.to_thread(fp.read_bytes)
    e = _meta.get(norm) or {"version": 0, "size": 0, "sha256": _sha256(b)}
    return {"bytes": base64.b64encode(b).decode("ascii"), "version": int(e["version"])}

async def PutFile(path: str, bytes: str, base_version: int):
    norm = _norm(path)
    lock = _lock_for(norm)
    async with lock:
        e = _meta.get(norm)
        cur = int(e["version"]) if e else 0
        if base_version != cur:
            raise ValueError("version conflict")
        data = base64.b64decode(bytes.encode("ascii"))
        fp = _fs_path(norm)
        await asyncio.to_thread(fp.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(fp.write_bytes, data)
        nv = cur + 1
        _meta[norm] = {"version": nv, "size": len(data), "sha256": _sha256(data)}
        await _save_meta() # CHANGED: call async save
        return {"ok": True, "new_version": nv}