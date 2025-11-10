import asyncio
import json
import struct
from typing import Any

MAX_FRAME_LEN = 64 * 1024 * 1024  # 64 MB
_LEN = struct.Struct("!I")


class FrameTooLarge(Exception):
    pass

def _to_json_bytes(obj: Any) -> bytes:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return s.encode("utf-8")

async def write_frame(writer: asyncio.StreamWriter, obj: Any) -> None:
    payload = _to_json_bytes(obj)
    n = len(payload)
    if n > MAX_FRAME_LEN:
        raise FrameTooLarge(f"payload {n} > MAX_FRAME_LEN {MAX_FRAME_LEN}")
    writer.write(_LEN.pack(n))
    writer.write(payload)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader) -> Any:
    raw_len = await reader.readexactly(_LEN.size)
    (n,) = _LEN.unpack(raw_len)

    if n > MAX_FRAME_LEN:
        raise FrameTooLarge(f"frame length {n} > MAX_FRAME_LEN {MAX_FRAME_LEN}")
    if n == 0:
        raise ValueError("empty frame is not allowed")
    data = await reader.readexactly(n)
    return json.loads(data.decode("utf-8"))


__all__ = ["MAX_FRAME_LEN", "FrameTooLarge", "write_frame", "read_frame"]
