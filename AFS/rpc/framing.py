"""
Module for sending and receiving framed JSON-encoded data over asynchronous streams.

This module provides utilities to serialize Python objects into JSON format,
package them into frames with length information, and asynchronously
send/receive these frames over network streams. It ensures proper handling
of data boundaries during transmission.

Constants:
    MAX_LEN: The maximum length for a frame, set to 64MB.
    LEN_STRUCT: Struct definition for a 4-byte unsigned integer in network byte order
                (big-endian), used for framing data.

Functions:
    write_frame: Asynchronously sends a Python object as a framed JSON-encoded payload.
    read_frame: Asynchronously reads a framed JSON-encoded payload and decodes it into
                a Python object.
"""

import asyncio
import json
import struct
from typing import Any

# Upper bound for a single JSON payload (bytes). Prevents memory abuse.
MAX_FRAME_LEN = 64 * 1024 * 1024  # 64 MB

# Precompiled struct for a 4-byte big-endian unsigned length prefix.
_LEN = struct.Struct("!I")


class FrameTooLarge(Exception):
    """Raised when the incoming frame length exceeds MAX_FRAME_LEN."""
    pass


def _to_json_bytes(obj: Any) -> bytes:
    """
    Serialize a Python object to compact UTF-8 JSON bytes.
    - ensure_ascii=False keeps UTF-8 characters as-is (shorter, human-readable).
    - separators remove spaces after separators to reduce size.
    """
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return s.encode("utf-8")


async def write_frame(writer: asyncio.StreamWriter, obj: Any) -> None:
    """
    Send one framed JSON message:
      [4-byte big-endian length][JSON payload bytes]
    Always drain after writing to honor backpressure.
    """
    payload = _to_json_bytes(obj)
    n = len(payload)
    if n > MAX_FRAME_LEN:
        raise FrameTooLarge(f"payload {n} > MAX_FRAME_LEN {MAX_FRAME_LEN}")
    writer.write(_LEN.pack(n))
    writer.write(payload)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader) -> Any:
    """
    Read one framed JSON message and return the decoded Python object.
    May raise:
      - asyncio.IncompleteReadError: peer closed or truncated frame
      - FrameTooLarge: declared length exceeds MAX_FRAME_LEN
      - json.JSONDecodeError: payload is not valid JSON
    """
    # 1) Read the 4-byte length prefix (must read exactly 4 bytes).
    raw_len = await reader.readexactly(_LEN.size)
    (n,) = _LEN.unpack(raw_len)

    if n > MAX_FRAME_LEN:
        raise FrameTooLarge(f"frame length {n} > MAX_FRAME_LEN {MAX_FRAME_LEN}")
    if n == 0:
        # Zero-length frames are almost always a protocol mistake; reject early.
        raise ValueError("empty frame is not allowed")

    # 2) Read the JSON payload (exactly n bytes).
    data = await reader.readexactly(n)

    # 3) Decode UTF-8 JSON into a Python object.
    return json.loads(data.decode("utf-8"))


__all__ = ["MAX_FRAME_LEN", "FrameTooLarge", "write_frame", "read_frame"]