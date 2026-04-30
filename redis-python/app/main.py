import argparse
import asyncio
import secrets
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

# ── Config ────────────────────────────────────────────────────────────────────


@dataclass
class ServerConfig:
    port: int = 6379
    replicaof: tuple[str, int] | None = None
    master_replid: str = field(default_factory=lambda: secrets.token_hex(20))
    master_repl_offset: int = 0

    @property
    def role(self) -> str:
        return "slave" if self.replicaof else "master"


config = ServerConfig()

# ── Store & dirty tracking ────────────────────────────────────────────────────

StreamEntry = tuple[str, list[str]]
store: dict[str, tuple[str | list[str] | list[StreamEntry], float | None]] = {}
dirty_keys: dict[str, int] = {}

waiters: dict[str, list[asyncio.Queue]] = defaultdict(list)
stream_waiters: dict[str, list[asyncio.Queue]] = defaultdict(list)


def mark_dirty(key: str) -> None:
    dirty_keys[key] = dirty_keys.get(key, 0) + 1


# ── RESP helpers ──────────────────────────────────────────────────────────────


def parse_resp(msg: bytes) -> list[str] | None:
    try:
        lines = msg.split(b"\r\n")
        if not lines[0].startswith(b"*"):
            return None
        count = int(lines[0][1:])
        args, i = [], 1
        for _ in range(count):
            if not lines[i].startswith(b"$"):
                return None
            i += 1
            args.append(lines[i].decode())
            i += 1
        return args
    except IndexError, ValueError:
        return None


def encode_bulk_string(arg: str) -> bytes:
    encoded = arg.encode()
    return b"$" + str(len(encoded)).encode() + b"\r\n" + encoded + b"\r\n"


def encode_array(items: list[str]) -> bytes:
    if not items:
        return b"*0\r\n"
    return (
        b"*"
        + str(len(items)).encode()
        + b"\r\n"
        + b"".join(encode_bulk_string(i) for i in items)
    )


def encode_int(value: int) -> bytes:
    return b":" + str(value).encode() + b"\r\n"


def encode_stream_entries(entries: list[StreamEntry]) -> bytes:
    result = b"*" + str(len(entries)).encode() + b"\r\n"
    for entry_id, fields in entries:
        result += b"*2\r\n"
        result += encode_bulk_string(entry_id)
        result += encode_array(fields)
    return result


# ── Store helpers ─────────────────────────────────────────────────────────────


def get_store_value(key: str) -> str | list[str] | list[StreamEntry] | None:
    entry = store.get(key)
    if entry is None:
        return None
    value, expiry = entry
    if expiry is not None and time.time() > expiry:
        del store[key]
        return None
    return value


def is_stream(value: object) -> bool:
    return isinstance(value, list) and len(value) > 0 and isinstance(value[0], tuple)


def parse_entry_id(entry_id: str) -> tuple[int, int] | None:
    try:
        ms, seq = entry_id.split("-")
        return int(ms), int(seq)
    except ValueError:
        return None


def parse_entry_id_range(entry_id: str, default_seq: int) -> tuple[int, int]:
    if "-" in entry_id:
        ms, seq = entry_id.split("-", 1)
        return int(ms), int(seq)
    return int(entry_id), default_seq


# ── Command handlers ──────────────────────────────────────────────────────────


def cmd_ping(args: list[str]) -> bytes:
    return b"+PONG\r\n"


def cmd_echo(args: list[str]) -> bytes:
    if len(args) != 2:
        return b"-ERR wrong number of arguments for 'echo' command\r\n"
    return encode_bulk_string(args[1])


def cmd_set(args: list[str]) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'set' command\r\n"
    key, value = args[1], args[2]
    expiry: float | None = None
    i = 3
    while i < len(args):
        option = args[i].upper()
        if option in ("EX", "PX"):
            if i + 1 >= len(args):
                return b"-ERR syntax error\r\n"
            try:
                expiry = time.time() + float(args[i + 1])
                if option == "PX":
                    expiry = time.time() + float(args[i + 1]) / 1000
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            i += 2
        else:
            return b"-ERR syntax error\r\n"
    store[key] = (value, expiry)
    mark_dirty(key)
    notify_waiter(key)
    return b"+OK\r\n"


def cmd_get(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'get' command\r\n"
    value = get_store_value(args[1])
    if value is None:
        return b"$-1\r\n"
    if not isinstance(value, str):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    return encode_bulk_string(value)


def cmd_incr(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'incr' command\r\n"
    key = args[1]
    value = get_store_value(key)
    if value is None:
        store[key] = ("1", None)
        mark_dirty(key)
        return encode_int(1)
    if not isinstance(value, str):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    try:
        new_value = int(value) + 1
    except ValueError:
        return b"-ERR value is not an integer or out of range\r\n"
    store[key] = (str(new_value), None)
    mark_dirty(key)
    return encode_int(new_value)


def cmd_rpush(args: list[str]) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'rpush' command\r\n"
    key = args[1]
    elements = args[2:]
    current = get_store_value(key)
    if current is None:
        lst: list[str] = []
    elif isinstance(current, list) and not is_stream(current):
        lst = current  # type: ignore
    else:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    lst.extend(elements)
    store[key] = (lst, None)
    mark_dirty(key)
    notify_waiter(key)
    return encode_int(len(lst))


def cmd_lpush(args: list[str]) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for 'lpush' command\r\n"
    key = args[1]
    elements = args[2:]
    current = get_store_value(key)
    if current is None:
        lst: list[str] = []
    elif isinstance(current, list) and not is_stream(current):
        lst = current  # type: ignore
    else:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    for element in elements:
        lst.insert(0, element)
    store[key] = (lst, None)
    mark_dirty(key)
    notify_waiter(key)
    return encode_int(len(lst))


def cmd_lrange(args: list[str]) -> bytes:
    if len(args) != 4:
        return b"-ERR wrong number of arguments for 'lrange' command\r\n"
    key = args[1]
    try:
        start = int(args[2])
        stop = int(args[3])
    except ValueError:
        return b"-ERR value is not integer or out of range\r\n"
    current = get_store_value(key)
    if current is None:
        return b"*0\r\n"
    if not isinstance(current, list) or is_stream(current):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    n = len(current)
    if start < 0:
        start = max(0, n + start)
    if stop < 0:
        stop = n + stop
    sliced = current[start : stop + 1]
    return encode_array(sliced)  # type: ignore


def cmd_llen(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'llen' command\r\n"
    value = get_store_value(args[1])
    if value is None:
        return encode_int(0)
    if isinstance(value, list) and not is_stream(value):
        return encode_int(len(value))
    return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"


def cmd_lpop(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'lpop' command\r\n"
    key = args[1]
    try:
        n = int(args[2]) if len(args) >= 3 else None
    except ValueError:
        return b"-ERR value is out of range, must be positive\r\n"
    value = get_store_value(key)
    if value is None:
        return b"$-1\r\n"
    if not isinstance(value, list) or is_stream(value):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    lst: list[str] = value  # type: ignore
    if n is None:
        popped = lst.pop(0)
        store[key] = (lst, None)
        mark_dirty(key)
        return encode_bulk_string(popped)
    else:
        popped_many = lst[:n]
        store[key] = (lst[n:], None)
        mark_dirty(key)
        return encode_array(popped_many)


def cmd_type(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'type' command\r\n"
    value = get_store_value(args[1])
    if value is None:
        return b"+none\r\n"
    if isinstance(value, str):
        return b"+string\r\n"
    if is_stream(value):
        return b"+stream\r\n"
    if isinstance(value, list):
        return b"+list\r\n"
    return b"+unknown\r\n"


def cmd_xadd(args: list[str]) -> bytes:
    if len(args) < 5 or len(args) % 2 == 0:
        return b"-ERR wrong number of arguments for 'xadd' command\r\n"
    key = args[1]
    entry_id = args[2]
    fields = args[3:]
    current = get_store_value(key)
    if current is None:
        stream: list[StreamEntry] = []
    elif is_stream(current):
        stream = current  # type: ignore
    else:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    last_ms, last_seq = parse_entry_id(stream[-1][0]) if stream else (0, -1)  # type: ignore

    if entry_id == "*":
        ms = int(time.time() * 1000)
        seq = (last_seq + 1) if ms == last_ms else 0
        entry_id = f"{ms}-{seq}"
    elif entry_id.endswith("-*"):
        try:
            ms = int(entry_id[:-2])
        except ValueError:
            return b"-ERR invalid stream ID\r\n"
        if ms < last_ms:
            return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        seq = (last_seq + 1) if ms == last_ms else 0
        if ms == 0 and seq == 0:
            seq = 1
        entry_id = f"{ms}-{seq}"
    else:
        parsed = parse_entry_id(entry_id)
        if parsed is None:
            return b"-ERR invalid stream ID\r\n"
        ms, seq = parsed
        if ms == 0 and seq == 0:
            return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
        if ms < last_ms or (ms == last_ms and seq <= last_seq):
            return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

    stream.append((entry_id, fields))
    store[key] = (stream, None)
    mark_dirty(key)
    notify_stream_waiters(key)
    return encode_bulk_string(entry_id)


def cmd_xrange(args: list[str]) -> bytes:
    if len(args) != 4:
        return b"-ERR wrong number of arguments for 'xrange' command\r\n"
    key = args[1]
    current = get_store_value(key)
    if current is None:
        return b"*0\r\n"
    if not is_stream(current):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    stream: list[StreamEntry] = current  # type: ignore
    try:
        start = (
            (0, 0) if args[2] == "-" else parse_entry_id_range(args[2], default_seq=0)
        )
        end = (
            (2**63, 2**63)
            if args[3] == "+"
            else parse_entry_id_range(args[3], default_seq=2**63)
        )
    except ValueError:
        return b"-ERR invalid stream ID\r\n"
    matched = [e for e in stream if start <= parse_entry_id(e[0]) <= end]  # type: ignore
    return encode_stream_entries(matched)


def xread_results(keys: list[str], ids: list[str]) -> bytes:
    result = b"*" + str(len(keys)).encode() + b"\r\n"
    for key, start_id in zip(keys, ids):
        current = get_store_value(key)
        try:
            start = parse_entry_id_range(start_id, default_seq=0)
        except ValueError:
            start = (0, 0)
        entries: list[StreamEntry] = []
        if current is not None and is_stream(current):
            entries = [e for e in current if parse_entry_id(e[0]) > start]  # type: ignore
        result += b"*2\r\n"
        result += encode_bulk_string(key)
        result += encode_stream_entries(entries)
    return result


def cmd_xread(args: list[str]) -> bytes:
    if len(args) < 4 or args[1].upper() != "STREAMS":
        return b"-ERR syntax error\r\n"
    after = args[2:]
    if len(after) % 2 != 0:
        return b"-ERR syntax error\r\n"
    mid = len(after) // 2
    return xread_results(after[:mid], after[mid:])


def cmd_info(args: list[str]) -> bytes:
    section = args[1].lower() if len(args) > 1 else "all"
    if section in ("replication", "all"):
        lines = [
            "# Replication",
            f"role:{config.role}",
            f"connected_slaves:0",
            f"master_replid:{config.master_replid}",
            f"master_repl_offset:{config.master_repl_offset}",
            "second_repl_offset:-1",
            "repl_backlog_active:0",
            "repl_backlog_size:1048576",
            "repl_backlog_first_byte_offset:0",
            "repl_backlog_histlen:0",
        ]
        return encode_bulk_string("\r\n".join(lines))
    return encode_bulk_string("")


# ── Waiter helpers ────────────────────────────────────────────────────────────


def notify_waiter(key: str) -> None:
    if waiters[key]:
        waiters[key][0].put_nowait(key)


def notify_stream_waiters(key: str) -> None:
    for q in stream_waiters[key]:
        q.put_nowait(key)


# ── Async command handlers ────────────────────────────────────────────────────


async def cmd_blpop(args: list[str], writer: asyncio.StreamWriter) -> None:
    if len(args) < 3:
        writer.write(b"-ERR wrong number of arguments for 'blpop' command\r\n")
        return
    key = args[1]
    try:
        timeout = float(args[-1])
    except ValueError:
        writer.write(b"-ERR timeout is not a float or out of range\r\n")
        return
    value = get_store_value(key)
    if isinstance(value, list) and not is_stream(value) and value:
        lst: list[str] = value  # type: ignore
        element = lst.pop(0)
        store[key] = (lst, None)
        writer.write(encode_array([key, element]))
        return
    q: asyncio.Queue = asyncio.Queue()
    waiters[key].append(q)
    try:
        await asyncio.wait_for(q.get(), timeout=timeout if timeout > 0 else None)
        value = get_store_value(key)
        if isinstance(value, list) and not is_stream(value) and value:
            lst = value  # type: ignore
            element = lst.pop(0)
            store[key] = (lst, None)
            writer.write(encode_array([key, element]))
        else:
            writer.write(b"*-1\r\n")
    except asyncio.TimeoutError:
        writer.write(b"*-1\r\n")
    finally:
        waiters[key].remove(q)


async def cmd_xread_block(args: list[str], writer: asyncio.StreamWriter) -> None:
    try:
        timeout_ms = float(args[2])
    except ValueError, IndexError:
        writer.write(b"-ERR timeout is not a float or out of range\r\n")
        return
    streams_idx = next((i for i, a in enumerate(args) if a.upper() == "STREAMS"), None)
    if streams_idx is None:
        writer.write(b"-ERR syntax error\r\n")
        return
    after = args[streams_idx + 1 :]
    if len(after) % 2 != 0:
        writer.write(b"-ERR syntax error\r\n")
        return
    mid = len(after) // 2
    keys = after[:mid]
    ids = after[mid:]
    resolved = []
    for key, id_ in zip(keys, ids):
        if id_ == "$":
            current = get_store_value(key)
            resolved.append(current[-1][0] if current and is_stream(current) else "0-0")  # type: ignore
        else:
            resolved.append(id_)
    for key, start_id in zip(keys, resolved):
        current = get_store_value(key)
        if current and is_stream(current):
            start = parse_entry_id_range(start_id, default_seq=0)
            if any(parse_entry_id(e[0]) > start for e in current):  # type: ignore
                writer.write(xread_results(keys, resolved))
                return
    q: asyncio.Queue = asyncio.Queue()
    for key in keys:
        stream_waiters[key].append(q)
    try:
        timeout = (timeout_ms / 1000) if timeout_ms > 0 else None
        await asyncio.wait_for(q.get(), timeout=timeout)
        writer.write(xread_results(keys, resolved))
    except asyncio.TimeoutError:
        writer.write(b"*-1\r\n")
    finally:
        for key in keys:
            stream_waiters[key].remove(q)


# ── Transaction & connection state ────────────────────────────────────────────


@dataclass
class Transaction:
    queue: list[list[str]] = field(default_factory=list)
    watched_keys: set[str] = field(default_factory=set)
    dirty_snapshot: dict[str, int] = field(default_factory=dict)
    active: bool = False

    def is_dirty(self) -> bool:
        return any(
            dirty_keys.get(k, 0) != self.dirty_snapshot.get(k, 0)
            for k in self.watched_keys
        )


@dataclass
class ConnectionState:
    tx: Transaction | None = None

    @property
    def in_multi(self) -> bool:
        return self.tx is not None and self.tx.active

    def watch(self, keys: list[str]) -> bytes:
        if self.in_multi:
            return b"-ERR WATCH inside MULTI is not allowed\r\n"
        if self.tx is None:
            self.tx = Transaction()
        self.tx.watched_keys.update(keys)
        for k in keys:
            self.tx.dirty_snapshot[k] = dirty_keys.get(k, 0)
        return b"+OK\r\n"

    def unwatch(self) -> bytes:
        if self.tx is not None:
            self.tx.watched_keys.clear()
            self.tx.dirty_snapshot.clear()

        return b"+OK\r\n"  # always OK, even if nothing was watched

    def multi(self) -> bytes:
        if self.in_multi:
            return b"-ERR MULTI calls can not be nested\r\n"
        if self.tx is None:
            self.tx = Transaction()
        self.tx.active = True
        return b"+OK\r\n"

    def queue_cmd(self, args: list[str]) -> bytes:
        self.tx.queue.append(args)  # type: ignore
        return b"+QUEUED\r\n"

    def exec(self) -> bytes:
        if not self.in_multi:
            return b"-ERR EXEC without MULTI\r\n"
        tx, self.tx = self.tx, None
        if tx.is_dirty():  # type: ignore
            return b"*-1\r\n"
        responses = b"*" + str(len(tx.queue)).encode() + b"\r\n"  # type: ignore
        for queued_args in tx.queue:  # type: ignore
            responses += dispatch_sync(queued_args)
        return responses

    def discard(self) -> bytes:
        if not self.in_multi:
            return b"-ERR DISCARD without MULTI\r\n"
        self.tx = None
        return b"+OK\r\n"


# ── Command registry ──────────────────────────────────────────────────────────


@dataclass
class Command:
    handler: Callable[[ConnectionState, list[str]], bytes]
    always: bool = False  # if True, bypasses transaction queue


def _s(
    fn: Callable[[list[str]], bytes],
) -> Callable[[ConnectionState, list[str]], bytes]:
    """Wrap a stateless handler to fit the (state, args) signature."""
    return lambda state, args: fn(args)


ALL_COMMANDS: dict[str, Command] = {
    # Transaction control — always run immediately
    "EXEC": Command(lambda state, args: state.exec(), always=True),
    "DISCARD": Command(lambda state, args: state.discard(), always=True),
    # Connection state commands — queued inside MULTI, run outside
    "WATCH": Command(lambda state, args: state.watch(args[1:]), always=True),
    "UNWATCH": Command(lambda state, args: state.unwatch(), always=True),
    "MULTI": Command(lambda state, args: state.multi()),
    # Regular stateless commands
    "PING": Command(_s(cmd_ping)),
    "ECHO": Command(_s(cmd_echo)),
    "SET": Command(_s(cmd_set)),
    "GET": Command(_s(cmd_get)),
    "INCR": Command(_s(cmd_incr)),
    "RPUSH": Command(_s(cmd_rpush)),
    "LPUSH": Command(_s(cmd_lpush)),
    "LRANGE": Command(_s(cmd_lrange)),
    "LLEN": Command(_s(cmd_llen)),
    "LPOP": Command(_s(cmd_lpop)),
    "TYPE": Command(_s(cmd_type)),
    "XADD": Command(_s(cmd_xadd)),
    "XRANGE": Command(_s(cmd_xrange)),
    "XREAD": Command(_s(cmd_xread)),
    "INFO": Command(_s(cmd_info)),
}


def dispatch_sync(args: list[str]) -> bytes:
    """Dispatch a command without connection state (used inside EXEC)."""
    entry = ALL_COMMANDS.get(args[0].upper())
    if entry is None:
        return b"-ERR unknown command\r\n"
    return entry.handler(ConnectionState(), args)


# ── Networking ────────────────────────────────────────────────────────────────


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    state = ConnectionState()

    try:
        while True:
            msg = await reader.read(1024)
            if not msg:
                break
            args = parse_resp(msg)
            if not args:
                writer.write(b"-ERR invalid input\r\n")
                await writer.drain()
                continue

            cmd = args[0].upper()

            if cmd == "BLPOP":
                await cmd_blpop(args, writer)
            elif cmd == "XREAD" and len(args) > 2 and args[1].upper() == "BLOCK":
                await cmd_xread_block(args, writer)
            else:
                entry = ALL_COMMANDS.get(cmd)
                if entry is None:
                    writer.write(b"-ERR unknown command\r\n")
                elif state.in_multi and not entry.always:
                    writer.write(state.queue_cmd(args))
                else:
                    writer.write(entry.handler(state, args))

            await writer.drain()

    except ConnectionResetError:
        print("Client disconnected")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default=None)
    cli_args = parser.parse_args()

    config.port = cli_args.port
    if cli_args.replicaof:
        host, port = cli_args.replicaof.split()
        config.replicaof = (host, int(port))

    print("Logs from your program will appear here!")
    server = await asyncio.start_server(
        handle_client, host="localhost", port=config.port, reuse_port=True
    )
    print(f"Serving on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

