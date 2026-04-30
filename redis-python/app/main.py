import asyncio
import time
from collections import defaultdict
from typing import Callable

# Stream entry: (id, fields) where fields is a list of alternating key-value strings
StreamEntry = tuple[str, list[str]]

store: dict[str, tuple[str | list[str] | list[StreamEntry], float | None]] = {}
waiters: dict[str, list[asyncio.Queue]] = defaultdict(list)


# --- RESP helpers ------------------------------------------------------------
def parse_resp(msg: bytes) -> list[str] | None:
    try:
        lines = msg.split(b"\r\n")
        if not lines[0].startswith(b"*"):
            return None

        count = int(lines[0][1:])
        args = []
        i = 1

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
    header = b"*" + str(len(items)).encode() + b"\r\n"
    return header + b"".join(encode_bulk_string(item) for item in items)


def encode_int(value: int) -> bytes:
    return b":" + str(value).encode() + b"\r\n"


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
    """Check if a value is a stream (list of StreamEntry tuples)."""
    return isinstance(value, list) and len(value) > 0 and isinstance(value[0], tuple)


def parse_entry_id(entry_id: str) -> tuple[int, int] | None:
    """Parse 'ms-seq' into (ms, seq). Returns None if invalid format."""
    try:
        ms, seq = entry_id.split("-")
        return int(ms), int(seq)
    except ValueError:
        return None


# --- Command handlers --------------------------------------------------------


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
    elif isinstance(value, list) and not is_stream(value):
        return encode_int(len(value))
    else:
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
        return encode_bulk_string(popped)
    else:
        popped_many = lst[:n]
        store[key] = (lst[n:], None)
        return encode_array(popped_many)


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
        # Auto-generate both ms and seq
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

        # 0-* minimum is 0-1
        if ms == 0 and seq == 0:
            seq = 1

        entry_id = f"{ms}-{seq}"

    else:
        # Explicit ID — validate
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
    return encode_bulk_string(entry_id)


def cmd_type(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'type' command\r\n"
    value = get_store_value(args[1])
    if value is None:
        return b"+none\r\n"
    elif isinstance(value, str):
        return b"+string\r\n"
    elif is_stream(value):
        return b"+stream\r\n"
    elif isinstance(value, list):
        return b"+list\r\n"
    else:
        return b"+unknown\r\n"


def notify_waiter(key: str) -> None:
    if waiters[key]:
        waiters[key][0].put_nowait(key)


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

    queue: asyncio.Queue = asyncio.Queue()
    waiters[key].append(queue)

    try:
        await asyncio.wait_for(queue.get(), timeout=timeout if timeout > 0 else None)
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
        waiters[key].remove(queue)


# --- Command registry --------------------------------------------------------

COMMANDS: dict[str, Callable[[list[str]], bytes]] = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
    "RPUSH": cmd_rpush,
    "LPUSH": cmd_lpush,
    "LRANGE": cmd_lrange,
    "LLEN": cmd_llen,
    "LPOP": cmd_lpop,
    "TYPE": cmd_type,
    "XADD": cmd_xadd,
}


def handle_command(args: list[str]) -> bytes:
    handler = COMMANDS.get(args[0].upper())
    if handler is None:
        return b"-ERR unknown command\r\n"
    return handler(args)


# --- Networking --------------------------------------------------------------


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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

            if args[0].upper() == "BLPOP":
                await cmd_blpop(args, writer)
            else:
                writer.write(handle_command(args))

            await writer.drain()

    except ConnectionResetError:
        print("Client disconnected")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(
        handle_client, host="localhost", port=6379, reuse_port=True
    )
    print(f"Serving on {server.sockets[0].getsockname()}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

