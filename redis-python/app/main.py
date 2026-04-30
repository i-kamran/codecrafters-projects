import asyncio
import time
from typing import Callable
from collections import defaultdict

store: dict[str, tuple[str | list[str], float | None]] = {}

# key -> list of asyncio.Event waiters (in arrival order)
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
    except (IndexError, ValueError):
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


def get_store_value(key: str) -> str | list[str] | None:
    entry = store.get(key)
    if entry is None:
        return None
    value, expiry = entry
    if expiry is not None and time.time() > expiry:
        del store[key]
        return None
    return value


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
    elif isinstance(current, list):
        lst = current
    else:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    lst.extend(elements)
    store[key] = (lst, None)

    # Wake the longest-waiting BLPOP waiter if any
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
    elif isinstance(current, list):
        lst = current
    else:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    for element in elements:
        lst.insert(0, element)
    store[key] = (lst, None)

    # Wake the longest-waiting BLPOP waiter if any
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
    if not isinstance(current, list):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    n = len(current)
    if start < 0:
        start = max(0, n + start)
    if stop < 0:
        stop = n + stop

    sliced = current[start : stop + 1]
    return encode_array(sliced)


def cmd_llen(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'llen' command\r\n"
    value = get_store_value(args[1])
    if value is None:
        return encode_int(0)
    elif isinstance(value, list):
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
    if not isinstance(value, list):
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    lst: list[str] = value

    if n is None:
        popped = lst.pop(0)
        store[key] = (lst, None)
        return encode_bulk_string(popped)
    else:
        popped_many: list[str] = lst[:n]
        store[key] = (lst[n:], None)
        return encode_array(popped_many)


def notify_waiter(key: str) -> None:
    """Wake the oldest BLPOP waiter for this key, if any."""
    if waiters[key]:
        queue = waiters[key][0]  # oldest waiter first
        queue.put_nowait(key)


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

    # Pop immediately if element already available
    value = get_store_value(key)
    if isinstance(value, list) and value:
        element = value.pop(0)
        store[key] = (value, None)
        writer.write(encode_array([key, element]))
        return

    # Otherwise block — register as a waiter
    queue: asyncio.Queue = asyncio.Queue()
    waiters[key].append(queue)

    try:
        wait = queue.get()
        await asyncio.wait_for(wait, timeout=timeout if timeout > 0 else None)

        # Woken up — pop the element
        value = get_store_value(key)
        if isinstance(value, list) and value:
            element = value.pop(0)
            store[key] = (value, None)
            writer.write(encode_array([key, element]))
        else:
            writer.write(b"*-1\r\n")

    except asyncio.TimeoutError:
        writer.write(b"*-1\r\n")
    finally:
        waiters[key].remove(queue)


# --- Command registry --------------------------------------------------------

# BLPOP is async so handled separately in handle_client
COMMANDS: dict[str, Callable[[list[str]], bytes]] = {
    "PING":   cmd_ping,
    "ECHO":   cmd_echo,
    "SET":    cmd_set,
    "GET":    cmd_get,
    "RPUSH":  cmd_rpush,
    "LPUSH":  cmd_lpush,
    "LRANGE": cmd_lrange,
    "LLEN":   cmd_llen,
    "LPOP":   cmd_lpop,
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
