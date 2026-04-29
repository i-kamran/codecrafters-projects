import asyncio
import time
from typing import Callable

store: dict[str, tuple[str, float | None]] = {}


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
            i += 1  # Skipping length of the line
            args.append(lines[i].decode())
            i += 1
        return args
    except IndexError, ValueError:
        return None


def encode_bulk_string(arg: str) -> bytes:
    encoded = arg.encode()
    length = str(len(encoded)).encode()
    return b"$" + length + b"\r\n" + encoded + b"\r\n"


# --- Command helpers ---------------------------------------------------------


def cmd_ping(args: list[str]) -> bytes:
    return b"+PONG\r\n"


def cmd_echo(args: list[str]) -> bytes:
    if len(args) != 2:
        return b"-ERR wrong number of argument for 'echo' command\r\n"
    return encode_bulk_string(args[1])


def cmd_set(args: list[str]) -> bytes:
    if len(args) < 3:
        return b"-ERR wrong number of arguments for `set` command\r\n"

    key = args[1]
    value = args[2]

    i = 3
    expiry: float | None = None

    while i < len(args):
        option = args[i].upper()

        if option in ("EX", "PX"):
            if i + 1 >= len(args):
                return b"-ERR syntax error"
            if option == "EX":
                expiry = time.time() + float(args[i + 1])
            else:
                expiry = time.time() + float(args[i + 1]) / 1000

            i += 2

        else:
            return b"-ERR wrong number of arguments for `set` command\r\n"

    store[key] = (value, expiry)

    return b"+OK\r\n"


def cmd_get(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for `get` command\r\n"
    entry = store.get(args[1])
    if entry is None:
        return b"$-1\r\n"
    value, expiry = entry
    if expiry is not None and time.time() > expiry:
        del store[args[1]]
        return b"$-1\r\n"

    return encode_bulk_string(value)

def cmd_rpush(args: list[str])-> bytes:
    return b""



# --- Command registry --------------------------------------------------------

COMMANDS: dict[str, Callable[[list[str]], bytes]] = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
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
            response = handle_command(args) if args else b"-ERR invalid input\r\n"
            writer.write(response)
            await writer.drain()

    except ConnectionResetError:
        print("Client disconnected")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server = await asyncio.start_server(
        handle_client, host="localhost", port=6379, reuse_port=True
    )
    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

