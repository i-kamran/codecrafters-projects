import asyncio


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    print(f"Client connected: {addr}")
    
    try:
        while True:
            query = await reader.read(1024)
            if not query:
                break
            writer.write(b"+PONG\r\n")
            await writer.drain()
    except ConnectionResetError:
        print(f"Client disconnected: {addr}")
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(
        handle_client,
        host="localhost",
        port=6379,
        reuse_port=True
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
