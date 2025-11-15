"""
Redis Server Main Entry Point

Handles server initialization and client connections.
"""
from app.handlers import *


async def main():
    """Main entry point - starts the asyncio event loop and server"""
    print("Logs from your program will appear here!")

    # Create asyncio server on port 6379
    server = await asyncio.start_server(
        handle_connection,
        host="localhost",
        port=6379,
        reuse_port=True
    )

    print("Redis server listening on port 6379")

    # Serve forever
    async with server:
        await server.serve_forever()


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    Handle a client connection using asyncio streams

    Args:
        reader: Async stream reader for receiving data
        writer: Async stream writer for sending data
    """
    address = writer.get_extra_info('peername')
    print(f"Client connected from {address}")

    try:
        while True:
            # Asynchronously read data from client
            data = await reader.read(1024)
            print(f"Received: {data}")

            if not data:
                # Client disconnected (empty data means connection closed)
                print("Client disconnected")
                break

            # Parse and validate the RESP command
            command = parse_command(data)
            print(f"Parsed command: {command}")

            # Dispatch to appropriate handler using pattern matching
            match command:
                case CommandError():
                    writer.write(encode_error(command.message))
                    await writer.drain()
                    print(f"Sent error: {command.message}")
                    continue
                case PingCommand():
                    response = handle_ping(command)
                case EchoCommand():
                    response = handle_echo(command)
                case SetCommand():
                    response = handle_set(command)
                case GetCommand():
                    response = handle_get(command)
                case RpushCommand():
                    response = handle_rpush(command)
                case LpushCommand():
                    response = handle_lpush(command)
                case LrangeCommand():
                    response = handle_lrange(command)
                case LlenCommand():
                    response = handle_llen(command)
                case LpopCommand():
                    response = handle_lpop(command)
                case BlpopCommand():
                    response = await handle_blpop(command)
                case TypeCommand():
                    response = handle_type(command)
                case XaddCommand():
                    response = handle_xadd(command)
                case XrangeCommand():
                    response = handle_xrange(command)
                case XreadCommand():
                    response = await handle_xread(command)
                case IncrCommand():
                    response = handle_incr(command)
                case MultiCommand():
                    response = handle_multi(command)
                case _:
                    writer.write(encode_error("Unknown command"))
                    await writer.drain()
                    print(f"Got unknown command: {command}")
                    continue

            # Send response to client
            if response:
                writer.write(response)
                await writer.drain()  # Ensure data is sent

    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        # Clean up the connection
        writer.close()
        await writer.wait_closed()
        print(f"Connection closed for {address}")


if __name__ == "__main__":
    asyncio.run(main())
