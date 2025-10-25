import asyncio
import time
from app.resp_parser import (
    parse_command,
    PingCommand,
    EchoCommand,
    SetCommand,
    GetCommand,
    RpushCommand,
    LrangeCommand,
    CommandError,
    encode_simple_string,
    encode_bulk_string,
    encode_null,
    encode_error, encode_integer, encode_array
)

store = {}


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

def handle_ping(command: PingCommand) -> bytes:
    """Handle PING command - returns PONG"""
    print("Sent: +PONG")
    return encode_simple_string("PONG")


def handle_echo(command: EchoCommand) -> bytes:
    """Handle ECHO command - returns the message as a bulk string"""
    print(f"Sent: {command.message}")
    return encode_bulk_string(command.message)


def handle_set(command: SetCommand) -> bytes:
    """Handle SET command - stores key-value pair with optional expiry"""
    now = round(time.time() * 1000)
    expiry = None

    if command.expiry_ms is not None:
        expiry = now + command.expiry_ms

    store[command.key] = (command.value, expiry)
    print(f"Saved: {command.key}={command.value} (expiry: {expiry})")
    return encode_simple_string("OK")


def handle_get(command: GetCommand) -> bytes:
    """Handle GET command - retrieves value by key"""
    value, expiry = store.get(command.key, (None, None))
    now = round(time.time() * 1000)

    if value is None:
        response = encode_null()
    elif expiry is not None and expiry < now:
        # Key expired, delete it
        del store[command.key]
        response = encode_null()
    else:
        response = encode_bulk_string(value)

    print(f"Sent: {value}")
    return response


def handle_rpush(command: RpushCommand) -> bytes:
    """Handle RPUSH command - adds value to the list"""

    store[command.list_key] = store.get(command.list_key, []) + command.values
    print(f"Saved: {command.list_key}={store[command.list_key]}")
    return encode_integer(len(store[command.list_key]))


def handle_lrange(command: LrangeCommand) -> bytes:
    """Handle LRANGE command - retrieves values from a list"""

    result = store.get(command.list_key, [])[command.start : command.stop + 1]
    print(f"Retrieved: {command.list_key}={result}")
    return encode_array([encode_bulk_string(x) for x in result])


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

            # Handle command errors
            if isinstance(command, CommandError):
                writer.write(encode_error(command.message))
                await writer.drain()
                print(f"Sent error: {command.message}")
                continue

            # Dispatch to appropriate handler
            response = None
            if isinstance(command, PingCommand):
                response = handle_ping(command)
            elif isinstance(command, EchoCommand):
                response = handle_echo(command)
            elif isinstance(command, SetCommand):
                response = handle_set(command)
            elif isinstance(command, GetCommand):
                response = handle_get(command)
            elif isinstance(command, RpushCommand):
                response = handle_rpush(command)
            elif isinstance(command, LrangeCommand):
                response = handle_lrange(command)
            else:
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
