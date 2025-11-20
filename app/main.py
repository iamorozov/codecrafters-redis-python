"""
Redis Server Main Entry Point

Handles server initialization and client connections.
"""
import argparse
from app.handlers import *


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Redis Server")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on (default: 6379)")
    parser.add_argument("--replicaof", type=str, default=None, help="Master host and port (e.g., 'localhost 6379')")
    return parser.parse_args()


async def main(port: int):
    """Main entry point - starts the asyncio event loop and server"""
    print("Logs from your program will appear here!")

    # Create asyncio server
    server = await asyncio.start_server(
        handle_connection,
        host="localhost",
        port=port,
        reuse_port=True
    )

    print(f"Redis server listening on port {port}")

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
    transaction_queue = None
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

            # handle transactions
            if isinstance(command, MultiCommand):
                transaction_queue = []
                response = handle_multi(command)
            elif isinstance(command, ExecCommand):
                response = await handle_exec(command, transaction_queue)
                transaction_queue = None
            elif isinstance(command, DiscardCommand):
                response = handle_discard(command, transaction_queue)
                transaction_queue = None
            elif transaction_queue is not None:
                transaction_queue.append(command)
                response = encode_simple_string('QUEUED')
            else:
                response = await handle_command(command)

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
    args = parse_args()

    # Set server role based on --replicaof argument
    import app.config as config
    if args.replicaof:
        config.server_role = "slave"
        # Parse "host port" format
        parts = args.replicaof.split()
        config.master_host = parts[0]
        config.master_port = int(parts[1])

    asyncio.run(main(args.port))
