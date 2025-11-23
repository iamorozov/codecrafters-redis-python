"""
Redis Server Main Entry Point

Handles server initialization and client connections.
"""
import argparse
from app.handlers import *

import app.config as config
from app.resp_encoder import encode_array

import base64


EMPTY_RDB_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Redis Server")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on (default: 6379)")
    parser.add_argument("--replicaof", type=str, default=None, help="Master host and port (e.g., 'localhost 6379')")
    return parser.parse_args()


async def perform_handshake():
    """Perform handshake with master server when running as replica"""
    print(f"Connecting to master at {config.master_host}:{config.master_port}")

    reader, writer = await asyncio.open_connection(config.master_host, config.master_port)

    # Send PING
    ping_command = encode_array(["PING"])
    writer.write(ping_command)
    await writer.drain()
    response = await reader.read(1024)
    print(f"PING response: {response}")

    # Send REPLCONF listening-port
    replconf_command1 = encode_array(["REPLCONF", "listening-port", str(config.listening_port)])
    writer.write(replconf_command1)
    await writer.drain()
    response = await reader.read(1024)  # Wait for OK
    print(f"REPLCONF listening-port response: {response}")

    # Send REPLCONF capa
    replconf_command2 = encode_array(["REPLCONF", "capa", "psync2"])
    writer.write(replconf_command2)
    await writer.drain()
    response = await reader.read(1024)  # Wait for OK
    print(f"REPLCONF capa response: {response}")

    # Send PSYNC
    psync_command = encode_array(["PSYNC", "?", "-1"])
    writer.write(psync_command)
    await writer.drain()

    response = await read_simple_string(reader)
    print(f"PSYNC response: {response}")

    rdb_data = await read_rdb_file(reader)
    print(f"Received RDB file: {len(rdb_data)} bytes")

    # use handle connection to listen for write operations on master
    await handle_connection(reader, writer, is_replica=True)


async def read_simple_string(reader) -> str:
    """Read a RESP simple string (+OK\r\n) or error (-ERR\r\n)"""
    line = await reader.readline()  # Reads until \r\n
    return line.decode().strip()


async def read_rdb_file(reader) -> bytes:
    """Read RDB file in bulk string format: $<length>\r\n<data>"""
    # Read the length line: $<length>\r\n
    length_line = await reader.readline()

    if not length_line.startswith(b'$'):
      raise ValueError(f"Expected bulk string, got: {length_line}")

    # Parse the length
    length = int(length_line[1:].strip())

    # Read exactly 'length' bytes
    rdb_data = await reader.readexactly(length)

    return rdb_data


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

    # Perform handshake if running as replica
    if config.server_role == "slave":
        await perform_handshake()

    # Serve forever
    async with server:
        await server.serve_forever()


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, is_replica = False):
    """
    Handle a client connection using asyncio streams

    Args:
        reader: Async stream reader for receiving data
        writer: Async stream writer for sending data
        :param is_replica:
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
            if response and not is_replica:
                writer.write(response)
                await writer.drain()  # Ensure data is sent

            # send replication file to client
            if isinstance(command, PsyncCommand):
                empty_rdb = base64.b64decode(EMPTY_RDB_BASE64)
                writer.write(b"$" + str(len(empty_rdb)).encode('utf-8') + b"\r\n" + empty_rdb)
                await writer.drain()
                # save replica writer to use for replication
                config.replica_streams.append((reader, writer))

            # if master and write operation use replica writers to propagate the operation
            if config.server_role == "master" and isinstance(command, SetCommand):
                for _, write in config.replica_streams:
                    write.write(data)

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
    if args.replicaof:
        config.server_role = "slave"
        # Parse "host port" format
        parts = args.replicaof.split()
        config.master_host = parts[0]
        config.master_port = int(parts[1])
        config.listening_port = args.port

    if config.server_role == "master":
        config.replication_id = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'

    asyncio.run(main(args.port))
