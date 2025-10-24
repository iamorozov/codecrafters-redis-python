import socket  # noqa: F401
import threading
import time
from app.resp_parser import (
    parse_command,
    PingCommand,
    EchoCommand,
    SetCommand,
    GetCommand,
    CommandError
)

store = {}

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Create server socket on port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Redis server listening on port 6379")

    threads = []

    while True:
        try:
            client_socket, address = server_socket.accept()
            print(f"Client connected from {address}")

            t = threading.Thread(target=handle_connection, args=(client_socket,))
            threads.append(t)
            t.start()

        except Exception as e:
            print(f"Error handling client: {e}")
            break

def handle_connection(client_socket):
    while True:
        data = client_socket.recv(1024)
        print(f"Received: {data}")

        if not data:
            # Client disconnected (empty data means connection closed)
            print("Client disconnected")
            client_socket.close()
            break

        # Parse and validate the RESP command
        command = parse_command(data)
        print(f"Parsed command: {command}")

        # Handle command errors
        if isinstance(command, CommandError):
            error = f"-ERR {command.message}\r\n"
            client_socket.send(error.encode('utf-8'))
            print(f"Sent error: {command.message}")
            continue

        # Handle PING command
        if isinstance(command, PingCommand):
            response = b"+PONG\r\n"
            client_socket.send(response)
            print("Sent: +PONG")

        # Handle ECHO command
        elif isinstance(command, EchoCommand):
            # Send as bulk string: $<length>\r\n<data>\r\n
            response = f"${len(command.message)}\r\n{command.message}\r\n".encode('utf-8')
            client_socket.send(response)
            print(f"Sent: {command.message}")

        # Handle SET command
        elif isinstance(command, SetCommand):
            now = round(time.time() * 1000)
            expiry = None

            if command.expiry_ms is not None:
                expiry = now + command.expiry_ms

            store[command.key] = (command.value, expiry)
            response = b"+OK\r\n"
            client_socket.send(response)
            print(f"Saved: {command.key}={command.value} (expiry: {expiry})")

        # Handle GET command
        elif isinstance(command, GetCommand):
            null = b"$-1\r\n"
            value, expiry = store.get(command.key, (None, None))
            now = round(time.time() * 1000)

            if value is None:
                response = null
            elif expiry is not None and expiry < now:
                # Key expired, delete it
                del store[command.key]
                response = null
            else:
                response = f"${len(value)}\r\n{value}\r\n".encode('utf-8')

            client_socket.send(response)
            print(f"Sent: {value}")


if __name__ == "__main__":
    main()
