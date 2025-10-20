import socket  # noqa: F401
import threading
from app.resp_parser import parse_command


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

        # Parse the RESP command
        command = parse_command(data)
        print(f"Parsed command: {command}")

        if command:
            # Handle different commands
            if command[0] == 'PING':
                response = b"+PONG\r\n"
                client_socket.send(response)
                print("Sent: +PONG")
            elif command[0] == 'ECHO':
                # ECHO command returns the argument as a bulk string
                if len(command) < 2:
                    error = b"-ERR wrong number of arguments for 'echo' command\r\n"
                    client_socket.send(error)
                    print("Sent error: ECHO requires argument")
                else:
                    message = command[1]
                    # Send as bulk string: $<length>\r\n<data>\r\n
                    response = f"${len(message)}\r\n{message}\r\n".encode('utf-8')
                    client_socket.send(response)
                    print(f"Sent: {message}")
            else:
                # Unknown command
                error = f"-ERR unknown command '{command[0]}'\r\n"
                client_socket.send(error.encode('utf-8'))
                print(f"Sent error: unknown command")


if __name__ == "__main__":
    main()
