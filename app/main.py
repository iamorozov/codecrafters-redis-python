import socket  # noqa: F401
import threading


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

        # Parse and handle the command
        command_str = data.decode('utf-8').upper()

        if 'PING' in command_str:
            response = b"+PONG\r\n"
            client_socket.send(response)
            print("Sent: +PONG")


if __name__ == "__main__":
    main()
