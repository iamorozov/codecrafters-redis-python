import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Create server socket on port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Redis server listening on port 6379")

    # Handle multiple client connections in a loop
    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address}")

        try:
            # Receive data from client
            data = client_socket.recv(1024)
            print(f"Received: {data}")

            if data:
                # Parse the command - simple parsing for PING
                command_str = data.decode('utf-8').upper()

                # Check if it's a PING command
                if 'PING' in command_str:
                    # Send PONG response in RESP format
                    response = b"+PONG\r\n"
                    client_socket.send(response)
                    print("Sent: +PONG")

        except Exception as e:
            print(f"Error handling client: {e}")

        finally:
            # Clean up the connection
            client_socket.close()
            print("Client connection closed")


if __name__ == "__main__":
    main()
