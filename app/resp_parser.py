"""
RESP (Redis Serialization Protocol) Parser

RESP types:
- Simple Strings: +OK\r\n
- Errors: -Error message\r\n
- Integers: :1000\r\n
- Bulk Strings: $6\r\nfoobar\r\n (length followed by data)
- Arrays: *2\r\n$4\r\nPING\r\n (count followed by elements)
"""


class RESPParser:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def parse(self):
        """Parse RESP data and return Python objects"""
        if self.pos >= len(self.data):
            return None

        type_byte = chr(self.data[self.pos])
        self.pos += 1

        if type_byte == '+':
            return self._parse_simple_string()
        elif type_byte == '-':
            return self._parse_error()
        elif type_byte == ':':
            return self._parse_integer()
        elif type_byte == '$':
            return self._parse_bulk_string()
        elif type_byte == '*':
            return self._parse_array()
        else:
            raise ValueError(f"Unknown RESP type: {type_byte}")

    def _read_until_crlf(self) -> bytes:
        """Read until \r\n and return the content (without \r\n)"""
        start = self.pos
        while self.pos < len(self.data) - 1:
            if self.data[self.pos] == ord('\r') and self.data[self.pos + 1] == ord('\n'):
                result = self.data[start:self.pos]
                self.pos += 2  # Skip \r\n
                return result
            self.pos += 1
        raise ValueError("CRLF not found")

    def _parse_simple_string(self) -> str:
        """Parse simple string: +OK\r\n"""
        return self._read_until_crlf().decode('utf-8')

    def _parse_error(self) -> str:
        """Parse error: -Error message\r\n"""
        return self._read_until_crlf().decode('utf-8')

    def _parse_integer(self) -> int:
        """Parse integer: :1000\r\n"""
        return int(self._read_until_crlf())

    def _parse_bulk_string(self) -> str | None:
        """Parse bulk string: $6\r\nfoobar\r\n or $-1\r\n (null)"""
        length = int(self._read_until_crlf())

        if length == -1:
            return None  # Null bulk string

        if self.pos + length > len(self.data):
            raise ValueError("Not enough data for bulk string")

        result = self.data[self.pos:self.pos + length].decode('utf-8')
        self.pos += length

        # Skip trailing \r\n
        if self.pos + 2 <= len(self.data):
            if self.data[self.pos:self.pos + 2] == b'\r\n':
                self.pos += 2

        return result

    def _parse_array(self) -> list:
        """Parse array: *2\r\n$4\r\nPING\r\n"""
        count = int(self._read_until_crlf())

        if count == -1:
            return None  # Null array

        result = []
        for _ in range(count):
            result.append(self.parse())

        return result


def parse_resp(data: bytes):
    """
    Convenience function to parse RESP data

    Example:
        >>> parse_resp(b'*1\r\n$4\r\nPING\r\n')
        ['PING']

        >>> parse_resp(b'*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n')
        ['ECHO', 'hello']
    """
    parser = RESPParser(data)
    return parser.parse()


def parse_command(data: bytes) -> list[str] | None:
    """
    Parse a Redis command from RESP format

    Returns a list of strings representing the command and its arguments,
    or None if parsing fails.

    Example:
        >>> parse_command(b'*1\r\n$4\r\nPING\r\n')
        ['PING']

        >>> parse_command(b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        ['SET', 'key', 'value']
    """
    try:
        result = parse_resp(data)

        # Commands are typically arrays of bulk strings
        if isinstance(result, list):
            # Convert to uppercase for command matching
            return [str(item).upper() if i == 0 else str(item)
                    for i, item in enumerate(result)]

        return None
    except Exception as e:
        print(f"Error parsing RESP: {e}")
        return None
