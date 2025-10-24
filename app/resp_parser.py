"""
RESP (Redis Serialization Protocol) Parser

RESP types:
- Simple Strings: +OK\r\n
- Errors: -Error message\r\n
- Integers: :1000\r\n
- Bulk Strings: $6\r\nfoobar\r\n (length followed by data)
- Arrays: *2\r\n$4\r\nPING\r\n (count followed by elements)
"""

from dataclasses import dataclass
from typing import Optional


# Command data classes
@dataclass
class PingCommand:
    """PING command - returns PONG"""
    pass


@dataclass
class EchoCommand:
    """ECHO command - returns the message"""
    message: str


@dataclass
class SetCommand:
    """SET command - stores a key-value pair with optional expiry"""
    key: str
    value: str
    expiry_ms: Optional[int] = None  # Expiry in milliseconds from now


@dataclass
class GetCommand:
    """GET command - retrieves a value by key"""
    key: str


@dataclass
class RpushCommand:
    """The RPUSH command is used to append elements to a list. If the list doesn't exist, it is created first."""
    list_key: str
    value: str


@dataclass
class CommandError:
    """Represents a command parsing/validation error"""
    message: str


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


def parse_command(data: bytes):
    """
    Parse and validate a Redis command from RESP format

    Returns a command object (PingCommand, EchoCommand, SetCommand, GetCommand)
    or CommandError if validation fails.

    Example:
        >>> parse_command(b'*1\r\n$4\r\nPING\r\n')
        PingCommand()

        >>> parse_command(b'*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n')
        EchoCommand(message='hello')

        >>> parse_command(b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
        SetCommand(key='key', value='value', expiry_ms=None)
    """
    try:
        result = parse_resp(data)

        # Commands are arrays of bulk strings
        if not isinstance(result, list) or len(result) == 0:
            return CommandError("Invalid command format")

        # Extract command name and arguments
        cmd_name = str(result[0]).upper()
        args = result[1:]

        # Validate and construct command objects
        if cmd_name == 'PING':
            if len(args) > 0:
                return CommandError("wrong number of arguments for 'ping' command")
            return PingCommand()

        elif cmd_name == 'ECHO':
            if len(args) != 1:
                return CommandError("wrong number of arguments for 'echo' command")
            return EchoCommand(message=str(args[0]))

        elif cmd_name == 'SET':
            if len(args) < 2:
                return CommandError("wrong number of arguments for 'set' command")

            key = str(args[0])
            value = str(args[1])
            expiry_ms = None

            # Parse optional expiry arguments
            if len(args) >= 4:
                expiry_type = str(args[2]).upper()
                try:
                    expiry_value = int(args[3])

                    if expiry_type == 'EX':
                        # EX: seconds
                        expiry_ms = expiry_value * 1000
                    elif expiry_type == 'PX':
                        # PX: milliseconds
                        expiry_ms = expiry_value
                    else:
                        return CommandError(f"invalid expiry option: {expiry_type}")

                except (ValueError, IndexError):
                    return CommandError("invalid expiry value")

            elif len(args) == 3:
                return CommandError("syntax error")

            return SetCommand(key=key, value=value, expiry_ms=expiry_ms)

        elif cmd_name == 'GET':
            if len(args) != 1:
                return CommandError("wrong number of arguments for 'get' command")
            return GetCommand(key=str(args[0]))

        elif cmd_name == 'RPUSH':
            if len(args) != 2:
                return CommandError("wrong number of arguments for 'rpush' command")
            return RpushCommand(list_key=str(args[0]), value=str(args[1]))

        else:
            return CommandError(f"unknown command '{cmd_name}'")

    except Exception as e:
        print(f"Error parsing RESP: {e}")
        return CommandError(f"parsing error: {str(e)}")


# RESP Encoding Functions

def encode_simple_string(s: str) -> bytes:
    """
    Encode a simple string in RESP format: +<string>\r\n

    Example:
        >>> encode_simple_string("OK")
        b'+OK\\r\\n'

        >>> encode_simple_string("PONG")
        b'+PONG\\r\\n'
    """
    return f"+{s}\r\n".encode('utf-8')


def encode_error(message: str) -> bytes:
    """
    Encode an error message in RESP format: -ERR <message>\r\n

    Example:
        >>> encode_error("unknown command 'foo'")
        b"-ERR unknown command 'foo'\\r\\n"
    """
    return f"-ERR {message}\r\n".encode('utf-8')


def encode_integer(n: int) -> bytes:
    """
    Encode an integer in RESP format: :<number>\r\n

    Example:
        >>> encode_integer(1000)
        b':1000\\r\\n'
    """
    return f":{n}\r\n".encode('utf-8')


def encode_bulk_string(s: Optional[str]) -> bytes:
    """
    Encode a bulk string in RESP format: $<length>\r\n<data>\r\n
    If None is passed, encodes a null bulk string: $-1\r\n

    Example:
        >>> encode_bulk_string("hello")
        b'$5\\r\\nhello\\r\\n'

        >>> encode_bulk_string("foo bar")
        b'$7\\r\\nfoo bar\\r\\n'

        >>> encode_bulk_string(None)
        b'$-1\\r\\n'
    """
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode('utf-8')


def encode_null() -> bytes:
    """
    Encode a null bulk string in RESP format: $-1\r\n

    Example:
        >>> encode_null()
        b'$-1\\r\\n'
    """
    return b"$-1\r\n"


def encode_array(items: Optional[list]) -> bytes:
    """
    Encode an array in RESP format: *<count>\r\n<element1><element2>...
    If None is passed, encodes a null array: *-1\r\n

    Note: Items should already be RESP-encoded bytes

    Example:
        >>> encode_array([encode_bulk_string("foo"), encode_bulk_string("bar")])
        b'*2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n'

        >>> encode_array(None)
        b'*-1\\r\\n'
    """
    if items is None:
        return b"*-1\r\n"

    result = f"*{len(items)}\r\n".encode('utf-8')
    for item in items:
        result += item
    return result
