"""
RESP (Redis Serialization Protocol) Encoder

Provides functions to encode Python objects into RESP format for Redis responses.

RESP types:
- Simple Strings: +OK\r\n
- Errors: -Error message\r\n
- Integers: :1000\r\n
- Bulk Strings: $6\r\nfoobar\r\n (length followed by data)
- Arrays: *2\r\n$4\r\nPING\r\n (count followed by elements)
"""
from typing import Optional


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
        if isinstance(item, bytes):
            result += item
        elif isinstance(item, str):
            result += encode_bulk_string(item)
        elif isinstance(item, int):
            result += encode_integer(item)
        elif isinstance(item, list) or isinstance(item, tuple):
            result += encode_array(item)
        elif isinstance(item, dict):
            array = []
            for k, v in item.items():
                array.append(str(k))
                array.append(str(v))
            result += encode_array(array)
        else:
            raise NotImplementedError(f"Unknown item type: {type(item)} for {item}")
    return result
