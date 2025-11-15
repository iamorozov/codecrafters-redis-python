"""
Redis Command Models and Parser

This module contains:
- Command dataclasses representing different Redis commands
- Command parsing logic that converts RESP arrays into command objects
"""
import time
from dataclasses import dataclass
from typing import Optional
from app.resp_parser import parse_resp


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
    values: list[str]


@dataclass
class LpushCommand:
    """The LPUSH command is used to prepend elements to a list in reverse order. If the list doesn't exist, it is created first."""
    list_key: str
    values: list[str]


@dataclass
class LrangeCommand:
    """The LRANGE command is used to retrieve elements from a list using a start index and a stop index."""
    list_key: str
    start: int
    stop: int


@dataclass
class LlenCommand:
    """The LLEN command is used to get the length of a list."""
    list_key: str


@dataclass
class LpopCommand:
    """The LPOP command is used to remove and return the first element(s) of a list."""
    list_key: str
    count: Optional[int] = None  # Number of elements to pop (None means 1)


@dataclass
class BlpopCommand:
    """BLPOP is a blocking variant of the LPOP command. It waits for an element to become available on a list before popping it."""
    list_key: str
    timeout: float


@dataclass
class TypeCommand:
    """The TYPE command returns the type of the value stored at key."""
    key: str


@dataclass
class XaddCommand:
    """The XADD command appends a new entry to a stream."""
    stream_key: str
    entry_id_ms: int  # Milliseconds part of entry ID
    entry_id_seq: Optional[int]  # Sequence number part of entry ID
    fields: dict[str, str]  # Key-value pairs for the entry


@dataclass
class XrangeCommand:
    """The XRANGE command returns a range of entries from a stream."""
    stream_key: str
    start_id_ms: Optional[int]  # Milliseconds part of start ID
    start_id_seq: Optional[int]  # Sequence number part of start ID
    end_id_ms: Optional[int]  # Milliseconds part of end ID
    end_id_seq: Optional[int]  # Sequence number part of end ID


@dataclass
class XreadCommand:
    """The XREAD command reads entries from one or more streams after a specified ID (exclusive)."""
    streams: list[tuple[str, Optional[int], Optional[int]]]  # List of (stream_key, last_id_ms, last_id_seq)
    block_ms: Optional[int] = None  # Blocking timeout in milliseconds (None = non-blocking)


@dataclass
class IncrCommand:
    """INCR command - increments the integer value of a key by one"""
    key: str


@dataclass
class MultiCommand:
    """MULTI command - marks the start of a transaction block"""
    pass


@dataclass
class ExecCommand:
    """EXEC command - executes all commands in the transaction block"""
    pass


@dataclass
class CommandError:
    """Represents a command parsing/validation error"""
    message: str


# Helper functions for parsing

def parse_stream_id(id_str: str, allow_special: bool = False) -> tuple[Optional[int], Optional[int]] | CommandError:
    """
    Parse a stream ID string into (milliseconds, sequence) tuple.

    Args:
        id_str: The ID string to parse (e.g., "1526985054069", "1526985054069-0", "-", "+", "*")
        allow_special: Whether to allow special values like "-" (min) or "+" (max)

    Returns:
        Tuple of (ms, seq) where either can be None for special values,
        or CommandError if parsing fails

    Examples:
        >>> parse_stream_id("1526985054069")
        (1526985054069, None)

        >>> parse_stream_id("1526985054069-0")
        (1526985054069, 0)

        >>> parse_stream_id("-", allow_special=True)
        (None, None)

        >>> parse_stream_id("+", allow_special=True)
        (None, None)
    """
    # Handle special values
    if allow_special:
        if id_str == '-' or id_str == '+' or id_str == '$':
            return (None, None)

    # Parse format: "ms" or "ms-seq"
    try:
        if '-' in id_str:
            parts = id_str.split('-')
            if len(parts) != 2:
                return CommandError("Invalid stream ID specified as stream command argument")

            ms = int(parts[0])
            seq = int(parts[1]) if parts[1] != '*' else None
            return (ms, seq)
        else:
            ms = int(id_str)
            return (ms, None)
    except ValueError:
        return CommandError("Invalid stream ID specified as stream command argument")


# Individual command parsers

def parse_ping(args: list):
    """Parse PING command"""
    if len(args) > 0:
        return CommandError("wrong number of arguments for 'ping' command")
    return PingCommand()


def parse_echo(args: list):
    """Parse ECHO command"""
    if len(args) != 1:
        return CommandError("wrong number of arguments for 'echo' command")
    return EchoCommand(message=str(args[0]))


def parse_set(args: list):
    """Parse SET command"""
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


def parse_get(args: list):
    """Parse GET command"""
    if len(args) != 1:
        return CommandError("wrong number of arguments for 'get' command")
    return GetCommand(key=str(args[0]))


def parse_rpush(args: list):
    """Parse RPUSH command"""
    if len(args) < 2:
        return CommandError("wrong number of arguments for 'rpush' command")
    return RpushCommand(list_key=str(args[0]), values=args[1:])


def parse_lpush(args: list):
    """Parse LPUSH command"""
    if len(args) < 2:
        return CommandError("wrong number of arguments for 'lpush' command")
    return LpushCommand(list_key=str(args[0]), values=args[1:])


def parse_lrange(args: list):
    """Parse LRANGE command"""
    if len(args) < 2:
        return CommandError("wrong number of arguments for 'lrange' command")
    return LrangeCommand(list_key=str(args[0]), start=int(args[1]), stop=int(args[2]))


def parse_llen(args: list):
    """Parse LLEN command"""
    if len(args) != 1:
        return CommandError("wrong number of arguments for 'llen' command")
    return LlenCommand(list_key=str(args[0]))


def parse_lpop(args: list):
    """Parse LPOP command"""
    if len(args) < 1 or len(args) > 2:
        return CommandError("wrong number of arguments for 'lpop' command")

    list_key = str(args[0])
    count = None

    if len(args) == 2:
        try:
            count = int(args[1])
            if count <= 0:
                return CommandError("count must be positive")
        except ValueError:
            return CommandError("count must be an integer")

    return LpopCommand(list_key=list_key, count=count)


def parse_blpop(args: list):
    """Parse BLPOP command"""
    if len(args) < 1 or len(args) > 2:
        return CommandError("wrong number of arguments for 'blpop' command")
    return BlpopCommand(list_key=str(args[0]), timeout=float(args[1]))


def parse_type(args: list):
    """Parse TYPE command"""
    if len(args) != 1:
        return CommandError("wrong number of arguments for 'type' command")
    return TypeCommand(key=str(args[0]))


def parse_xadd(args: list):
    """Parse XADD command"""
    if len(args) < 3:
        return CommandError("wrong number of arguments for 'xadd' command")

    stream_key = str(args[0])
    entry_id = str(args[1])

    if entry_id == '*':
        current_millis = int(round(time.time() * 1000))
        entry_id = str(current_millis) + '-*'

    # Parse and validate entry ID format: <milliseconds>-<sequence>
    if '-' not in entry_id:
        return CommandError("Invalid stream ID specified as stream command argument")

    result = parse_stream_id(entry_id, allow_special=False)
    if isinstance(result, CommandError):
        return result

    entry_id_ms, entry_id_seq = result

    # Validate: 0-0 is not allowed
    if entry_id_ms == 0 and entry_id_seq == 0:
        return CommandError("The ID specified in XADD must be greater than 0-0")

    # Parse field-value pairs (remaining args must be pairs)
    field_args = args[2:]
    if len(field_args) % 2 != 0:
        return CommandError("wrong number of arguments for XADD")

    fields = {}
    for i in range(0, len(field_args), 2):
        field_name = str(field_args[i])
        field_value = str(field_args[i + 1])
        fields[field_name] = field_value

    return XaddCommand(
        stream_key=stream_key,
        entry_id_ms=entry_id_ms,
        entry_id_seq=entry_id_seq,
        fields=fields
    )


def parse_xrange(args: list):
    """Parse XRANGE command"""
    if len(args) != 3:
        return CommandError("wrong number of arguments for 'xrange' command")

    stream_key = str(args[0])
    start_id = str(args[1])
    end_id = str(args[2])

    # Parse start ID (can be "-", "ms" or "ms-seq")
    start_result = parse_stream_id(start_id, allow_special=True)
    if isinstance(start_result, CommandError):
        return start_result
    start_id_ms, start_id_seq = start_result

    # Parse end ID (can be "ms", "+" or "ms-seq")
    end_result = parse_stream_id(end_id, allow_special=True)
    if isinstance(end_result, CommandError):
        return end_result
    end_id_ms, end_id_seq = end_result

    return XrangeCommand(
        stream_key=stream_key,
        start_id_ms=start_id_ms,
        start_id_seq=start_id_seq,
        end_id_ms=end_id_ms,
        end_id_seq=end_id_seq
    )


def parse_xread(args: list):
    """Parse XREAD command - syntax: XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]"""
    if len(args) < 3:
        return CommandError("wrong number of arguments for 'xread' command")

    # Check for optional BLOCK keyword
    block_ms = None
    args_offset = 0

    if len(args) >= 2 and str(args[0]).upper() == 'BLOCK':
        try:
            block_ms = int(args[1])
            args_offset = 2
        except ValueError:
            return CommandError("invalid BLOCK timeout")

    # Find STREAMS keyword
    streams_idx = None
    for i, arg in enumerate(args[args_offset:], start=args_offset):
        if str(arg).upper() == 'STREAMS':
            streams_idx = i
            break

    if streams_idx is None:
        return CommandError("syntax error")

    # Arguments after STREAMS should be: key1 key2 ... id1 id2 ...
    stream_args = args[streams_idx + 1:]

    if len(stream_args) == 0 or len(stream_args) % 2 != 0:
        return CommandError("wrong number of arguments for 'xread' command")

    num_streams = len(stream_args) // 2
    stream_keys = stream_args[:num_streams]
    stream_ids = stream_args[num_streams:]

    # Parse each stream key and ID pair
    streams = []
    for key, id_str in zip(stream_keys, stream_ids):
        # Parse ID (can be "ms" or "ms-seq")
        result = parse_stream_id(id_str, allow_special=True)
        if isinstance(result, CommandError):
            return result

        last_id_ms, last_id_seq = result
        streams.append((key, last_id_ms, last_id_seq))

    return XreadCommand(streams=streams, block_ms=block_ms)


def parse_incr(args: list):
    """Parse INCR command"""
    if len(args) != 1:
        return CommandError("wrong number of arguments for 'incr' command")
    return IncrCommand(key=str(args[0]))


def parse_multi(args: list):
    """Parse MULTI command"""
    if len(args) > 0:
        return CommandError("wrong number of arguments for 'multi' command")
    return MultiCommand()


def parse_exec(args: list):
    """Parse EXEC command"""
    if len(args) > 0:
        return CommandError("wrong number of arguments for 'exec' command")
    return ExecCommand()


# Command parser registry
COMMAND_PARSERS = {
    'PING': parse_ping,
    'ECHO': parse_echo,
    'SET': parse_set,
    'GET': parse_get,
    'RPUSH': parse_rpush,
    'LPUSH': parse_lpush,
    'LRANGE': parse_lrange,
    'LLEN': parse_llen,
    'LPOP': parse_lpop,
    'BLPOP': parse_blpop,
    'TYPE': parse_type,
    'XADD': parse_xadd,
    'XRANGE': parse_xrange,
    'XREAD': parse_xread,
    'INCR': parse_incr,
    'MULTI': parse_multi,
    'EXEC': parse_exec,
}


def parse_command(data: bytes):
    """
    Parse and validate a Redis command from RESP format

    Uses a registry-based dispatcher to route commands to individual parsers.

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

        # Dispatch to appropriate parser using registry
        parser = COMMAND_PARSERS.get(cmd_name)
        if parser:
            return parser(args)
        else:
            return CommandError(f"unknown command '{cmd_name}'")

    except Exception as e:
        print(f"Error parsing RESP: {e}")
        return CommandError(f"parsing error: {str(e)}")
