# Redis Server - Implemented Commands

This document lists all Redis commands implemented in this server.

## Table of Contents

- [Connection Commands](#connection-commands)
- [String Commands](#string-commands)
- [List Commands](#list-commands)
- [Stream Commands](#stream-commands)
- [Generic Commands](#generic-commands)

---

## Connection Commands

### PING

Tests the connection to the server.

**Syntax:**
```
PING
```

**Returns:** `PONG`

**Example:**
```bash
redis-cli PING
# Output: PONG
```

---

### ECHO

Returns the given message.

**Syntax:**
```
ECHO message
```

**Arguments:**
- `message` (string): The message to echo back

**Returns:** The message as a bulk string

**Example:**
```bash
redis-cli ECHO "Hello World"
# Output: "Hello World"
```

---

## String Commands

### SET

Set a key to hold a string value with optional expiry.

**Syntax:**
```
SET key value [EX seconds] [PX milliseconds]
```

**Arguments:**
- `key` (string): The key name
- `value` (string): The value to store
- `EX seconds` (optional): Set expiry time in seconds
- `PX milliseconds` (optional): Set expiry time in milliseconds

**Returns:** `OK`

**Examples:**
```bash
# Simple SET
redis-cli SET mykey "Hello"
# Output: OK

# SET with expiry in seconds
redis-cli SET mykey "Hello" EX 10
# Output: OK

# SET with expiry in milliseconds
redis-cli SET mykey "Hello" PX 10000
# Output: OK
```

---

### GET

Get the value of a key.

**Syntax:**
```
GET key
```

**Arguments:**
- `key` (string): The key name

**Returns:** The value of the key, or `nil` if key doesn't exist or has expired

**Examples:**
```bash
redis-cli SET mykey "Hello"
redis-cli GET mykey
# Output: "Hello"

redis-cli GET nonexistent
# Output: (nil)
```

---

## List Commands

### RPUSH

Append one or multiple values to the end of a list.

**Syntax:**
```
RPUSH key value [value ...]
```

**Arguments:**
- `key` (string): The list key
- `value` (string): One or more values to append

**Returns:** Integer - the length of the list after the push operation

**Example:**
```bash
redis-cli RPUSH mylist "a" "b" "c"
# Output: (integer) 3
```

---

### LPUSH

Prepend one or multiple values to the beginning of a list (in reverse order).

**Syntax:**
```
LPUSH key value [value ...]
```

**Arguments:**
- `key` (string): The list key
- `value` (string): One or more values to prepend

**Returns:** Integer - the length of the list after the push operation

**Example:**
```bash
redis-cli LPUSH mylist "a" "b" "c"
# Output: (integer) 3
# List will be: ["c", "b", "a"]
```

---

### LRANGE

Get a range of elements from a list.

**Syntax:**
```
LRANGE key start stop
```

**Arguments:**
- `key` (string): The list key
- `start` (integer): Start index (0-based, can be negative)
- `stop` (integer): Stop index (inclusive, can be negative)

**Returns:** Array of elements in the specified range

**Examples:**
```bash
redis-cli RPUSH mylist "a" "b" "c" "d"
redis-cli LRANGE mylist 0 2
# Output:
# 1) "a"
# 2) "b"
# 3) "c"

redis-cli LRANGE mylist -2 -1
# Output:
# 1) "c"
# 2) "d"
```

---

### LLEN

Get the length of a list.

**Syntax:**
```
LLEN key
```

**Arguments:**
- `key` (string): The list key

**Returns:** Integer - the length of the list, or 0 if key doesn't exist

**Example:**
```bash
redis-cli RPUSH mylist "a" "b" "c"
redis-cli LLEN mylist
# Output: (integer) 3
```

---

### LPOP

Remove and return the first element(s) from a list.

**Syntax:**
```
LPOP key [count]
```

**Arguments:**
- `key` (string): The list key
- `count` (integer, optional): Number of elements to pop (default: 1)

**Returns:**
- If count is 1: The popped element, or `nil` if list is empty
- If count > 1: Array of popped elements

**Examples:**
```bash
redis-cli RPUSH mylist "a" "b" "c"
redis-cli LPOP mylist
# Output: "a"

redis-cli LPOP mylist 2
# Output:
# 1) "b"
# 2) "c"
```

---

### BLPOP

Blocking variant of LPOP. Waits for an element to become available.

**Syntax:**
```
BLPOP key timeout
```

**Arguments:**
- `key` (string): The list key
- `timeout` (float): Timeout in seconds (0 = wait indefinitely)

**Returns:**
- Array with key name and popped element if successful
- `nil` if timeout expired

**Example:**
```bash
# In one terminal
redis-cli BLPOP mylist 10

# In another terminal (within 10 seconds)
redis-cli RPUSH mylist "hello"

# First terminal output:
# 1) "mylist"
# 2) "hello"
```

---

## Stream Commands

### XADD

Append a new entry to a stream.

**Syntax:**
```
XADD key ID field value [field value ...]
```

**Arguments:**
- `key` (string): The stream key
- `ID` (string): Entry ID in format `<ms>-<seq>` or `*` for auto-generation
  - `*`: Auto-generate full ID using current timestamp
  - `<ms>-*`: Use specified milliseconds, auto-generate sequence
  - `<ms>-<seq>`: Fully specified ID
- `field` (string): Field name
- `value` (string): Field value

**Returns:** The ID of the added entry

**Rules:**
- ID must be greater than the last entry ID
- ID `0-0` is not allowed

**Examples:**
```bash
# Auto-generate ID
redis-cli XADD mystream * temperature 25
# Output: "1699999999999-0"

# Specify milliseconds, auto-generate sequence
redis-cli XADD mystream 1526985054069-* temperature 30
# Output: "1526985054069-0"

# Fully specify ID
redis-cli XADD mystream 1526985054070-0 temperature 28 humidity 60
# Output: "1526985054070-0"
```

---

### XRANGE

Returns a range of entries from a stream.

**Syntax:**
```
XRANGE key start end
```

**Arguments:**
- `key` (string): The stream key
- `start` (string): Minimum entry ID (inclusive)
  - `-`: Minimum possible ID
  - `<ms>`: Timestamp in milliseconds
  - `<ms>-<seq>`: Specific ID
- `end` (string): Maximum entry ID (inclusive)
  - `+`: Maximum possible ID
  - `<ms>`: Timestamp in milliseconds
  - `<ms>-<seq>`: Specific ID

**Returns:** Array of entries, each containing ID and field-value pairs

**Examples:**
```bash
redis-cli XADD mystream 1526985054069-0 temp 25
redis-cli XADD mystream 1526985054070-0 temp 30
redis-cli XADD mystream 1526985054071-0 temp 28

# Get all entries
redis-cli XRANGE mystream - +
# Output:
# 1) 1) "1526985054069-0"
#    2) 1) "temp"
#       2) "25"
# 2) 1) "1526985054070-0"
#    2) 1) "temp"
#       2) "30"
# 3) 1) "1526985054071-0"
#    2) 1) "temp"
#       2) "28"

# Get specific range
redis-cli XRANGE mystream 1526985054069-0 1526985054070-0
```

---

### XREAD

Read entries from one or more streams starting after specified IDs (exclusive).

**Syntax:**
```
XREAD STREAMS key [key ...] id [id ...]
```

**Arguments:**
- `STREAMS`: Keyword indicating start of stream specifications
- `key` (string): One or more stream keys
- `id` (string): Last seen ID for each stream (exclusive)
  - `<ms>`: Timestamp only
  - `<ms>-<seq>`: Full ID

**Returns:**
- Array of streams with their entries (only streams with new data)
- `nil` if no streams have data

**Behavior:**
- Returns entries with ID strictly greater than specified ID (exclusive)
- Unlike XRANGE, this is exclusive (does not include the specified ID)

**Examples:**
```bash
redis-cli XADD stream1 1526985054069-0 temp 25
redis-cli XADD stream1 1526985054070-0 temp 30
redis-cli XADD stream2 1526999999999-0 humidity 60

# Read from single stream after ID 1526985054069-0
redis-cli XREAD STREAMS stream1 1526985054069-0
# Output:
# 1) 1) "stream1"
#    2) 1) 1) "1526985054070-0"
#          2) 1) "temp"
#             2) "30"

# Read from multiple streams
redis-cli XREAD STREAMS stream1 stream2 1526985054069-0 1526999999998-0
# Output:
# 1) 1) "stream1"
#    2) 1) 1) "1526985054070-0"
#          2) 1) "temp"
#             2) "30"
# 2) 1) "stream2"
#    2) 1) 1) "1526999999999-0"
#          2) 1) "humidity"
#             2) "60"
```

---

## Generic Commands

### TYPE

Determine the type of value stored at a key.

**Syntax:**
```
TYPE key
```

**Arguments:**
- `key` (string): The key name

**Returns:** String indicating the type:
- `string`: Key holds a string value
- `list`: Key holds a list
- `stream`: Key holds a stream
- `none`: Key doesn't exist

**Examples:**
```bash
redis-cli SET mykey "hello"
redis-cli TYPE mykey
# Output: string

redis-cli RPUSH mylist "a"
redis-cli TYPE mylist
# Output: list

redis-cli XADD mystream * temp 25
redis-cli TYPE mystream
# Output: stream

redis-cli TYPE nonexistent
# Output: none
```

---

## Implementation Notes

### ID Format

Stream IDs follow the format `<milliseconds>-<sequence>`:
- `milliseconds`: Unix timestamp in milliseconds
- `sequence`: Sequence number for entries with same timestamp

### Data Storage

- **Strings**: Stored as `(value, expiry_timestamp)` tuples
- **Lists**: Stored as Python lists
- **Streams**: Stored as lists of tuples `(ms, seq, fields_dict)`

### Pattern Matching

The server uses Python 3.10+ pattern matching for command dispatching, providing clean and maintainable code structure.

### Architecture

The codebase follows a modular architecture:
- `commands.py`: Command models and parsing
- `handlers.py`: Command handler functions
- `resp_parser.py`: Low-level RESP protocol parsing
- `resp_encoder.py`: RESP protocol encoding
- `storage.py`: Shared in-memory storage
- `main.py`: Server initialization and connection handling
