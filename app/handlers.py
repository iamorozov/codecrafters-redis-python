"""
Redis Command Handlers

Contains handler functions for all Redis commands.
Each handler takes a command object and returns RESP-encoded bytes.
"""
import asyncio

from app.commands import *
from app.resp_encoder import *
from app.storage import store, queues, stream_queues


def handle_ping(command: PingCommand) -> bytes:
    """Handle PING command - returns PONG"""
    print("Sent: +PONG")
    return encode_simple_string("PONG")


def handle_echo(command: EchoCommand) -> bytes:
    """Handle ECHO command - returns the message as a bulk string"""
    print(f"Sent: {command.message}")
    return encode_bulk_string(command.message)


def handle_set(command: SetCommand) -> bytes:
    """Handle SET command - stores key-value pair with optional expiry"""
    now = round(time.time() * 1000)
    expiry = None

    if command.expiry_ms is not None:
        expiry = now + command.expiry_ms

    store[command.key] = (command.value, expiry)
    print(f"Saved: {command.key}={command.value} (expiry: {expiry})")
    return encode_simple_string("OK")


def handle_get(command: GetCommand) -> bytes:
    """Handle GET command - retrieves value by key"""
    value, expiry = store.get(command.key, (None, None))
    now = round(time.time() * 1000)

    if value is None:
        response = encode_null()
    elif expiry is not None and expiry < now:
        # Key expired, delete it
        del store[command.key]
        response = encode_null()
    else:
        response = encode_bulk_string(value)

    print(f"Sent: {value}")
    return response


def handle_rpush(command: RpushCommand) -> bytes:
    """Handle RPUSH command - appends values to the end of the list"""
    store[command.list_key] = store.get(command.list_key, []) + command.values
    print(f"Saved: {command.list_key}={store[command.list_key]}")

    push_values_into_queue(command.list_key, command.values)

    return encode_integer(len(store[command.list_key]))


def handle_lpush(command: LpushCommand) -> bytes:
    """Handle LPUSH command - prepends values to the beginning of the list in reverse order"""
    # LPUSH inserts values in reverse order at the beginning
    # Example: LPUSH mylist "a" "b" "c" → ["c", "b", "a", ...existing items]
    existing_list = store.get(command.list_key, [])
    reversed_values = list(reversed(command.values))
    store[command.list_key] = reversed_values + existing_list

    push_values_into_queue(command.list_key, reversed_values)

    print(f"Saved: {command.list_key}={store[command.list_key]}")
    return encode_integer(len(store[command.list_key]))


def push_values_into_queue(key: str, values: list) -> None:
    """Helper function to push values into blocking queue if it exists"""
    queue = queues.get(key)
    if queue:
        for value in values:
            queue.put_nowait(value)


def handle_lrange(command: LrangeCommand) -> bytes:
    """Handle LRANGE command - retrieves values from a list"""
    stored_list = store.get(command.list_key, [])
    start = command.start if command.start >= 0 else max(0, len(stored_list) + command.start)
    stop = command.stop + 1 if command.stop >= 0 else max(0, len(stored_list) + command.stop + 1)

    result = stored_list[start : stop]
    print(f"Retrieved: {command.list_key}={result}")
    return encode_array([encode_bulk_string(x) for x in result])


def handle_llen(command: LlenCommand) -> bytes:
    """Handle LLEN command - returns the length of a list"""
    stored_list = store.get(command.list_key, [])
    length = len(stored_list)
    print(f"Length of {command.list_key}: {length}")
    return encode_integer(length)


def handle_lpop(command: LpopCommand) -> bytes:
    """Handle LPOP command - removes and returns the first element(s) of a list"""
    stored_list = store.get(command.list_key, [])

    if not stored_list:
        # List doesn't exist or is empty
        print(f"LPOP {command.list_key}: list is empty or doesn't exist")
        return encode_null()

    # Determine how many elements to pop
    count = command.count if command.count is not None else 1

    # Pop multiple elements and return as array
    elements_to_pop = min(count, len(stored_list))
    popped_elements = stored_list[:elements_to_pop]
    remaining_list = stored_list[elements_to_pop:]

    # Update the store (remove key if list is now empty)
    if remaining_list:
        store[command.list_key] = remaining_list
    else:
        del store[command.list_key]

    print(f"Popped {elements_to_pop} from {command.list_key}: {popped_elements}")
    return encode_array([encode_bulk_string(x) for x in popped_elements]) if len(popped_elements) > 1 \
        else encode_bulk_string(popped_elements[0])


def handle_type(command: TypeCommand) -> bytes:
    """Handle TYPE command - returns the type of value stored at key"""
    if command.key not in store:
        # Key doesn't exist
        print(f"TYPE {command.key}: none")
        return encode_simple_string("none")

    value = store[command.key]

    # Determine the type based on the value structure
    if isinstance(value, list):
        # Check if it's a stream (list of tuples with (ms, seq, fields))
        if value and isinstance(value[0], tuple) and len(value[0]) == 3 and isinstance(value[0][2], dict):
            type_name = "stream"
        else:
            type_name = "list"
    elif isinstance(value, tuple) and len(value) == 2:
        # This is our (value, expiry) format for strings
        type_name = "string"
    else:
        # Fallback for any other type
        type_name = "string"

    print(f"TYPE {command.key}: {type_name}")
    return encode_simple_string(type_name)


def handle_xadd(command: XaddCommand) -> bytes:
    """Handle XADD command - adds an entry to a stream"""
    # Get or create the stream
    stream = store.get(command.stream_key, [])

    # Generate sequence number if missing
    if not command.entry_id_seq and stream:
        last_ms, last_seq, _ = stream[-1]

        if command.entry_id_ms == last_ms:
            command.entry_id_seq = last_seq + 1
        else:
            command.entry_id_seq = 0

    if not command.entry_id_seq and not stream:
        if command.entry_id_ms == 0:
            command.entry_id_seq = 1
        else:
            command.entry_id_seq = 0

    # Validate against last entry ID if stream is not empty
    if stream:
        last_ms, last_seq, _ = stream[-1]

        # Check ordering: milliseconds must be >= last_ms
        if command.entry_id_ms < last_ms:
            return encode_error("The ID specified in XADD is equal or smaller than the target stream top item")

        # If milliseconds are equal, sequence must be strictly greater
        if command.entry_id_ms == last_ms and command.entry_id_seq <= last_seq:
            return encode_error("The ID specified in XADD is equal or smaller than the target stream top item")

    # Create entry as tuple: (ms, seq, fields_dict)
    entry = (command.entry_id_ms, command.entry_id_seq, command.fields)

    # Append entry to stream
    stream.append(entry)
    store[command.stream_key] = stream

    entry_id_str = f"{command.entry_id_ms}-{command.entry_id_seq}"
    print(f"Added to stream {command.stream_key}: ID={entry_id_str}, fields={command.fields}")

    # Notify waiting XREAD BLOCK commands
    if command.stream_key in stream_queues:
        for queue in stream_queues[command.stream_key]:
            try:
                queue.put_nowait(entry)
            except asyncio.QueueFull:
                pass  # Queue is full, skip

    # Return the entry ID as a bulk string
    return encode_bulk_string(entry_id_str)


def handle_xrange(command: XrangeCommand) -> bytes:
    """Handle XRANGE command - returns a range of entries from a stream"""
    start_ms = command.start_id_ms
    start_seq = command.start_id_seq if command.start_id_seq is not None else 0
    end_ms = command.end_id_ms
    end_seq = command.end_id_seq

    # Get or create the stream
    stream = store.get(command.stream_key, [])

    if not stream:
        print(f"XRANGE {command.stream_key}: list is empty or doesn't exist")
        return encode_null()

    if not start_ms:
        start_ms = stream[0][0]

    if not end_ms:
        end_ms = stream[-1][0]

    def include(ms, seq):
        return (start_ms <= ms <= end_ms and (ms == start_ms and seq >= start_seq or ms != start_ms) and
                (end_seq is None or ms == end_ms and seq <= end_seq or ms != end_ms))

    result = [(f"{ms}-{seq}", data) for (ms, seq, data) in stream if include(ms, seq)]

    print(f"XRANGE called on {command.stream_key}: start={command.start_id_ms}-{command.start_id_seq}, end={command.end_id_ms}-{command.end_id_seq}. Result={result}")
    return encode_array(result)


async def handle_xread(command: XreadCommand) -> bytes:
    """Handle XREAD command - reads entries from streams after specified IDs (exclusive)"""

    def get_new_entries():
        """Helper to get new entries from all streams"""
        results = []
        for i, (stream_key, last_id_ms, last_id_seq) in enumerate(command.streams):
            # Get the stream
            stream = store.get(stream_key, [])

            if not stream:
                # If stream doesn't exist, skip it
                continue

            # If last id not specified, set it to the greatest value for async wait
            if last_id_ms is None:
                command.streams[i] = (stream_key, stream[-1][0], stream[-1][1])
                continue

            # Default sequence to 0 if not specified
            seq_to_check = last_id_seq if last_id_seq is not None else 0

            # Filter entries with ID > last_id (exclusive)
            def is_greater(ms, seq):
                if ms > last_id_ms:
                    return True
                if ms == last_id_ms and seq > seq_to_check:
                    return True
                return False

            stream_entries = [(f"{ms}-{seq}", data) for (ms, seq, data) in stream if is_greater(ms, seq)]

            # Only include stream if it has entries
            if stream_entries:
                results.append([stream_key, stream_entries])

        return results

    # Try to get data immediately
    results = get_new_entries()

    # If no data and blocking is requested, wait for new data
    if not results and command.block_ms is not None:
        # Create queues for each stream we're watching
        my_queues = []
        for stream_key, _, _ in command.streams:
            queue = asyncio.Queue(maxsize=1)
            my_queues.append((stream_key, queue))

            # Register queue for this stream
            if stream_key not in stream_queues:
                stream_queues[stream_key] = []
            stream_queues[stream_key].append(queue)

        try:
            # Wait for data with timeout
            timeout_seconds = command.block_ms / 1000.0 if command.block_ms > 0 else None

            # Wait for any queue to receive data
            async def wait_for_any_queue():
                tasks = [asyncio.create_task(queue.get()) for _, queue in my_queues]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                # Cancel pending tasks
                for task in pending:
                    task.cancel()

                return True

            try:
                if timeout_seconds is not None and timeout_seconds > 0:
                    await asyncio.wait_for(wait_for_any_queue(), timeout=timeout_seconds)
                elif timeout_seconds is None:
                    await wait_for_any_queue()
                # If timeout is 0, we already checked once above
            except asyncio.TimeoutError:
                pass  # Timeout expired, return empty

            # After waking up, get new entries
            results = get_new_entries()

        finally:
            # Clean up queues
            for stream_key, queue in my_queues:
                if stream_key in stream_queues:
                    try:
                        stream_queues[stream_key].remove(queue)
                        if not stream_queues[stream_key]:
                            del stream_queues[stream_key]
                    except (ValueError, KeyError):
                        pass

    print(f"XREAD called. Results: {results}")

    # If no streams have data, return null
    if not results:
        return encode_array(None)

    return encode_array(results)


async def handle_blpop(command: BlpopCommand) -> bytes:
    """Handle BLPOP command - removes and returns the first element(s) of a list (Blocking)"""
    stored_list = store.get(command.list_key, [])

    if not stored_list:
        queue = queues.get(command.list_key, asyncio.Queue())
        queues[command.list_key] = queue

        try:
            timeout = command.timeout if command.timeout > 0 else None
            item = await asyncio.wait_for(queue.get(), timeout=timeout)
            queue.task_done()
            return encode_array([encode_bulk_string(command.list_key), encode_bulk_string(item)])
        except asyncio.TimeoutError:
            return encode_array(None)
    else:
        return encode_array([encode_bulk_string(command.list_key), handle_lpop(LpopCommand(list_key=command.list_key))])
