"""
Redis In-Memory Storage

Shared storage state used by command handlers.
"""

# Main key-value store
# - For strings: {key: (value, expiry_timestamp)}
# - For lists: {key: [item1, item2, ...]}
# - For streams: {key: [(ms, seq, fields_dict), ...]}
store = {}

# Queues for blocking operations (BLPOP)
# Maps list keys to asyncio.Queue instances
queues = {}

# Queues for blocking stream operations (XREAD BLOCK)
# Maps stream keys to list of asyncio.Queue instances (multiple readers can wait)
stream_queues = {}
