"""
Redis Server Configuration

Server configuration state set from command line arguments.
"""

# Server role: "master" or "slave"
server_role = "master"

# Master server info (when running as slave)
master_host = None
master_port = None
listening_port = None
