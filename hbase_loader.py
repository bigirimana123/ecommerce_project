# Import packages necesarry for hbase_loader.py

import json
import happybase
from datetime import datetime

# Connect to HBase running via Docker
connection = happybase.Connection('localhost', port=9090)
connection.open()

# Create table for user_sessions if not exists
if b'user_sessions' not in connection.tables():
    connection.create_table(
        'user_sessions',
        {'session_info': dict()}
    )

# Open data file
with open("data/sessions.json", "r") as f:
    sessions = json.load(f)

table = connection.table('user_sessions')

# Insert sessions
for sess in sessions:
    ts = datetime.fromisoformat(sess["start_time"]).strftime('%Y%m%d%H%M')
    row_key = f"{sess['user_id']}#{ts}"   #This relationship- creates a unique link between user_id and their sessions
    data = {
        b'session_info:session_id': sess["session_id"].encode(),
        b'session_info:start_time': sess["start_time"].encode(),
        b'session_info:end_time': sess["end_time"].encode(),
        b'session_info:duration': str(sess["duration_seconds"]).encode(),
        b'session_info:device': sess["device_profile"]["type"].encode(),
        b'session_info:browser': sess["device_profile"]["browser"].encode(),
        b'session_info:referrer': sess["referrer"].encode()
    }
    table.put(row_key.encode(), data)

print("✅ Loaded sessions into HBase")
