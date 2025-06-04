# Import packages necesarry for hbase_query_user_sessions.py

import happybase

connection = happybase.Connection('localhost', port=9090)
connection.open()

table = connection.table('user_sessions')

# Change to any user_id present in sessions
user_id = "user_000001"

# Scan by prefix of row key
prefix = f"{user_id}#"
print(f"🔍 Sessions for {user_id}")
for key, data in table.scan(row_prefix=prefix.encode()):
    print(f"Row Key: {key.decode()}")
    for col, val in data.items():
        print(f"  {col.decode()}: {val.decode()}")
    print()
