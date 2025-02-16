import redis

r = redis.Redis(host='localhost', port=6379, db=0)
# r.flushdb()  # This will drop all keys in the currently selected database.
print("Number of keys in the database:", r.dbsize())