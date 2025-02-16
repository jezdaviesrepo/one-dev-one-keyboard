import redis
from redis.commands.search.query import Query

class RedisSecurityCacheIndexer:
    def __init__(self, host='localhost', port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db)
        self.index_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]

    def _make_key(self, record):
        # Build a composite key from the first 8 fields.
        key_parts = [record.get(field, "").strip() for field in self.index_fields]
        return "security:" + "|".join(key_parts)

    def update_record(self, record):
        """Stores a record in a Redis hash and updates index sets for each key field."""
        key = self._make_key(record)
        self.r.hset(key, mapping=record)
        for field in self.index_fields:
            value = record.get(field, "").strip()
            if value:
                index_key = f"index:{field}:{value}"
                self.r.sadd(index_key, key)
        print(f"Record stored with key: {key}")

    def get_record_by_key(self, key):
        """Retrieve a record directly using its composite key."""
        rec = self.r.hgetall(key)
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in rec.items()}

    def search_by_field(self, field, value):
        """
        Searches for records by a given field (e.g. COMPANY_NAME).
        Returns a list of composite keys that match.
        """
        index_key = f"index:{field}:{value}"
        keys = self.r.smembers(index_key)
        results = []
        for key in keys:
            key_decoded = key.decode('utf-8')
            record = self.get_record_by_key(key_decoded)
            results.append(record)
        return results

# Example usage:
if __name__ == "__main__":
    # Initialize the indexer.
    indexer = RedisSecurityCacheIndexer()
    
    # Option 1: Get a record by its known composite key.
    example_key = "security:BBG000BLNNH6|037833100|2046251|US0378331005|Apple Inc.|USD|Equity|Domestic Equity"
    record = indexer.get_record_by_key(example_key)
    print("Record by key:")
    print(record)
    
    # Option 2: Search for records where COMPANY_NAME equals "Apple Inc."
    search_results = indexer.search_by_field("FIGI", "BBG000BLNNH6")
    print("\nSearch results for FIGI 'BBG000BLNNH6':")
    for rec in search_results:
        print(rec)