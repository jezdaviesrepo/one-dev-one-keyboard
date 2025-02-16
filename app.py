from flask import Flask, render_template, request, jsonify
import redis
import psycopg2
import datetime
import json

app = Flask(__name__)

### Redis Setup ###
# Connect to Redis (adjust host/port as needed)
redis_client = redis.Redis(host="localhost", port=6379, db=0)

### Postgres Connection Helper ###
def get_latest_security_record(params):
    """
    Given a dictionary with keys: FIGI, CUSIP, SEDOL, ISIN, COMPANY_NAME, CURRENCY, ASSET_CLASS, ASSET_GROUP,
    query Postgres for the latest full record (all columns) for that security, ordered by APPLIED_DATE.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""SELECT * FROM dummy_security_master 
              WHERE {where_clause} 
              ORDER BY "APPLIED_DATE" DESC LIMIT 1;"""
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
        cur = conn.cursor()
        cur.execute(sql, values)
        row = cur.fetchone()
        colnames = [desc[0] for desc in cur.description] if row else []
        conn.close()
        if row:
            return dict(zip(colnames, row))
        else:
            return {}
    except Exception as e:
        print("Error querying Postgres:", e)
        return {}

def get_all_security_versions(params):
    """
    Queries Postgres for all versions of a security matching the key fields,
    ordered by APPLIED_DATE descending.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""SELECT * FROM dummy_security_master 
              WHERE {where_clause} 
              ORDER BY "APPLIED_DATE" DESC;"""
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
        cur = conn.cursor()
        cur.execute(sql, values)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        conn.close()
        return [dict(zip(colnames, row)) for row in rows]
    except Exception as e:
        print("Error querying Postgres for versions:", e)
        return []

### Routes ###

@app.route("/")
def index():
    # Render the grid UI
    return render_template("grid.html")

@app.route("/data")
def data():
    """
    Retrieves up to 1000 security records from Redis.
    Each record is stored as a hash under keys with prefix "security:".
    Only the first 8 fields are returned.
    """
    keys = redis_client.keys("security:*")[:1000]
    records = []
    for key in keys:
        rec = redis_client.hgetall(key)
        record = {k.decode("utf-8"): v.decode("utf-8") for k, v in rec.items()}
        filtered = {field: record.get(field, "") for field in [
            "FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"
        ]}
        records.append(filtered)
    return jsonify(records)

@app.route("/security_detail")
def security_detail():
    """
    Reads query parameters for the 8 key fields, queries Postgres for the latest full record,
    and renders the detail page.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = {field: request.args.get(field, "") for field in fields}
    record = get_latest_security_record(params)
    versions = get_all_security_versions(params)
    return render_template("security_detail.html", record=record, versions=versions)

if __name__ == "__main__":
    app.run(debug=True)