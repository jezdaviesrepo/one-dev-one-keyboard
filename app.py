from flask import Flask, render_template, request, jsonify
import redis
import psycopg2
import datetime

app = Flask(__name__)

### Redis Setup ###
redis_client = redis.Redis(host="localhost", port=6379, db=0)

### Postgres Helpers ###
def get_latest_security_record(params):
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM dummy_security_master 
          WHERE {where_clause}
          ORDER BY "APPLIED_DATE" DESC LIMIT 1;
          """
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres",
                                host="localhost", port=5432)
        cur = conn.cursor()
        cur.execute(sql, values)
        row = cur.fetchone()
        colnames = [desc[0] for desc in cur.description] if row else []
        conn.close()
        return dict(zip(colnames, row)) if row else {}
    except Exception as e:
        print("Error querying Postgres:", e)
        return {}

def get_all_security_versions(params):
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT DISTINCT ON ("APPLIED_DATE") *
          FROM dummy_security_master 
          WHERE {where_clause}
          ORDER BY "APPLIED_DATE" DESC;
          """
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres",
                                host="localhost", port=5432)
        cur = conn.cursor()
        cur.execute(sql, values)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        conn.close()
        return [dict(zip(colnames, row)) for row in rows]
    except Exception as e:
        print("Error querying Postgres for versions:", e)
        return []

def get_security_record_by_date(params, applied_date):
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM dummy_security_master 
          WHERE {where_clause} AND "APPLIED_DATE" = %s 
          LIMIT 1;
          """
    values = [params.get(field, "") for field in fields] + [applied_date]
    try:
        conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres",
                                host="localhost", port=5432)
        cur = conn.cursor()
        cur.execute(sql, values)
        row = cur.fetchone()
        colnames = [desc[0] for desc in cur.description] if row else []
        conn.close()
        return dict(zip(colnames, row)) if row else {}
    except Exception as e:
        print("Error querying Postgres by date:", e)
        return {}

### Flask Routes ###

@app.route("/")
def index():
    return render_template("grid.html")

@app.route("/data")
def data():
    """
    Returns up to 1000 security records from Redis.
    Each record is stored as a hash under keys with prefix "security:".
    Only the first 8 key fields are returned.
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
    Renders the detail page for a security.
    Expects query parameters for the 8 key fields.
    Queries Postgres for the latest full record and all version history.
    """
    key_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = { field: request.args.get(field, "") for field in key_fields }
    record = get_latest_security_record(params)
    versions = get_all_security_versions(params)
    return render_template("security_detail.html", record=record, versions=versions)

@app.route("/security_detail_json")
def security_detail_json():
    """
    Returns JSON for a specific version.
    Expects the 8 key fields and an APPLIED_DATE.
    """
    key_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = { field: request.args.get(field, "") for field in key_fields }
    applied_date = request.args.get("APPLIED_DATE", "")
    record = get_security_record_by_date(params, applied_date)
    return jsonify(record)


@app.route("/security_versions")
def security_versions():
    """
    Returns JSON for version history.
    Each object includes APPLIED_DATE and the 8 key fields.
    """
    key_fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = { field: request.args.get(field, "") for field in key_fields }
    versions = get_all_security_versions(params)
    output = [{"APPLIED_DATE": rec["APPLIED_DATE"],
               "FIGI": rec["FIGI"],
               "CUSIP": rec["CUSIP"],
               "SEDOL": rec["SEDOL"],
               "ISIN": rec["ISIN"],
               "COMPANY_NAME": rec["COMPANY_NAME"],
               "CURRENCY": rec["CURRENCY"],
               "ASSET_CLASS": rec["ASSET_CLASS"],
               "ASSET_GROUP": rec["ASSET_GROUP"]} for rec in versions]
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)