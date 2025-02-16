from flask import Flask, render_template, request, jsonify
import psycopg2
import datetime

app = Flask(__name__)

def get_latest_security_record(params):
    """
    Query Postgres for the latest record (by APPLIED_DATE) matching the given 8 key fields.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM dummy_security_master 
          WHERE {where_clause} 
          ORDER BY "APPLIED_DATE" DESC 
          LIMIT 1;
          """
    values = [params.get(field, "") for field in fields]
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

def get_all_security_versions(params):
    """
    Query Postgres for all versions of a security (matching the 8 key fields), ordered by APPLIED_DATE descending.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM dummy_security_master 
          WHERE {where_clause} 
          ORDER BY "APPLIED_DATE" DESC;
          """
    values = [params.get(field, "") for field in fields]
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
    cur = conn.cursor()
    cur.execute(sql, values)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    conn.close()
    records = [dict(zip(colnames, row)) for row in rows]
    return records

@app.route("/security_detail")
def security_detail():
    """
    Renders the security detail page.
    Expects query parameters for the 8 key fields.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = {field: request.args.get(field, "") for field in fields}
    record = get_latest_security_record(params)
    return render_template("security_detail.html", record=record)

@app.route("/security_versions")
def security_versions():
    """
    Returns JSON for all versions of a security matching the 8 key fields.
    """
    fields = ["FIGI", "CUSIP", "SEDOL", "ISIN", "COMPANY_NAME", "CURRENCY", "ASSET_CLASS", "ASSET_GROUP"]
    params = {field: request.args.get(field, "") for field in fields}
    records = get_all_security_versions(params)
    return jsonify(records)

### Grid UI Route ###
@app.route("/")
def index():
    return render_template("grid.html")

if __name__ == "__main__":
    app.run(debug=True)