from flask import Flask, render_template, request, jsonify
import io, csv, re
import redis, psycopg2, datetime
from flask_socketio import SocketIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)

### Redis Setup ###
redis_client = redis.Redis(host="localhost", port=6379, db=0)

### Postgres Helpers ###
def get_latest_security_record(params):
    fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM security_master 
          WHERE {where_clause}
          ORDER BY applied_date DESC LIMIT 1;
          """
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="jez", password="",
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
    fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT DISTINCT ON (applied_date) *
          FROM security_master 
          WHERE {where_clause}
          ORDER BY applied_date DESC;
          """
    values = [params.get(field, "") for field in fields]
    try:
        conn = psycopg2.connect(dbname="postgres", user="jez", password="",
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
    fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    where_clause = " AND ".join([f'"{field}" = %s' for field in fields])
    sql = f"""
          SELECT * FROM security_master 
          WHERE {where_clause} AND applied_date = %s 
          LIMIT 1;
          """
    values = [params.get(field, "") for field in fields] + [applied_date]
    try:
        conn = psycopg2.connect(dbname="postgres", user="jez", password="",
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

### Custom Jinja Filter to detect type ###
@app.template_filter('detect_type')
def detect_type(value):
    try:
        int(value)
        return "Integer"
    except Exception:
        pass
    try:
        float(value)
        return "Float"
    except Exception:
        pass
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d")
        return "Date"
    except Exception:
        pass
    if value.lower() in ['true', 'false']:
        return "Boolean"
    return "String"

### Flask Routes ###
@app.route("/")
def index():
    return render_template("grid.html")

@app.route("/dataz")
def dataz():
    keys = redis_client.keys("*")
    records = []
    for key in keys:
        rec = redis_client.hgetall(key)
        record = {k.decode("utf-8"): v.decode("utf-8") for k, v in rec.items()}
        filtered = {field: record.get(field, "") for field in [
            "figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"
        ]}
        records.append(filtered)
    return jsonify(records)

@app.route("/data")
def data():
    # Define the key fields to return (including APPLIED_DATE)
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group", "applied_date"]
    
    # Retrieve filter values from the query string (if any)
    filter_asset_class = request.args.get("asset_class", "").strip()
    filter_asset_group = request.args.get("asset_group", "").strip()
    
    # Fetch up to 1000 keys from Redis
    keys = redis_client.keys("*")[:1000]
    records = []
    for key in keys:
        rec = redis_client.hgetall(key)
        # Decode each field from bytes to strings
        record = {k.decode("utf-8"): v.decode("utf-8") for k, v in rec.items()}
        # Apply asset_class filter if provided
        if filter_asset_class and record.get("asset_class", "").strip() != filter_asset_class:
            continue
        # Apply asset_group filter if provided
        if filter_asset_group and record.get("asset_group", "").strip() != filter_asset_group:
            continue
        # Include only the specified key fields in the output
        filtered = {field: record.get(field, "") for field in key_fields}
        records.append(filtered)
    return jsonify(records)

@app.route("/security_detail")
def security_detail():
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    params = { field: request.args.get(field, "") for field in key_fields }
    record = get_latest_security_record(params)
    versions = get_all_security_versions(params)
    
    today = datetime.date.today()
    if versions and len(versions) > 1:
        try:
            latest_date = datetime.datetime.strptime(versions[0]["applied_date"], "%Y-%m-%d").date()
            previous_date = datetime.datetime.strptime(versions[1]["applied_date"], "%Y-%m-%d").date()
            days_since_last_update = abs((latest_date - previous_date).days)
        except Exception as e:
            print("Error computing days since last update:", e)
            days_since_last_update = "N/A"
    else:
        days_since_last_update = "N/A"
        
    try:
        applied_date = datetime.datetime.strptime(record.get("applied_date", ""), "%Y-%m-%d").date()
        age = abs((today - applied_date).days)
    except Exception as e:
        print("Error computing age:", e)
        age = "N/A"
    
    record["days_since_last_update"] = days_since_last_update
    record["age"] = age
    
    return render_template("security_detail.html", record=record, versions=versions)

@app.route("/security_detail_json")
def security_detail_json():
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    params = { field: request.args.get(field, "") for field in key_fields }
    applied_date = request.args.get("applied_date", "")
    record = get_security_record_by_date(params, applied_date)
    return jsonify(record)

@app.route("/security_versions")
def security_versions():
    key_fields = ["figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"]
    params = { field: request.args.get(field, "") for field in key_fields }
    versions = get_all_security_versions(params)
    output = [{"applied_date": rec["applied_date"],
               "figi": rec["figi"],
               "cusip": rec["cusip"],
               "sedol": rec["sedol"],
               "isin": rec["isin"],
               "company_name": rec["company_name"],
               "currency": rec["currency"],
               "asset_class": rec["asset_class"],
               "asset_group": rec["asset_group"]} for rec in versions]
    return jsonify(output)

@app.route("/company_detail")
def company_detail():
    # Get company name from query parameters
    company_name = request.args.get("company_name", "")
    # Render a new template for company detail
    return render_template("company_detail.html", company_name=company_name)

@app.route("/company_data")
def company_data():
    """Return JSON data filtered by company_name."""
    company_name = request.args.get("company_name", "").lower()
    keys = redis_client.keys("*")
    records = []
    for key in keys:
        rec = redis_client.hgetall(key)
        record = {k.decode("utf-8"): v.decode("utf-8") for k, v in rec.items()}
        # Check if the record's company_name (if exists) matches (case-insensitive)
        if record.get("company_name", "").lower() == company_name:
            filtered = {field: record.get(field, "") for field in [
                "figi", "cusip", "sedol", "isin", "company_name", "currency", "asset_class", "asset_group"
            ]}
            records.append(filtered)
    return jsonify(records)

# Define regex patterns for validation
PATTERNS = {
    "FIGI": r"^BBG[A-Z0-9]{8}\d$",
    "CUSIP": r"^[A-Z0-9*@#]{9}$",
    "SEDOL": r"^[A-Z0-9]{7}$",
    "ISIN": r"^[A-Z]{2}[A-Z0-9]{9}\d$"
}

@app.route("/upload_soi", methods=["POST"])
def upload_soi():
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    try:
        content = file.stream.read()
        try:
            decoded = content.decode("utf-8")
        except UnicodeDecodeError:
            decoded = content.decode("utf-8-sig")
        stream = io.StringIO(decoded)
    except Exception as e:
        return jsonify({"error": "File decoding error", "details": str(e)}), 400

    reader = csv.DictReader(stream)
    total_rows = 0
    error_count = 0
    error_data = []

    for i, row in enumerate(reader, start=1):
        total_rows += 1
        row_errors = []
        for field, pattern in PATTERNS.items():
            value = row.get(field, "").strip()
            # Only check if value is not empty; if it doesn't match the pattern, mark it as an error.
            if value and not re.match(pattern, value):
                row_errors.append({
                    "RowNumber": i,
                    "UniqueKey": "",  # will be computed below
                    "Field": field,
                    "FieldValue": value,
                    "Issue": "Error",
                    "Message": f"Value does not match expected pattern for {field}."
                })
        if row_errors:
            error_count += 1
            unique_key = "|".join([
                row.get("FIGI", "").strip(),
                row.get("CUSIP", "").strip(),
                row.get("SEDOL", "").strip(),
                row.get("ISIN", "").strip()
            ])
            for err in row_errors:
                err["UniqueKey"] = unique_key
            error_data.extend(row_errors)

    summary = {
        "totalRows": total_rows,
        "errorCount": error_count,
        "errorData": error_data
    }
    return jsonify(summary)



@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@app.route("/dashboard_data")
def dashboard_data():
    asset_classes = {}
    asset_groups = {}
    
    # Iterate over all keys in Redis.
    # For a large dataset, consider using a SCAN-based approach or storing aggregated counts.
    for key in redis_client.scan_iter("*"):
        try:
            record = redis_client.hgetall(key)
            # Decode the record from bytes to strings
            record = {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}
            # Get asset_class and asset_group (default to 'Unknown' if missing)
            ac = record.get("asset_class", "Unknown")
            ag = record.get("asset_group", "Unknown")
            asset_classes[ac] = asset_classes.get(ac, 0) + 1
            asset_groups[ag] = asset_groups.get(ag, 0) + 1
        except Exception as e:
            # Log or skip problematic keys
            continue

    return jsonify({
        "assetClasses": asset_classes,
        "assetGroups": asset_groups
    })

@app.route("/securities")
def securities():
    asset_class = request.args.get("asset_class", "").strip()
    asset_group = request.args.get("asset_group", "").strip()
    return render_template("grid.html", asset_class=asset_class, asset_group=asset_group)

if __name__ == "__main__":
    socketio.run(app, debug=True)