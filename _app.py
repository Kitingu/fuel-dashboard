from flask import Flask, render_template, request, send_file
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
from sqlalchemy import create_engine, text
from io import BytesIO
from dotenv import load_dotenv

# 🔹 Fix Matplotlib error
matplotlib.use("Agg")

# 🔹 Load environment variables from .env fil
load_dotenv()
# Flask App
app = Flask(__name__)

# 🔹 Use SQLAlchemy for PostgreSQL connection
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

print("Loaded DB config:")
print(f"{DB_USER=}, {DB_PASS=}, {DB_HOST=}, {DB_PORT=}, {DB_NAME=}")

DB_CONFIG = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_CONFIG)
# DB_CONFIG = "postgresql://postgres:postgres@localhost:5432/ingestion_db"
# engine = create_engine(DB_CONFIG)

# Function to Fetch Data with Filters
def get_fuel_data(vehicle_reg=None, department=None, service_station=None, start_date=None, end_date=None):
    query = "SELECT * FROM fuel_transactions WHERE 1=1"
    params = {}

    if vehicle_reg:
        query += " AND vehicle_registration ILIKE :vehicle_reg"
        params["vehicle_reg"] = f"%{vehicle_reg}%"

    if department:
        query += " AND department ILIKE :department"
        params["department"] = f"%{department}%"

    if service_station:
        query += " AND service_station ILIKE :service_station"
        params["service_station"] = f"%{service_station}%"

    if start_date and end_date:
        query += " AND date BETWEEN :start_date AND :end_date"
        params["start_date"] = start_date
        params["end_date"] = end_date

    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn, params=params)

    return df

# Function to Generate Charts
def generate_charts(df):
    os.makedirs("static/charts", exist_ok=True)

    ### 🔹 First Chart: Top quantity per department
    plt.figure(figsize=(10, 5))

    # 🔹 Group by department & sum quantity
    departmerts = df.groupby("department")["quantity"].sum()

    # 🔹 Plot top 10 service stations
    sns.barplot(x=departmerts.index, y=departmerts.values, palette="Blues_r")
    
    plt.xticks(rotation=45)
    plt.xlabel("Department")
    plt.ylabel("Total Fuel Quantity (Liters)")
    plt.title("Fuel Quantity Per department")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig("static/charts/fuel_quantity_chart.png")
    plt.close()

    ### 🔹 Second Chart: Total Revenue by Department
    plt.figure(figsize=(10, 5))

    revenue_by_department = df.groupby("department")["customer_amount"].sum()

# 🔹 Plot revenue by department
    sns.barplot(x=revenue_by_department.index, y=revenue_by_department.values, palette="Greens_r")

    plt.xticks(rotation=45)
    plt.xlabel("Department")
    plt.ylabel("Total Amount (KES, in millions)")

    # Format y-axis to show values in millions
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M'))

    plt.title("Total Amount by Department")
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig("static/charts/revenue_chart.png")
    plt.close()
    
def get_dropdown_options():
    with engine.connect() as conn:
        departments = pd.read_sql("SELECT DISTINCT department FROM fuel_transactions ORDER BY department", conn)
        stations = pd.read_sql("SELECT DISTINCT service_station FROM fuel_transactions ORDER BY service_station", conn)
    return departments["department"].dropna().tolist(), stations["service_station"].dropna().tolist()


# 🔹 Route for Exporting Filtered Data to Excel
@app.route("/export")
def export_data():
    vehicle_reg = request.args.get("vehicle_reg")
    department = request.args.get("department")
    service_station = request.args.get("service_station")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    df = get_fuel_data(vehicle_reg, department, service_station, start_date, end_date)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Fuel Data")
    
    output.seek(0)
    
    return send_file(output, download_name="filtered_fuel_data.xlsx", as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# Flask Route with Search, Filters & Data Table
@app.route("/", methods=["GET", "POST"])
def dashboard():
    filters = {
        "vehicle_reg": request.form.get("vehicle_reg", ""),
        "department": request.form.get("department", ""),
        "service_station": request.form.get("service_station", ""),
        "start_date": request.form.get("start_date", ""),
        "end_date": request.form.get("end_date", ""),
    }

    df = get_fuel_data(**filters)
    departments, stations = get_dropdown_options()

    summary = {
        "Total Transactions": f"{df.shape[0]:,}",
        "Total Quantity": f"{df['quantity'].sum():,.2f}",
        "Total Revenue (KES)": f"{df['customer_amount'].sum():,.2f}",
        "Average Price per Liter": f"{df['terminal_price'].mean():,.2f}" if not df.empty else "0.00"
    }

    generate_charts(df)

    return render_template("dashboard.html", summary=summary, data=df.to_dict(orient="records"), filters=filters, departments=departments, stations=stations)




    # Generate Charts
    generate_charts(df)

    return render_template("dashboard.html", summary=summary, data=df.to_dict(orient="records"), filters=filters)

# Run Flask App
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
