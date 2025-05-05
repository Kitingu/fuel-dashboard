from flask import Flask, render_template, request, send_file
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import os
from sqlalchemy import create_engine, text
from io import BytesIO
from dotenv import load_dotenv

# 🔹 Fix Matplotlib error
matplotlib.use("Agg")

# 🔹 Load environment variables from .env file
load_dotenv()

# Flask App
app = Flask(__name__)

# 🔹 Use SQLAlchemy for SQL Server connection
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

print("Loaded DB config:")
print(f"{DB_USER=}, {DB_PASS=}, {DB_HOST=}, {DB_PORT=}, {DB_NAME=}")

# SQL Server connection string using pyodbc driver
DB_CONFIG = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONFIG)

# Function to Fetch Data with Filters
def get_fuel_data(vehicle_reg=None, department=None, service_station=None, start_date=None, end_date=None):
    query = "SELECT * FROM fuel_transactions WHERE 1=1"
    params = {}

    if vehicle_reg:
        query += " AND vehicle_registration LIKE :vehicle_reg"
        params["vehicle_reg"] = f"%{vehicle_reg}%"

    if department:
        query += " AND department LIKE :department"
        params["department"] = f"%{department}%"

    if service_station:
        query += " AND service_station LIKE :service_station"
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
    departments = df.groupby("department")["quantity"].sum()

    # 🔹 Plot top 10 service stations
    sns.barplot(x=departments.index, y=departments.values, palette="Blues_r")
    
    plt.xticks(rotation=45)
    plt.xlabel("Department")
    plt.ylabel("Total Fuel Quantity (Liters)")
    plt.title("Fuel Quantity Per Department")
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

    # Check if "Clear" button was clicked (all filters empty)
    if request.method == "POST" and not any(filters.values()):
        filters = {key: "" for key in filters}  # Reset filters to empty

    df = get_fuel_data(**filters)

    # Summary Stats
    summary = {
        "Total Transactions": df.shape[0],
        "Total Quantity": df["quantity"].sum(),
        "Total Revenue (KES)": df["customer_amount"].sum(),
        "Average Price per Liter": df["terminal_price"].mean() if not df.empty else 0,
    }

    # Generate Charts
    generate_charts(df)

    return render_template("dashboard.html", summary=summary, data=df.to_dict(orient="records"), filters=filters)

# Run Flask App
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)