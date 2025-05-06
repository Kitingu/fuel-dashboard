from flask import Flask, render_template, request, send_file
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import os
from sqlalchemy import create_engine, text
from io import BytesIO
from dotenv import load_dotenv
from functools import lru_cache

# Fix matplotlib backend for servers
matplotlib.use("Agg")

# Load environment variables
load_dotenv()

app = Flask(__name__)

# DB connection using SQLAlchemy + pyodbc
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_CONFIG = f"mssql+pyodbc://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONFIG)

@lru_cache(maxsize=1)
def get_dropdown_options():
    with engine.connect() as conn:
        departments = pd.read_sql("SELECT name FROM departments ORDER BY name", conn)
        stations = pd.read_sql("SELECT name FROM service_stations ORDER BY name", conn)
    return (
        departments["name"].dropna().tolist(),
        stations["name"].dropna().tolist()
    )

def get_fuel_data(vehicle_reg=None, department=None, service_station=None, start_date=None, end_date=None):
    query = """
    SELECT 
        ft.id, ft.date, ft.time, ft.vehicle_registration,
        d.name AS department, ft.truck_model, ft.service_provider,
        s.name AS service_station, ft.product, ft.quantity,
        ft.full_tank_capacity, ft.terminal_price, ft.customer_amount, ft.region
    FROM fuel_transactions ft
    LEFT JOIN departments d ON ft.department_id = d.id
    LEFT JOIN service_stations s ON ft.service_station_id = s.id
    WHERE 1=1
    """
    params = {}

    if vehicle_reg:
        query += " AND ft.vehicle_registration LIKE :vehicle_reg"
        params["vehicle_reg"] = f"%{vehicle_reg}%"
    if department:
        query += " AND d.name LIKE :department"
        params["department"] = f"%{department}%"
    if service_station:
        query += " AND s.name LIKE :service_station"
        params["service_station"] = f"%{service_station}%"
    if start_date and end_date:
        query += " AND ft.date BETWEEN :start_date AND :end_date"
        params["start_date"] = start_date
        params["end_date"] = end_date

    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn, params=params)

    return df

def generate_charts(df):
    os.makedirs("static/charts", exist_ok=True)

    # Chart 1: Quantity per department
    plt.figure(figsize=(10, 5))
    dept_group = df.groupby("department")["quantity"].sum().sort_values(ascending=False).head(10)
    sns.barplot(x=dept_group.index, y=dept_group.values, palette="Blues_r")
    plt.xticks(rotation=45)
    plt.xlabel("Department")
    plt.ylabel("Fuel Quantity (Liters)")
    plt.title("Top 10 Departments by Fuel Quantity")
    plt.tight_layout()
    plt.savefig("static/charts/fuel_quantity_chart.png")
    plt.close()

    # Chart 2: Revenue per department
    plt.figure(figsize=(10, 5))
    rev_group = df.groupby("department")["customer_amount"].sum().sort_values(ascending=False).head(10)
    sns.barplot(x=rev_group.index, y=rev_group.values, palette="Greens_r")
    plt.xticks(rotation=45)
    plt.xlabel("Department")
    plt.ylabel("Revenue (KES Millions)")
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M'))
    plt.title("Top 10 Departments by Revenue")
    plt.tight_layout()
    plt.savefig("static/charts/revenue_chart.png")
    plt.close()

@app.route("/export")
def export_data():
    filters = {
        "vehicle_reg": request.args.get("vehicle_reg"),
        "department": request.args.get("department"),
        "service_station": request.args.get("service_station"),
        "start_date": request.args.get("start_date"),
        "end_date": request.args.get("end_date"),
    }

    df = get_fuel_data(**filters)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Fuel Data")
    output.seek(0)

    return send_file(
        output,
        download_name="filtered_fuel_data.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.route("/", methods=["GET", "POST"])
def dashboard():
    filters = {
        "vehicle_reg": request.form.get("vehicle_reg", ""),
        "department": request.form.get("department", ""),
        "service_station": request.form.get("service_station", ""),
        "start_date": request.form.get("start_date", ""),
        "end_date": request.form.get("end_date", ""),
    }

    if request.method == "POST" and not any(filters.values()):
        filters = {key: "" for key in filters}  # Reset filters

    df = get_fuel_data(**filters)
    departments, stations = get_dropdown_options()

    summary = {
        "Total Transactions": f"{df.shape[0]:,}",
        "Total Quantity": f"{df['quantity'].sum():,.2f}",
        "Total Revenue (KES)": f"{df['customer_amount'].sum():,.2f}",
        "Avg Price/Liter": f"{df['terminal_price'].mean():,.2f}" if not df.empty else "0.00"
    }

    generate_charts(df)

    return render_template(
        "dashboard.html",
        summary=summary,
        data=df.to_dict(orient="records"),
        filters=filters,
        departments=departments,
        stations=stations
    )

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
