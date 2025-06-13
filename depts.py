import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# SQL Server connection string
CONN_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={DB_HOST},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASS};"
    "Connection Timeout=30;"
)

try:
    # Connect to SQL Server
    conn = pyodbc.connect(CONN_STRING)
    cursor = conn.cursor()
    print("✅ Connected to SQL Server")

    # Read Excel File
    df = pd.read_excel("main.xlsx")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")

    # Ensure required columns exist
    if "department" not in df.columns or "service_station_name" not in df.columns:
        raise ValueError("Excel must contain 'department' and 'service_station_name' columns")

    # Create tables if they don't exist
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments' AND xtype='U')
    CREATE TABLE departments (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) UNIQUE NOT NULL
    );
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='service_stations' AND xtype='U')
    CREATE TABLE service_stations (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) UNIQUE NOT NULL
    );
    """)
    conn.commit()

    # Insert unique departments
    departments = df["department"].dropna().unique()
    for dept in departments:
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM departments WHERE name = ?) INSERT INTO departments (name) VALUES (?)", dept, dept)
    print(f"✅ Inserted {len(departments)} unique departments")

    # Insert unique service stations
    stations = df["service_station_name"].dropna().unique()
    for station in stations:
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM service_stations WHERE name = ?) INSERT INTO service_stations (name) VALUES (?)", station, station)
    print(f"✅ Inserted {len(stations)} unique service stations")

    conn.commit()

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()
    print("✅ Connection closed")
