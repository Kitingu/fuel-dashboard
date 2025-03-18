import pandas as pd
import psycopg2
import numpy as np

# PostgreSQL Configuration
DB_CONFIG = {
    "dbname": "ingestion_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

# Path to Excel File
EXCEL_FILE = "main.xlsx"  # Change this if needed

# Read Excel Data
df = pd.read_excel(EXCEL_FILE)

# 🔹 Normalize column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")

# 🔹 Rename columns correctly
column_mapping = {
    "date": "date",
    "time": "time",
    "vehicle_registration_number": "vehicle_registration",
    "department": "department",
    "truck_model": "truck_model",
    "service_provider": "service_provider",
    "service_station_name": "service_station",
    "product/service": "product",
    "quantity": "quantity",
    "full_tank_capacity": "full_tank_capacity",
    "terminal_price": "terminal_price",
    "customer_amount": "customer_amount"
}
df = df.rename(columns=column_mapping)

# 🔹 Ensure required columns exist
missing_columns = [col for col in column_mapping.values() if col not in df.columns]
if missing_columns:
    raise ValueError(f"Missing Columns in DataFrame: {missing_columns}")

# 🔹 Convert date & time formats
df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce").dt.date
df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time

# 🔹 Replace `NaT` with `None` (NULL in PostgreSQL)
df["date"] = df["date"].where(pd.notna(df["date"]), None)
df["time"] = df["time"].where(pd.notna(df["time"]), None)

# 🔹 Convert numeric columns, replacing invalid values with None
numeric_columns = ["quantity", "full_tank_capacity", "terminal_price", "customer_amount"]

for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")  # Convert invalid numbers to NaN
    df[col] = df[col].replace({np.nan: None})  # Convert NaN to None for PostgreSQL

# 🔹 Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# 🔹 Create Table with Indexes
create_table_query = """
CREATE TABLE IF NOT EXISTS fuel_transactions (
    id SERIAL PRIMARY KEY,
    date DATE,
    time TIME,
    vehicle_registration TEXT,
    department TEXT,
    truck_model TEXT,
    service_provider TEXT,
    service_station TEXT,
    product TEXT,
    quantity REAL,
    full_tank_capacity REAL,
    terminal_price REAL,
    customer_amount REAL
);
"""
cursor.execute(create_table_query)

# 🔹 Add Indexes for Faster Searching
cursor.execute("CREATE INDEX IF NOT EXISTS idx_vehicle_reg ON fuel_transactions(vehicle_registration);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_department ON fuel_transactions(department);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_service_station ON fuel_transactions(service_station);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON fuel_transactions(date);")

conn.commit()

# 🔹 Insert Data
insert_query = """
INSERT INTO fuel_transactions 
(date, time, vehicle_registration, department, truck_model, service_provider, service_station, product, quantity, full_tank_capacity, terminal_price, customer_amount) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# 🔹 Insert each row safely
for _, row in df.iterrows():
    values = (
        row["date"],
        row["time"],  # 🔹 `NaT` will be replaced with `None`
        row["vehicle_registration"],
        row["department"],
        row["truck_model"],
        row["service_provider"],
        row["service_station"],
        row["product"],
        row["quantity"],
        row["full_tank_capacity"],
        row["terminal_price"],
        row["customer_amount"]
    )
    cursor.execute(insert_query, values)

conn.commit()

print("✅ Data inserted successfully.")

# 🔹 Close Connection
cursor.close()
conn.close()
