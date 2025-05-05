import pandas as pd
import pyodbc
import numpy as np
import os
from dotenv import load_dotenv

# SQL Server Configuration
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
    "Login Timeout=15;"
)

try:
    # Connect to SQL Server
    conn = pyodbc.connect(CONN_STRING)
    cursor = conn.cursor()
    print("✅ Successfully connected to SQL Server")

    # Path to Excel File
    EXCEL_FILE = "main.xlsx"  # Change this if needed

    # Read Excel Data
    df = pd.read_excel(EXCEL_FILE)

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("(", "").str.replace(")", "")

    # Rename columns correctly
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
        "customer_amount": "customer_amount",
        "region": "region"
    }
    df = df.rename(columns=column_mapping)

    # Ensure required columns exist
    missing_columns = [col for col in column_mapping.values() if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing Columns in DataFrame: {missing_columns}")

    # Convert date & time formats
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y", errors="coerce").dt.date
    df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S", errors="coerce").dt.time

    # Replace `NaT` with `None` (NULL in SQL Server)
    df["date"] = df["date"].where(pd.notna(df["date"]), None)
    df["time"] = df["time"].where(pd.notna(df["time"]), None)

    # Convert numeric columns, replacing invalid values with None
    numeric_columns = ["quantity", "full_tank_capacity", "terminal_price", "customer_amount"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")  # Convert invalid numbers to NaN
        df[col] = df[col].replace({np.nan: None})  # Convert NaN to None for SQL Server

    # Create Table with Indexes
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fuel_transactions' AND xtype='U')
    CREATE TABLE fuel_transactions (
        id INT IDENTITY(1,1) PRIMARY KEY,
        date DATE,
        time TIME,
        vehicle_registration NVARCHAR(255),
        department NVARCHAR(255),
        truck_model NVARCHAR(255),
        service_provider NVARCHAR(255),
        service_station NVARCHAR(255),
        product NVARCHAR(255),
        quantity FLOAT,
        full_tank_capacity FLOAT,
        terminal_price FLOAT,
        customer_amount FLOAT,
        region NVARCHAR(255)
    );
    """
    cursor.execute(create_table_query)

    # Add Indexes for Faster Searching
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_vehicle_reg' AND object_id = OBJECT_ID('fuel_transactions'))
    CREATE INDEX idx_vehicle_reg ON fuel_transactions(vehicle_registration);
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_department' AND object_id = OBJECT_ID('fuel_transactions'))
    CREATE INDEX idx_department ON fuel_transactions(department);
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_service_station' AND object_id = OBJECT_ID('fuel_transactions'))
    CREATE INDEX idx_service_station ON fuel_transactions(service_station);
    """)
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_date' AND object_id = OBJECT_ID('fuel_transactions'))
    CREATE INDEX idx_date ON fuel_transactions(date);
    """)

    conn.commit()

    # Insert Data
    insert_query = """
    INSERT INTO fuel_transactions 
    (date, time, vehicle_registration, department, truck_model, service_provider, 
     service_station, product, quantity, full_tank_capacity, terminal_price, customer_amount, region) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    # Insert each row safely
    total_rows = len(df)
    for i, (_, row) in enumerate(df.iterrows(), 1):
        try:
            values = (
                row["date"],
                row["time"],
                row["vehicle_registration"],
                row["department"],
                row["truck_model"],
                row["service_provider"],
                row["service_station"],
                row["product"],
                row["quantity"],
                row["full_tank_capacity"],
                row["terminal_price"],
                row["customer_amount"],
                row["region"]
            )
            cursor.execute(insert_query, values)
            
            # Commit every 100 rows to avoid memory issues
            if i % 100 == 0:
                conn.commit()
                print(f"✅ Processed {i}/{total_rows} rows")
                
        except Exception as e:
            print(f"❌ Error inserting row {i}: {e}")
            print(f"Problematic row data: {row.to_dict()}")
            conn.rollback()
            continue

    conn.commit()
    print(f"✅ Successfully inserted {total_rows} rows into SQL Server")

except pyodbc.OperationalError as e:
    print(f"❌ Connection failed: {e}")
    print("Troubleshooting steps:")
    print("1. Verify SQL Server is running and accessible")
    print("2. Check network connectivity (can you ping the server?)")
    print("3. Ensure port 1433 (or your custom port) is open")
    print("4. Verify credentials in your .env file")
    print("5. Check SQL Server error logs for more details")
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("✅ Database connection closed")