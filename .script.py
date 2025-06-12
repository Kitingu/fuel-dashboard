import pandas as pd
import pyodbc
from datetime import datetime

# Database connection parameters
server = "10.2.0.6"
database = "ProtoEnergy"
username = "readonlyuser"
password = "readonlypassword"
driver = "{ODBC Driver 17 for SQL Server}"

# SQL Server connection string
conn_str = f"""
    DRIVER={driver};
    SERVER={server};
    DATABASE={database};
    UID={username};
    PWD={password};
"""

# SQL Insert query
insert_query = """
INSERT INTO dbo.vessel_reconciliation (
    vessel_name,
    vessel_registration,
    terminal_name,
    voyage_name,
    date_time,
    transaction_type,
    quantity,
    full_tank_capacity,
    terminal_price,
    customer_name,
    customer_amount,
    payment_mode,
    remarks,
    created_at,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

def prepare_data(file_path):
    # Read Excel file
    df = pd.read_excel(file_path)

    # Standardize column names to match DB schema
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Ensure required columns exist
    required_columns = [
        "vessel_name",
        "vessel_registration",
        "terminal_name",
        "voyage_name",
        "date_time",
        "transaction_type",
        "quantity",
        "full_tank_capacity",
        "terminal_price",
        "customer_name",
        "customer_amount",
        "payment_mode",
        "remarks"
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in Excel file: {missing_cols}")

    # Clean numeric columns: remove commas, trim spaces, coerce to numeric
    numeric_cols = ["quantity", "full_tank_capacity", "terminal_price", "customer_amount"]
    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(",", "").str.strip()
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure date_time is datetime
    df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")

    # Add created_at and updated_at columns with current timestamp
    now = datetime.now()
    df["created_at"] = now
    df["updated_at"] = now

    # Replace NaN with None for SQL compatibility
    df = df.where(pd.notna(df), None)

    # Validate critical numeric fields (Optional strict check)
    for col in numeric_cols:
        if not df[col].apply(lambda x: isinstance(x, (float, int)) or x is None).all():
            raise ValueError(f"Invalid data found in {col}")

    return df

def insert_data(df):
    # Connect to SQL Server
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True  # Enable batch inserts

        # Prepare data tuples
        data_tuples = list(df[[
            "vessel_name",
            "vessel_registration",
            "terminal_name",
            "voyage_name",
            "date_time",
            "transaction_type",
            "quantity",
            "full_tank_capacity",
            "terminal_price",
            "customer_name",
            "customer_amount",
            "payment_mode",
            "remarks",
            "created_at",
            "updated_at"
        ]].itertuples(index=False, name=None))

        try:
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            print(f"Inserted {cursor.rowcount} rows successfully.")
        except pyodbc.Error as e:
            print(f"⚠️ Chunk insert failed, falling back to row-by-row: {e}")
            # Fallback to row-by-row insert for debugging
            for row in data_tuples:
                try:
                    cursor.execute(insert_query, row)
                except pyodbc.Error as row_error:
                    print(f"❌ Failed to insert row {row}: {row_error}")
            conn.commit()

if __name__ == "__main__":
    file_path = "vessel_reconciliation.xlsx"  # Change this to your actual file path
    df = prepare_data(file_path)
    insert_data(df)
