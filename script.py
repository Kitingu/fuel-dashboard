import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
from time import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import decimal
import numpy as np
import warnings
import re

# Suppress warnings
warnings.filterwarnings('ignore')

# Configuration
load_dotenv()
CHUNK_SIZE = 400
RETRY_ATTEMPTS = 5
MAX_STRING_LENGTH = 255
MAX_DECIMAL_PRECISION = 10

DB_CONFIG = {
    'driver': os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
    'server': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"),
    'timeout': 120,
    'login_timeout': 60,
    'autocommit': False
}

def get_connection_string(config):
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"Connection Timeout={config['timeout']};"
        f"Login Timeout={config['login_timeout']};"
    )

@retry(
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=5, max=60),
    retry=retry_if_exception_type((pyodbc.OperationalError, pyodbc.InterfaceError, pyodbc.Error))
)
def connect_to_sql():
    print("⏳ Connecting to SQL Server...")
    conn_str = get_connection_string(DB_CONFIG)
    return pyodbc.connect(conn_str)

def validate_and_convert_value(value, col_type):
    if pd.isna(value) or value is None:
        return None
    
    try:
        if col_type == 'string':
            str_val = str(value).strip()[:MAX_STRING_LENGTH]
            return str_val if str_val else None
        elif col_type == 'decimal':
            str_val = f"{float(value):.{MAX_DECIMAL_PRECISION-2}f}"
            return decimal.Decimal(str_val).quantize(
                decimal.Decimal(f"1.{'0' * (MAX_DECIMAL_PRECISION-2)}"),
                rounding=decimal.ROUND_HALF_UP
            )
        elif col_type == 'date':
            dt = pd.to_datetime(value, errors='coerce')
            return dt.date() if pd.notna(dt) else None
        elif col_type == 'time':
            dt = pd.to_datetime(value, errors='coerce')
            return dt.time() if pd.notna(dt) else None
        else:
            return str(value)[:MAX_STRING_LENGTH] if value else None
    except (ValueError, TypeError, decimal.InvalidOperation):
        return None

def prepare_data(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[()]", "", regex=True)
    
    column_specs = {
        "date": ("date", 'date'),
        "time": ("time", 'time'),
        "vehicle_registration_number": ("vehicle_registration", 'string'),
        "department": ("department", 'string'),
        "truck_model": ("truck_model", 'string'),
        "service_provider": ("service_provider", 'string'),
        "service_station_name": ("service_station", 'string'),
        "product/service": ("product", 'string'),
        "quantity": ("quantity", 'decimal'),
        "full_tank_capacity": ("full_tank_capacity", 'decimal'),
        "terminal_price": ("terminal_price", 'decimal'),
        "customer_amount": ("customer_amount", 'decimal'),
        "region": ("region", 'string')
    }
    
    processed_df = pd.DataFrame()
    for old_name, (new_name, col_type) in column_specs.items():
        if old_name in df.columns:
            processed_df[new_name] = df[old_name].apply(lambda x: validate_and_convert_value(x, col_type))
        else:
            print(f"⚠️ Column '{old_name}' not found in DataFrame")
    
    # Data quality checks
    if 'department' in processed_df.columns:
        null_depts = processed_df['department'].isna().sum()
        if null_depts > 0:
            print(f"⚠️ Warning: {null_depts} null department values after processing")
        empty_depts = processed_df['department'].apply(lambda x: x is not None and str(x).strip() == '').sum()
        if empty_depts > 0:
            print(f"⚠️ Warning: {empty_depts} empty department strings after processing")
    
    return processed_df.replace({np.nan: None})

def create_tables(cursor, conn):
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departments' AND xtype='U')
        CREATE TABLE departments (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255) UNIQUE NOT NULL
        );
    
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='service_stations' AND xtype='U')
        CREATE TABLE service_stations (
            id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255) UNIQUE NOT NULL,
            region NVARCHAR(255)
        );
    
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fuel_transactions' AND xtype='U')
        CREATE TABLE fuel_transactions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date DATE,
            time TIME,
            vehicle_registration NVARCHAR(255),
            department_id INT FOREIGN KEY REFERENCES departments(id),
            truck_model NVARCHAR(255),
            service_provider NVARCHAR(255),
            service_station_id INT FOREIGN KEY REFERENCES service_stations(id),
            product NVARCHAR(255),
            quantity DECIMAL(10,2),
            full_tank_capacity DECIMAL(10,2),
            terminal_price DECIMAL(10,2),
            customer_amount DECIMAL(12,2),
            region NVARCHAR(255)
        );
    """)

    indexes = [
        ('idx_vehicle_reg', 'vehicle_registration'),
        ('idx_department_id', 'department_id'),
        ('idx_service_station_id', 'service_station_id'),
        ('idx_date', 'date'),
        ('idx_product', 'product')
    ]
    
    for idx_name, col in indexes:
        cursor.execute(f"""
        IF NOT EXISTS (
            SELECT * FROM sys.indexes 
            WHERE name=? AND object_id = OBJECT_ID('fuel_transactions')
        )
        CREATE INDEX {idx_name} ON fuel_transactions({col})
        """, idx_name)
    
    conn.commit()
    print("✅ Tables and indexes created successfully")

def normalize_name(name):
    """Normalize a name by removing extra spaces and special characters."""
    if pd.isna(name) or name is None:
        return None
    try:
        str_name = str(name).strip()
        str_name = re.sub(r'[^\w\s]', '', str_name)
        str_name = re.sub(r'\s+', ' ', str_name)
        return str_name.upper() if str_name else None
    except Exception as e:
        print(f"Error normalizing name '{name}': {e}")
        return None

@retry(
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=5, max=60),
    retry=retry_if_exception_type((pyodbc.OperationalError, pyodbc.Error))
)
def get_or_create_name_ids_bulk(cursor, names, table, region_map=None):
    if not names or all(name is None for name in names):
        print(f"No {table} names provided or all are None")
        return {}
    
    name_mapping = {}
    original_names = {}
    for name in names:
        if pd.isna(name) or name is None:
            continue
        norm_name = normalize_name(name)
        if norm_name:
            name_mapping[norm_name] = str(name).strip()
            original_names[str(name).strip()] = norm_name
    
    if not name_mapping:
        print(f"No valid {table} names after cleaning")
        return {}
    
    print(f"Processing {len(name_mapping)} {table} names")
    print(f"Sample names (original): {list(name_mapping.values())[:5]}")
    print(f"Sample names (normalized): {list(name_mapping.keys())[:5]}")
    
    # Check existing records using original names
    params = [name_mapping[norm_name] for norm_name in name_mapping]
    placeholders = ','.join(['?'] * len(params))
    existing = {}
    if params:
        try:
            cursor.execute(
                f"SELECT id, name FROM {table} WHERE name IN ({placeholders})",
                params
            )
            existing = {name: id_ for id_, name in cursor.fetchall()}
            print(f"Found {len(existing)} existing {table} records")
        except pyodbc.Error as e:
            print(f"Error checking existing {table} records: {e}")
            raise
    
    new_names = [norm_name for norm_name in name_mapping if name_mapping[norm_name] not in existing]
    print(f"Found {len(new_names)} new {table} names to insert: {[name_mapping[n] for n in new_names[:10]]}")
    
    if new_names:
        for norm_name in new_names:
            name = name_mapping.get(norm_name)
            if not name:
                print(f"⚠️ Skipping insertion for norm_name '{norm_name}' not found in name_mapping")
                continue
            try:
                if table == "service_stations" and region_map:
                    cursor.execute(
                        f"INSERT INTO {table} (name, region) VALUES (?, ?)",
                        name, region_map.get(name)
                    )
                else:
                    cursor.execute(
                        f"INSERT INTO {table} (name) VALUES (?)",
                        name
                    )
                print(f"Successfully inserted: '{name}' into {table}")
            except pyodbc.IntegrityError as e:
                print(f"Duplicate detected for '{name}' in {table}: {e}")
                continue
            except pyodbc.Error as e:
                print(f"❌ Failed to insert '{name}' into {table}: {e}")
                raise
    
        cursor.connection.commit()
    
        # Re-fetch all names using original names
        try:
            cursor.execute(
                f"SELECT id, name FROM {table} WHERE name IN ({placeholders})",
                params
            )
            existing = {name: id_ for id_, name in cursor.fetchall()}
            print(f"Retrieved IDs for {len(existing)} {table} records after insertion")
        except pyodbc.Error as e:
            print(f"Error retrieving {table} IDs after insertion: {e}")
            raise
    
    result = {}
    unmapped = []
    for orig_name in original_names:
        if orig_name in existing:
            result[orig_name] = existing[orig_name]
        else:
            unmapped.append(orig_name)
    
    if unmapped:
        print(f"⚠️ {len(unmapped)} unmapped {table} names: {unmapped[:10]}...")
    
    print(f"Returning {len(result)} {table} ID mappings")
    return result

def verify_database_state(cursor):
    """Verify what's actually in the database"""
    try:
        cursor.execute("SELECT COUNT(*) FROM departments")
        dept_count = cursor.fetchone()[0]
        print(f"Total departments in database: {dept_count}")
        
        cursor.execute("SELECT id, name, UPPER(TRIM(name)) as norm_name FROM departments")
        dept_info = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
        print(f"All departments (id, original, normalized): {dept_info}")
        
        cursor.execute("SELECT COUNT(*) FROM service_stations")
        station_count = cursor.fetchone()[0]
        print(f"Total service stations in database: {station_count}")
        
        cursor.execute("SELECT id, name, region FROM service_stations")
        station_info = [(row[0], row[1], row[2]) for row in cursor.fetchall()]
        print(f"All service stations (id, name, region): {station_info}")
    except Exception as e:
        print(f"Error verifying database state: {e}")

def insert_reference_data(df):
    try:
        conn = connect_to_sql()
        cursor = conn.cursor()
        
        create_tables(cursor, conn)
        
        region_map = dict(zip(
            df['service_station'].str.strip(), 
            df['region'].str.strip()
        )) if 'region' in df.columns else None
        
        departments = df['department'].dropna().unique().tolist()
        service_stations = df['service_station'].dropna().unique().tolist()
        
        print(f"\nUnique departments to process ({len(departments)}): {departments}")
        print(f"\nUnique service stations to process ({len(service_stations)}): {service_stations}")
        
        print(f"\n⏳ Inserting {len(departments)} departments...")
        dept_ids = get_or_create_name_ids_bulk(cursor, departments, "departments")
        print(f"✅ Inserted {len(dept_ids)} departments")
        print(f"Department ID mappings: {dept_ids}")
        
        print(f"\n⏳ Inserting {len(service_stations)} service stations...")
        station_ids = get_or_create_name_ids_bulk(
            cursor, 
            service_stations, 
            "service_stations",
            region_map
        )
        print(f"✅ Inserted {len(station_ids)} service stations")
        print(f"Service station ID mappings: {station_ids}")
        
        print("\nVerifying database state:")
        verify_database_state(cursor)
        
        return dept_ids, station_ids
        
    except Exception as e:
        print(f"❌ Error inserting reference data: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("Reference data connection closed")

def enrich_with_foreign_keys(df, dept_ids, station_ids):
    df = df.copy()
    
    if "department" not in df.columns:
        raise ValueError("Required column 'department' not found in DataFrame")
    if "service_station" not in df.columns:
        raise ValueError("Required column 'service_station' not found in DataFrame")
    
    # Debug: Print sample department and service station names
    print(f"Sample department names in DataFrame: {df['department'].dropna().unique()[:5]}")
    print(f"Sample service station names in DataFrame: {df['service_station'].dropna().unique()[:5]}")
    print(f"Available department IDs: {dept_ids}")
    print(f"Available service station IDs: {station_ids}")
    
    # Map department IDs using original names
    df["department_id"] = df["department"].apply(
        lambda x: dept_ids.get(str(x).strip() if pd.notna(x) else None, None)
    )
    unmapped_depts = df[df["department_id"].isna() & df["department"].notna()]["department"].unique()
    if len(unmapped_depts) > 0:
        print(f"⚠️ Warning: {len(unmapped_depts)} unmapped departments: {unmapped_depts[:10]}...")
        for dept in unmapped_depts:
            norm_dept = normalize_name(dept)
            print(f"  - '{dept}' (normalized: '{norm_dept}')")
        df["department_id"] = df["department_id"].fillna(-1).astype(int).replace(-1, None)
    
    # Map service station IDs using original names
    df["service_station_id"] = df["service_station"].apply(
        lambda x: station_ids.get(str(x).strip() if pd.notna(x) else None, None)
    )
    unmapped_stations = df[df["service_station_id"].isna() & df["service_station"].notna()]["service_station"].unique()
    if len(unmapped_stations) > 0:
        print(f"⚠️ Warning: {len(unmapped_stations)} unmapped service stations: {unmapped_stations[:10]}...")
        for station in unmapped_stations:
            norm_station = normalize_name(station)
            print(f"  - '{station}' (normalized: '{norm_station}')")
        df["service_station_id"] = df["service_station_id"].fillna(-1).astype(int).replace(-1, None)
    
    return df

def insert_fuel_transactions(cursor, df):
    insert_cols = [
        'date', 'time', 'vehicle_registration', 'department_id', 'truck_model', 
        'service_provider', 'service_station_id', 'product', 'quantity', 
        'full_tank_capacity', 'terminal_price', 'customer_amount', 'region'
    ]

    placeholders = ','.join(['?'] * len(insert_cols))
    insert_sql = f"""
    INSERT INTO fuel_transactions ({', '.join(insert_cols)}) 
    VALUES ({placeholders})
    """

    rows = []
    for _, row in df.iterrows():
        values = [row.get(col) if pd.notna(row.get(col)) else None for col in insert_cols]
        rows.append(values)

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i+CHUNK_SIZE]
        try:
            cursor.executemany(insert_sql, chunk)
            cursor.connection.commit()
            print(f"Inserted chunk {i // CHUNK_SIZE + 1} with {len(chunk)} records")
        except pyodbc.Error as e:
            print(f"❌ Error inserting chunk {i // CHUNK_SIZE + 1}: {e}")
            cursor.connection.rollback()
            raise

def insert_transaction_data(df, dept_ids, station_ids):
    try:
        conn = connect_to_sql()
        cursor = conn.cursor()
        
        print("DataFrame columns before enrichment:", df.columns.tolist())
        df_fk = enrich_with_foreign_keys(df, dept_ids, station_ids)
        
        insert_fuel_transactions(cursor, df_fk)
        
    except Exception as e:
        print(f"❌ Error inserting transaction data: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("Transaction data connection closed")

def main():
    try:
        df = pd.read_excel("main.xlsx")
        print(f"Loaded {len(df)} rows")
        print(f"Input DataFrame columns: {df.columns.tolist()}")
    
        print("\nPreparing data...")
        df_clean = prepare_data(df)
        print("✅ Data preparation completed")
    
        print("\nData Quality Check:")
        print(f"Total rows: {len(df_clean)}")
        if 'department' in df_clean.columns:
            unique_depts = df_clean['department'].nunique()
            print(f"Unique departments found: {unique_depts}")
            print("Sample departments:", df_clean['department'].dropna().unique()[:10])
        if 'service_station' in df_clean.columns:
            unique_stations = df_clean['service_station'].nunique()
            print(f"Unique service stations found: {unique_stations}")
            print("Sample service stations:", df_clean['service_station'].dropna().unique()[:10])
    
        print("\n=== PHASE 1: Inserting Reference Data ===")
        dept_ids, station_ids = insert_reference_data(df_clean)
        
        print("\n=== PHASE 2: Inserting Transaction Data ===")
        insert_transaction_data(df_clean, dept_ids, station_ids)
        
        print("\n✅ All data inserted successfully!")
    except Exception as e:
        print(f"\n❌ Failed to complete data insertion: {e}")
        raise

if __name__ == "__main__":
    main()