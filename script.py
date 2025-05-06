import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
from time import sleep, time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import decimal
import numpy as np
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Configuration
load_dotenv()
CHUNK_SIZE = 300  # Optimal balance between speed and reliability
RETRY_ATTEMPTS = 5
MAX_STRING_LENGTH = 255  # Maximum length for string fields
MAX_DECIMAL_PRECISION = 10  # Maximum precision for decimal values

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
    conn_str = get_connection_string(DB_CONFIG)
    return pyodbc.connect(conn_str)

def validate_and_convert_value(value, col_type):
    """Validate and convert values based on their expected type"""
    if pd.isna(value) or value is None:
        return None
    
    try:
        if col_type == 'string':
            # Truncate strings to maximum allowed length
            str_val = str(value).strip()[:MAX_STRING_LENGTH]
            return str_val if str_val else None
        elif col_type == 'decimal':
            # Handle decimal conversion with proper precision
            str_val = f"{float(value):.{MAX_DECIMAL_PRECISION-2}f}"
            return decimal.Decimal(str_val).quantize(
                decimal.Decimal(f"1.{'0' * (MAX_DECIMAL_PRECISION-2)}"), 
                rounding=decimal.ROUND_HALF_UP
            )
        elif col_type == 'date':
            return pd.to_datetime(value, errors='coerce').date() if pd.notna(value) else None
        elif col_type == 'time':
            return pd.to_datetime(value, errors='coerce').time() if pd.notna(value) else None
        else:
            return str(value)[:MAX_STRING_LENGTH] if value else None
    except (ValueError, TypeError, decimal.InvalidOperation):
        return None

def prepare_data(df):
    # Clean column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[()]", "", regex=True)
    
    # Standardize column names and types
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
    
    # Apply column renaming and type conversion
    processed_df = pd.DataFrame()
    for old_name, (new_name, col_type) in column_specs.items():
        if old_name in df.columns:
            processed_df[new_name] = df[old_name].apply(lambda x: validate_and_convert_value(x, col_type))
    
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
            name NVARCHAR(255) UNIQUE NOT NULL
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

    # Create indexes
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

def get_or_create_name_ids_bulk(cursor, names, table):
    """Optimized bulk lookup and insert for names with duplicate handling"""
    if not names or all(name is None for name in names):
        return {}

    # Clean and get unique non-null names (case-insensitive, trimmed)
    clean_names = {str(name).strip() for name in names if name is not None and str(name).strip()}
    if not clean_names:
        return {}

    # Create mapping from clean name to original name (preserve original case)
    name_mapping = {str(name).strip().upper(): str(name).strip() for name in names if name is not None}
    unique_names = list(clean_names)

    # Check existing names in one query (case-insensitive comparison)
    params = ','.join(['?'] * len(unique_names))
    cursor.execute(
        f"SELECT id, UPPER(TRIM(name)) FROM {table} WHERE UPPER(TRIM(name)) IN ({params})", 
        [name.upper() for name in unique_names]
    )
    existing = {name.upper(): id_ for id_, name in cursor.fetchall()}

    # Find new names to insert (case-insensitive check)
    new_names = [name for name in unique_names if name.upper() not in existing]

    if new_names:
        # Insert new names with original casing (but trimmed)
        try:
            cursor.executemany(
                f"INSERT INTO {table} (name) VALUES (?)", 
                [(name,) for name in new_names]
            )
            cursor.commit()
            
            # Get IDs for newly inserted names
            cursor.execute(
                f"SELECT id, UPPER(TRIM(name)) FROM {table} WHERE UPPER(TRIM(name)) IN ({params})", 
                [name.upper() for name in new_names]
            )
            existing.update({name.upper(): id_ for id_, name in cursor.fetchall()})
        except pyodbc.IntegrityError:
            # If duplicate was inserted concurrently, refresh existing names
            cursor.execute(
                f"SELECT id, UPPER(TRIM(name)) FROM {table} WHERE UPPER(TRIM(name)) IN ({params})", 
                [name.upper() for name in unique_names]
            )
            existing = {name.upper(): id_ for id_, name in cursor.fetchall()}
        except pyodbc.Error as e:
            print(f"⚠️ Error inserting {table} names: {e}")
            conn.rollback()

    # Return mapping from original names to IDs
    return {name_mapping[k]: v for k, v in existing.items() if k in name_mapping}

def enrich_with_foreign_keys(df, dept_ids, station_ids):
    """Replace department/station names with their IDs using case-insensitive matching"""
    df["department_id"] = df["department"].apply(
        lambda x: dept_ids.get(str(x).strip(), None) if x is not None else None
    )
    df["service_station_id"] = df["service_station"].apply(
        lambda x: station_ids.get(str(x).strip(), None) if x is not None else None
    )
    return df.drop(columns=["department", "service_station"])

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(pyodbc.Error)
)
def insert_chunk(cursor, chunk, query):
    """Insert a chunk of data with error handling and retries"""
    try:
        cursor.fast_executemany = True
        cursor.executemany(query, chunk)
        return len(chunk), 0
    except pyodbc.Error as bulk_error:
        print(f"⚠️ Bulk insert failed, falling back to row-by-row: {bulk_error}")
        cursor.fast_executemany = False
        success_count = 0
        failed_rows = []
        
        for row in chunk:
            try:
                cursor.execute(query, row)
                success_count += 1
            except pyodbc.Error as row_error:
                failed_rows.append((row, str(row_error)))
        
        if failed_rows:
            print(f"❌ Failed to insert {len(failed_rows)} rows in chunk")
            for row, error in failed_rows[:3]:  # Print first 3 errors to avoid flooding
                print(f"Problematic row: {row}\nError: {error}")
        
        return success_count, len(failed_rows)

def process_data():
    start_time = time()
    conn = None
    cursor = None
    total_failed = 0
    
    try:
        # Establish database connection
        conn = connect_to_sql()
        cursor = conn.cursor()
        print("✅ Successfully connected to SQL Server")

        # Load and prepare data
        print("⏳ Loading and preparing data...")
        df = pd.read_excel("main.xlsx")
        df = prepare_data(df)
        
        # Create tables if they don't exist
        create_tables(cursor, conn)
        
        # Process departments and stations in bulk
        print("⏳ Processing departments and service stations...")
        dept_ids = get_or_create_name_ids_bulk(cursor, df["department"].tolist(), "departments")
        station_ids = get_or_create_name_ids_bulk(cursor, df["service_station"].tolist(), "service_stations")
        
        # Replace names with foreign keys
        df = enrich_with_foreign_keys(df, dept_ids, station_ids)
        
        # Prepare insert query
        insert_query = """
        INSERT INTO fuel_transactions 
        (date, time, vehicle_registration, department_id, truck_model, service_provider, 
         service_station_id, product, quantity, full_tank_capacity, terminal_price, customer_amount, region)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Convert to list of tuples for efficient insertion
        data = [tuple(x) for x in df.to_numpy()]
        total_rows = len(data)
        inserted_rows = 0
        
        print(f"⏳ Starting bulk insert of {total_rows} rows in chunks of {CHUNK_SIZE}...")
        
        # Process data in chunks
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk = data[i:i+CHUNK_SIZE]
            chunk_size = len(chunk)
            
            try:
                # Insert chunk with retry logic
                success_count, failed_count = insert_chunk(cursor, chunk, insert_query)
                conn.commit()
                inserted_rows += success_count
                total_failed += failed_count
                
                progress = (i + min(CHUNK_SIZE, total_rows - i)) / total_rows * 100
                print(f"✅ Inserted {i+1}-{i+success_count} ({success_count}/{chunk_size} rows) [{progress:.1f}%]")
                
                # Small delay to prevent server overload
                sleep(0.2)
                
            except Exception as e:
                conn.rollback()
                print(f"❌ Chunk insert error: {e}")
                # Try to reconnect if connection was lost
                try:
                    cursor.close()
                    conn.close()
                    conn = connect_to_sql()
                    cursor = conn.cursor()
                    print("✅ Reconnected to SQL Server")
                except Exception as reconnect_error:
                    print(f"❌ Failed to reconnect: {reconnect_error}")
                    raise
        
        # Final statistics
        elapsed = time() - start_time
        rate = inserted_rows / elapsed if elapsed > 0 else 0
        print(f"\n🎉 Successfully inserted {inserted_rows}/{total_rows} rows "
              f"in {elapsed:.2f} seconds ({rate:.1f} rows/sec)")
        print(f"💾 Storage efficiency: Departments: {len(dept_ids)}, Stations: {len(station_ids)}")
        if total_failed > 0:
            print(f"⚠️ Failed to insert {total_failed} rows ({(total_failed/total_rows)*100:.2f}%)")

    except Exception as e:
        print(f"❌ Critical error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("✅ Database connection closed")

if __name__ == "__main__":
    process_data()