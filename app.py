from flask import Flask, render_template, request, send_file, url_for
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import os
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime
from math import ceil
from sqlalchemy import create_engine
import pyodbc

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Prevent caching of chart images

# Database configuration for cloud-hosted SQL Server (Azure SQL)
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),  # Use your Azure SQL or cloud server host
    'user': os.getenv('DB_USER'),  # Your database username
    'password': os.getenv('DB_PASS'),  # Your database password
    'database': os.getenv('DB_NAME'),  # Your database name
    'driver': 'ODBC Driver 17 for SQL Server'  # This driver is cloud-managed
}

# Cloud-based connection string using pyodbc (ODBC Driver managed by cloud)
DB_CONNECTION_STRING = f"mssql+pyodbc://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?driver={DB_CONFIG['driver']}"

# Create SQLAlchemy engine with the connection string
engine = create_engine(DB_CONNECTION_STRING)

# Constants
ITEMS_PER_PAGE = 20  # Number of items per page for pagination

def get_dropdown_options():
    """Fetch all dropdown options from the database"""
    with engine.connect() as conn:
        departments = pd.read_sql("SELECT id, name FROM departments ORDER BY name", conn)
        stations = pd.read_sql("SELECT id, name, region FROM service_stations ORDER BY name", conn)
        regions = pd.read_sql("SELECT DISTINCT region FROM service_stations WHERE region IS NOT NULL ORDER BY region", conn)
        products = pd.read_sql("SELECT DISTINCT product FROM fuel_transactions WHERE product IS NOT NULL ORDER BY product", conn)
    
    return {
        'departments': departments.to_dict('records'),
        'stations': stations.to_dict('records'),
        'regions': regions['region'].tolist(),
        'products': products['product'].tolist()
    }

def get_fuel_data(filters, page=1, per_page=ITEMS_PER_PAGE):
    """Get filtered fuel data with pagination"""
    base_query = """
    SELECT 
        ft.date, ft.vehicle_registration, d.name AS department,
        s.name AS service_station, s.region, ft.product, ft.quantity,
        ft.customer_amount, ft.terminal_price
    FROM fuel_transactions ft
    LEFT JOIN departments d ON ft.department_id = d.id
    LEFT JOIN service_stations s ON ft.service_station_id = s.id
    WHERE 1=1
    """
    
    count_query = """
    SELECT COUNT(*) as total
    FROM fuel_transactions ft
    LEFT JOIN departments d ON ft.department_id = d.id
    LEFT JOIN service_stations s ON ft.service_station_id = s.id
    WHERE 1=1
    """
    
    params = []
    conditions = []
    
    if filters.get('vehicle_reg'):
        conditions.append("ft.vehicle_registration LIKE %s")
        params.append(f"%{filters['vehicle_reg']}%")
    
    if filters.get('department'):
        conditions.append("d.id = %s")
        params.append(filters['department'])
    
    if filters.get('service_station'):
        conditions.append("s.id = %s")
        params.append(filters['service_station'])
    
    if filters.get('region'):
        conditions.append("s.region = %s")
        params.append(filters['region'])
    
    if filters.get('product'):
        conditions.append("ft.product = %s")
        params.append(filters['product'])
    
    if filters.get('start_date') and filters.get('end_date'):
        conditions.append("ft.date BETWEEN %s AND %s")
        params.extend([filters['start_date'], filters['end_date']])
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
        count_query += " AND " + " AND ".join(conditions)
    
    # Get total count for pagination
    with engine.connect() as conn:
        total = pd.read_sql(count_query, conn, params=params).iloc[0]['total']
    
    # Add pagination
    base_query += " ORDER BY ft.date DESC"
    base_query += f" OFFSET {(page-1)*per_page} ROWS FETCH NEXT {per_page} ROWS ONLY"
    
    with engine.connect() as conn:
        df = pd.read_sql(base_query, conn, params=params)
    
    return df, total

def generate_charts(df, chart_dir='static/charts'):
    """Generate and save charts"""
    os.makedirs(chart_dir, exist_ok=True)
    
    # Clear old charts
    for f in os.listdir(chart_dir):
        os.remove(os.path.join(chart_dir, f))
    
    # Only generate charts if we have data
    if df.empty:
        return None
    
    charts = {}
    
    # Quantity by Department
    if 'department' in df.columns and not df['department'].isnull().all():
        plt.figure(figsize=(12, 6))
        dept_qty = df.groupby('department')['quantity'].sum().nlargest(10)
        if not dept_qty.empty:
            ax = sns.barplot(x=dept_qty.index, y=dept_qty.values, palette='Blues_r')
            plt.title('Top 10 Departments by Fuel Quantity')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(chart_dir, 'dept_qty.png'), bbox_inches='tight')
            plt.close()
            charts['dept_qty'] = url_for('static', filename='charts/dept_qty.png')
    
    # Revenue by Department
    if 'department' in df.columns and not df['department'].isnull().all():
        plt.figure(figsize=(12, 6))
        dept_rev = df.groupby('department')['customer_amount'].sum().nlargest(10)
        if not dept_rev.empty:
            ax = sns.barplot(x=dept_rev.index, y=dept_rev.values, palette='Greens_r')
            plt.title('Top 10 Departments by Revenue')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(chart_dir, 'dept_rev.png'), bbox_inches='tight')
            plt.close()
            charts['dept_rev'] = url_for('static', filename='charts/dept_rev.png')
    
    # Fuel by Region
    if 'region' in df.columns and not df['region'].isnull().all():
        plt.figure(figsize=(12, 6))
        region_qty = df.groupby('region')['quantity'].sum().sort_values(ascending=False)
        if not region_qty.empty:
            ax = sns.barplot(x=region_qty.index, y=region_qty.values, palette='Reds_r')
            plt.title('Fuel Consumption by Region')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(chart_dir, 'region_qty.png'), bbox_inches='tight')
            plt.close()
            charts['region_qty'] = url_for('static', filename='charts/region_qty.png')
    
    # Products by Volume
    if 'product' in df.columns and not df['product'].isnull().all():
        plt.figure(figsize=(12, 6))
        product_qty = df.groupby('product')['quantity'].sum().nlargest(10)
        if not product_qty.empty:
            ax = sns.barplot(x=product_qty.index, y=product_qty.values, palette='Purples_r')
            plt.title('Top 10 Products by Volume')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(os.path.join(chart_dir, 'product_qty.png'), bbox_inches='tight')
            plt.close()
            charts['product_qty'] = url_for('static', filename='charts/product_qty.png')
    
    return charts

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    page = request.args.get('page', 1, type=int)
    
    filters = {
        'vehicle_reg': request.values.get('vehicle_reg', ''),
        'department': request.values.get('department', ''),
        'service_station': request.values.get('service_station', ''),
        'region': request.values.get('region', ''),
        'product': request.values.get('product', ''),
        'start_date': request.values.get('start_date', ''),
        'end_date': request.values.get('end_date', '')
    }
    
    # Get filtered data
    df, total = get_fuel_data(filters, page)
    options = get_dropdown_options()
    
    # Generate summary
    summary = {
        'transactions': "{:,}".format(total),
        'total_quantity': f"{df['quantity'].sum():,.2f} L" if not df.empty else "0.00 L",
        'total_revenue': f"KES {df['customer_amount'].sum():,.2f}" if not df.empty else "KES 0.00",
    }
    
    charts = generate_charts(df) if not df.empty else None

    # Calculate pagination
    total_pages = ceil(total / ITEMS_PER_PAGE)
    pagination = {
        'page': page,
        'per_page': ITEMS_PER_PAGE,
        'total': total,
        'total_pages': total_pages
    }

    return render_template(
        'dashboard.html',
        data=df.to_dict('records'),
        filters=filters,
        options=options,
        summary=summary,
        charts=charts,
        pagination=pagination  # Pass pagination to template
    )

@app.route('/export')
def export_data():
    filters = {
        'vehicle_reg': request.args.get('vehicle_reg'),
        'department': request.args.get('department'),
        'service_station': request.args.get('service_station'),
        'region': request.args.get('region'),
        'product': request.args.get('product'),
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date')
    }
    
    # Get all data for export
    df, _ = get_fuel_data(filters, page=1, per_page=1000000)
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Fuel Data')
    
    output.seek(0)
    filename = f"fuel_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
