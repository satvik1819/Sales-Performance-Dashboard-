# etl_load_superstore.py  (updated to avoid deprecated VALUES())
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

CSV_PATH = "Sample - Superstore.csv"   # your dataset file

DB_CONFIG = {
    'user': 'root',        # <-- change this
    'password': 'sathvik1234',    # <-- change this
    'host': '127.0.0.1',
    'database': 'sales_db',
    'raise_on_warnings': True
}

def main():
    # Load CSV
    df = pd.read_csv(CSV_PATH, encoding='latin1')
    df.columns = [c.strip() for c in df.columns]  # clean column names

    # Fix date formats (Superstore uses m/d/yyyy)
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce')

    # Convert numeric values
    df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce').fillna(0)
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(0).astype(int)
    df['Discount'] = pd.to_numeric(df['Discount'], errors='coerce').fillna(0)
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce').fillna(0)

    # Trim whitespace in string columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()

    # Connect to MySQL
    cnx = mysql.connector.connect(**DB_CONFIG)
    cursor = cnx.cursor()

    print("Loading customers...")

    # ---------------------------- CUSTOMERS (using VALUES alias 'new') ----------------------------
    customer_sql = """
        INSERT INTO customers
          (customer_id, customer_name, segment, country, state, city, postal_code, region)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) AS new
        ON DUPLICATE KEY UPDATE
            customer_name = new.customer_name,
            segment = new.segment,
            country = new.country,
            state = new.state,
            city = new.city,
            postal_code = new.postal_code,
            region = new.region
    """

    cust_df = df[['Customer ID','Customer Name','Segment','Country','State','City','Postal Code','Region']].drop_duplicates()
    for _, r in cust_df.iterrows():
        cursor.execute(customer_sql, (
            r['Customer ID'],
            r['Customer Name'],
            r['Segment'],
            r['Country'],
            r['State'],
            r['City'],
            str(r['Postal Code']),
            r['Region']
        ))

    print("Loading products...")

    # ---------------------------- PRODUCTS ----------------------------
    product_sql = """
        INSERT INTO products (product_id, product_name, category, subcategory)
        VALUES (%s,%s,%s,%s) AS new
        ON DUPLICATE KEY UPDATE
            product_name = new.product_name,
            category = new.category,
            subcategory = new.subcategory
    """

    prod_df = df[['Product ID','Product Name','Category','Sub-Category']].drop_duplicates()
    for _, r in prod_df.iterrows():
        cursor.execute(product_sql, (
            r['Product ID'],
            r['Product Name'],
            r['Category'],
            r['Sub-Category']
        ))

    print("Loading orders...")

    # ---------------------------- ORDERS ----------------------------
    order_sql = """
        INSERT INTO orders (order_id, order_date, ship_date, ship_mode)
        VALUES (%s,%s,%s,%s) AS new
        ON DUPLICATE KEY UPDATE
            order_date = new.order_date,
            ship_date = new.ship_date,
            ship_mode = new.ship_mode
    """

    ord_df = df[['Order ID','Order Date','Ship Date','Ship Mode']].drop_duplicates()
    for _, r in ord_df.iterrows():
        cursor.execute(order_sql, (
            r['Order ID'],
            r['Order Date'].strftime('%Y-%m-%d') if not pd.isna(r['Order Date']) else None,
            r['Ship Date'].strftime('%Y-%m-%d') if not pd.isna(r['Ship Date']) else None,
            r['Ship Mode']
        ))

    print("Loading order items...")

    # ---------------------------- ORDER ITEMS ----------------------------
    item_sql = """
        INSERT INTO order_items
        (order_id, product_id, customer_id, sales, quantity, discount, profit)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    for _, r in df.iterrows():
        cursor.execute(item_sql, (
            r['Order ID'],
            r['Product ID'],
            r['Customer ID'],
            float(r['Sales']),
            int(r['Quantity']),
            float(r['Discount']),
            float(r['Profit'])
        ))

    print("Committing changes...")
    cnx.commit()

    cursor.close()
    cnx.close()
    print("ETL Finished Successfully ✔")

if __name__ == "__main__":
    main()
