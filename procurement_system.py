import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "advanced_procurement.db"

def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Drop tables if exist
    for table in ["invoices", "stock_movements", "orders", "products", "vendors"]:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")

    # Create tables
    cursor.execute("""
    CREATE TABLE vendors (
        vendor_id INTEGER PRIMARY KEY,
        vendor_name TEXT
    )""")
    
    cursor.execute("""
    CREATE TABLE products (
        product_id INTEGER PRIMARY KEY,
        product_name TEXT,
        unit_price REAL
    )""")
    
    cursor.execute("""
    CREATE TABLE orders (
        order_id INTEGER PRIMARY KEY,
        vendor_id INTEGER,
        product_id INTEGER,
        order_date TEXT,
        delivery_date TEXT,
        quantity INTEGER,
        status TEXT,
        FOREIGN KEY (vendor_id) REFERENCES vendors(vendor_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )""")
    
    cursor.execute("""
    CREATE TABLE stock_movements (
        movement_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        movement_date TEXT,
        quantity INTEGER,
        type TEXT CHECK(type IN ('IN', 'OUT')),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )""")
    
    cursor.execute("""
    CREATE TABLE invoices (
        invoice_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        invoice_date TEXT,
        due_date TEXT,
        paid_date TEXT,
        status TEXT CHECK(status IN ('Paid', 'Unpaid', 'Overdue')),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )""")

    # Insert sample data
    vendors = [
        (1, "Green Supply Co."),
        (2, "EcoSmart Vendors"),
        (3, "SolarTech Solutions")
    ]
    cursor.executemany("INSERT INTO vendors VALUES (?, ?)", vendors)
    
    products = [
        (1, "Smart Thermostat", 199.99),
        (2, "Solar Panel", 499.99),
        (3, "Heat Pump", 799.99),
        (4, "Battery Pack", 299.99)
    ]
    cursor.executemany("INSERT INTO products VALUES (?, ?, ?)", products)
    
    orders = [
        (1, 1, 1, "2025-07-01", "2025-07-05", 10, "Delivered"),
        (2, 1, 2, "2025-07-01", "2025-07-10", 5, "Delivered"),
        (3, 2, 3, "2025-07-03", "2025-07-08", 3, "Delivered"),
        (4, 2, 1, "2025-07-07", "2025-07-18", 7, "Delayed"),
        (5, 3, 4, "2025-07-05", "2025-07-20", 6, "Delivered"),
        (6, 3, 3, "2025-07-10", None, 2, "Pending")
    ]
    cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", orders)
    
    stock_movements = [
        (1, 1, "2025-07-05", 10, "IN"),
        (2, 2, "2025-07-10", 5, "IN"),
        (3, 3, "2025-07-08", 3, "IN"),
        (4, 1, "2025-07-18", 7, "IN"),
        (5, 4, "2025-07-20", 6, "IN"),
        (6, 1, "2025-07-22", -4, "OUT"),
        (7, 2, "2025-07-22", -2, "OUT"),
        (8, 3, "2025-07-22", -1, "OUT")
    ]
    cursor.executemany("INSERT INTO stock_movements VALUES (?, ?, ?, ?, ?)", stock_movements)
    
    invoices = [
        (1, 1, "2025-07-05", "2025-07-10", "2025-07-09", "Paid"),
        (2, 2, "2025-07-10", "2025-07-20", "2025-07-19", "Paid"),
        (3, 3, "2025-07-08", "2025-07-15", None, "Unpaid"),
        (4, 4, "2025-07-18", "2025-07-25", None, "Overdue"),
        (5, 5, "2025-07-20", "2025-07-28", None, "Unpaid")
    ]
    cursor.executemany("INSERT INTO invoices VALUES (?, ?, ?, ?, ?, ?)", invoices)
    
    conn.commit()
    conn.close()
    print("Database setup complete.")

def run_queries():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Total spend per vendor
    cursor.execute("""
    SELECT v.vendor_name, ROUND(SUM(p.unit_price * o.quantity), 2) AS total_spend
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY v.vendor_name
    ORDER BY total_spend DESC
    """)
    spend = cursor.fetchall()
    print("Total Spend per Vendor:")
    for v, s in spend:
        print(f"  {v}: ${s}")

    # Average delivery delay
    cursor.execute("""
    SELECT AVG(JULIANDAY(delivery_date) - JULIANDAY(order_date)) 
    FROM orders 
    WHERE delivery_date IS NOT NULL
    """)
    avg_delay = cursor.fetchone()[0]
    print(f"\nAverage Delivery Delay (days): {avg_delay:.2f}")

    # Delayed order rate by vendor
    cursor.execute("""
    SELECT v.vendor_name,
           COUNT(CASE WHEN o.status = 'Delayed' THEN 1 END) * 100.0 / COUNT(*) AS delay_rate
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    """)
    print("\nDelayed Order Rate by Vendor:")
    for v, rate in cursor.fetchall():
        print(f"  {v}: {rate:.2f}%")

    # Pending orders
    cursor.execute("""
    SELECT o.order_id, v.vendor_name, p.product_name, o.order_date
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.status = 'Pending'
    """)
    pending = cursor.fetchall()
    print("\nPending Orders:")
    for order_id, vendor, product, order_date in pending:
        print(f"  Order {order_id} - {vendor} - {product} (Ordered: {order_date})")

    # Current stock per product
    cursor.execute("""
    SELECT p.product_name, SUM(s.quantity) as current_stock
    FROM stock_movements s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_name
    """)
    print("\nCurrent Stock Levels:")
    for product, stock in cursor.fetchall():
        print(f"  {product}: {stock}")

    # Overdue invoices
    cursor.execute("""
    SELECT i.invoice_id, o.order_id, v.vendor_name, i.due_date
    FROM invoices i
    JOIN orders o ON i.order_id = o.order_id
    JOIN vendors v ON o.vendor_id = v.vendor_id
    WHERE i.status = 'Overdue'
    """)
    print("\nOverdue Invoices:")
    for invoice_id, order_id, vendor, due_date in cursor.fetchall():
        print(f"  Invoice {invoice_id} for Order {order_id} - {vendor}, Due: {due_date}")

    # Vendor performance score (simple example)
    # Score = (100 - delay_rate) weighted by total spend percentile
    cursor.execute("""
    WITH vendor_spend AS (
        SELECT v.vendor_id, SUM(p.unit_price * o.quantity) as spend
        FROM orders o
        JOIN vendors v ON o.vendor_id = v.vendor_id
        JOIN products p ON o.product_id = p.product_id
        GROUP BY v.vendor_id
    ),
    vendor_delays AS (
        SELECT v.vendor_id,
               COUNT(CASE WHEN o.status = 'Delayed' THEN 1 END) * 100.0 / COUNT(*) AS delay_rate
        FROM orders o
        JOIN vendors v ON o.vendor_id = v.vendor_id
        GROUP BY v.vendor_id
    )
    SELECT v.vendor_name,
           ROUND(
               (100 - COALESCE(d.delay_rate, 0)) * 0.5 + 
               50 * (sp.spend / (SELECT MAX(spend) FROM vendor_spend)),
               2
           ) AS performance_score
    FROM vendors v
    LEFT JOIN vendor_spend sp ON v.vendor_id = sp.vendor_id
    LEFT JOIN vendor_delays d ON v.vendor_id = d.vendor_id
    ORDER BY performance_score DESC
    """)
    print("\nVendor Performance Scores:")
    for vendor_name, score in cursor.fetchall():
        print(f"  {vendor_name}: {score}")

    conn.close()

def export_reports():
    conn = sqlite3.connect(DB_NAME)
    
    # Export vendor performance to CSV
    query = """
    WITH vendor_spend AS (
        SELECT v.vendor_id, SUM(p.unit_price * o.quantity) as spend
        FROM orders o
        JOIN vendors v ON o.vendor_id = v.vendor_id
        JOIN products p ON o.product_id = p.product_id
        GROUP BY v.vendor_id
    ),
    vendor_delays AS (
        SELECT v.vendor_id,
               COUNT(CASE WHEN o.status = 'Delayed' THEN 1 END) * 100.0 / COUNT(*) AS delay_rate
        FROM orders o
        JOIN vendors v ON o.vendor_id = v.vendor_id
        GROUP BY v.vendor_id
    )
    SELECT v.vendor_name,
           ROUND(
               (100 - COALESCE(d.delay_rate, 0)) * 0.5 + 
               50 * (sp.spend / (SELECT MAX(spend) FROM vendor_spend)),
               2
           ) AS performance_score
    FROM vendors v
    LEFT JOIN vendor_spend sp ON v.vendor_id = sp.vendor_id
    LEFT JOIN vendor_delays d ON v.vendor_id = d.vendor_id
    ORDER BY performance_score DESC
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv("vendor_performance.csv", index=False)
    print("\nExported vendor performance to 'vendor_performance.csv'")

    # Export alerts to text file
    alerts = []

    # Low stock alert (<10 units)
    stock_query = """
    SELECT p.product_name, SUM(s.quantity) as current_stock
    FROM stock_movements s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_name
    HAVING current_stock < 10
    """
    low_stock_df = pd.read_sql_query(stock_query, conn)
    for _, row in low_stock_df.iterrows():
        alerts.append(f"LOW STOCK ALERT: {row['product_name']} stock is low ({row['current_stock']} units)")

    # Overdue invoice alert
    overdue_query = """
    SELECT i.invoice_id, o.order_id, v.vendor_name, i.due_date
    FROM invoices i
    JOIN orders o ON i.order_id = o.order_id
    JOIN vendors v ON o.vendor_id = v.vendor_id
    WHERE i.status = 'Overdue'
    """
    overdue_df = pd.read_sql_query(overdue_query, conn)
    for _, row in overdue_df.iterrows():
        alerts.append(f"OVERDUE INVOICE ALERT: Invoice {row['invoice_id']} for Order {row['order_id']} from {row['vendor_name']} was due {row['due_date']}")

    # Write alerts to file
    with open("alerts.txt", "w") as f:
        for alert in alerts:
            f.write(alert + "\n")

    print("Exported alerts to 'alerts.txt'")
    conn.close()

if __name__ == "__main__":
    setup_database()
    run_queries()
    export_reports()
