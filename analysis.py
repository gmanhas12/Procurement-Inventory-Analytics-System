import sqlite3
from datetime import datetime

DB_NAME = "advanced_procurement.db"  # Make sure this matches your actual DB file

def connect_db():
    return sqlite3.connect(DB_NAME)

def total_spend_per_vendor(conn):
    print("\n📊 Total Spend per Vendor:")
    query = """
    SELECT v.vendor_name, ROUND(SUM(p.unit_price * o.quantity), 2) AS total_spend
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY v.vendor_name
    ORDER BY total_spend DESC
    """
    cursor = conn.execute(query)
    for vendor, spend in cursor.fetchall():
        print(f"{vendor}: ${spend}")

def average_delivery_delay(conn):
    print("\n⏱️ Average Delivery Delay (in days):")
    query = """
    SELECT AVG(JULIANDAY(delivery_date) - JULIANDAY(order_date)) FROM orders WHERE delivery_date IS NOT NULL
    """
    cursor = conn.execute(query)
    avg_delay = cursor.fetchone()[0]
    print(f"{avg_delay:.2f} days" if avg_delay else "No deliveries recorded")

def delayed_order_rate(conn):
    print("\n⚠️ Delayed Order Rate by Vendor:")
    query = """
    SELECT v.vendor_name,
           COUNT(CASE WHEN o.status = 'Delayed' THEN 1 END) * 100.0 / COUNT(*) AS delay_rate
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    GROUP BY v.vendor_name
    """
    cursor = conn.execute(query)
    for vendor, rate in cursor.fetchall():
        print(f"{vendor}: {rate:.2f}% delayed")

def pending_orders(conn):
    print("\n📦 Pending Orders:")
    query = """
    SELECT o.order_id, v.vendor_name, p.product_name, o.order_date
    FROM orders o
    JOIN vendors v ON o.vendor_id = v.vendor_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.status = 'Pending'
    """
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    if not rows:
        print("No pending orders.")
    for order_id, vendor, product, order_date in rows:
        print(f"Order {order_id} - {vendor} - {product} (Ordered: {order_date})")

def current_stock_levels(conn):
    print("\n📦 Current Stock Levels:")
    query = """
    SELECT p.product_name, SUM(s.quantity) as current_stock
    FROM stock_movements s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.product_name
    """
    cursor = conn.execute(query)
    for product, stock in cursor.fetchall():
        print(f"{product}: {stock}")

def overdue_invoices(conn):
    print("\n⏰ Overdue Invoices:")
    query = """
    SELECT i.invoice_id, o.order_id, v.vendor_name, i.due_date
    FROM invoices i
    JOIN orders o ON i.order_id = o.order_id
    JOIN vendors v ON o.vendor_id = v.vendor_id
    WHERE i.status = 'Overdue'
    """
    cursor = conn.execute(query)
    rows = cursor.fetchall()
    if not rows:
        print("No overdue invoices.")
    for invoice_id, order_id, vendor, due_date in rows:
        print(f"Invoice {invoice_id} for Order {order_id} - {vendor}, Due: {due_date}")

def vendor_performance_scores(conn):
    print("\n🏆 Vendor Performance Scores:")
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
    cursor = conn.execute(query)
    for vendor, score in cursor.fetchall():
        print(f"{vendor}: {score}")

def main():
    print("🔗 Connecting to database...")
    conn = connect_db()
    try:
        total_spend_per_vendor(conn)
        average_delivery_delay(conn)
        delayed_order_rate(conn)
        pending_orders(conn)
        current_stock_levels(conn)
        overdue_invoices(conn)
        vendor_performance_scores(conn)
    finally:
        conn.close()
        print("\n🔒 Database connection closed.")

if __name__ == "__main__":
    main()
