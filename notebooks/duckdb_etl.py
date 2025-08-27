import duckdb

def run():
    con = duckdb.connect()
    con.execute("CREATE TABLE orders AS SELECT * FROM read_csv_auto('data/orders_large.csv')")

    con.execute("""
        ALTER TABLE orders ADD COLUMN total_amount DOUBLE;
        UPDATE orders SET total_amount = quantity * price;
    """)

    category_sales = con.execute("SELECT category, SUM(total_amount) FROM orders GROUP BY category").fetchdf()

    monthly_sales = con.execute("""
        SELECT 
            strftime(order_date, '%Y-%m') AS month, 
            SUM(total_amount)
        FROM orders GROUP BY month
    """).fetchdf()

    top_products = con.execute("""
        SELECT product, SUM(total_amount) AS revenue
        FROM orders GROUP BY product
        ORDER BY revenue DESC LIMIT 5
    """).fetchdf()

    category_sales.to_csv("output/duckdb_category_sales.csv")
    monthly_sales.to_csv("output/duckdb_monthly_sales.csv")
    top_products.to_csv("output/duckdb_top_products.csv")