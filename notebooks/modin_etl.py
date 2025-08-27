import modin.pandas as pd
import pandas as pd

def run():
    df = pd.read_csv("data/orders_large.csv")
    df["total_amount"] = df["quantity"] * df["price"]

    category_sales = df.groupby("category")["total_amount"].sum()
    monthly_sales = df.groupby(df["order_date"].str[:7])["total_amount"].sum()
    top_products = df.groupby("product")["total_amount"].sum().nlargest(5)

    category_sales.to_csv("output/modin_pandas_category_sales.csv")
    monthly_sales.to_csv("output/modin_pandas_monthly_sales.csv")
    top_products.to_csv("output/modin_pandas_top_products.csv")