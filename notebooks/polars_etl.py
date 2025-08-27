import polars as pl

def run():
    df = pl.read_csv("data/orders_large.csv")
    df = df.with_columns((df["quantity"] * df["price"]).alias("total_amount"))

    category_sales = df.group_by("category").agg(pl.sum("total_amount"))
    monthly_sales = df.group_by(df["order_date"].str.slice(0, 7)).agg(pl.sum("total_amount"))
    top_products = df.group_by("product").agg(pl.sum("total_amount")).sort("total_amount", descending=True).head(5)

    category_sales.write_csv("output/polars_category_sales.csv")
    monthly_sales.write_csv("output/polars_monthly_sales.csv")
    top_products.write_csv("output/polars_top_products.csv")