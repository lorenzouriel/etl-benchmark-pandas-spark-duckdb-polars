from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()
print(spark.version)

def run():
    df = spark.read.csv("data/orders_large.csv", header=True, inferSchema=True)
    df = df.withColumn("total_amount", col("quantity") * col("price"))

    category_sales = df.groupBy("category").sum("total_amount")
    monthly_sales = df.groupBy(year("order_date").alias("year"), month("order_date").alias("month")).sum("total_amount")

    top_products = df.groupBy("product").sum("total_amount").orderBy(col("sum(total_amount)").desc()).limit(5)

    category_sales.toPandas().to_csv("output/spark_category_sales.csv", index=False)
    monthly_sales.toPandas().to_csv("output/spark_monthly_sales.csv", index=False)
    top_products.toPandas().to_csv("output/spark_top_products.csv", index=False)