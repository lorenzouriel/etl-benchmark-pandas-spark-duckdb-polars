import time
from notebooks.pandas_etl import run as pandas_etl
from notebooks.polars_etl import run as polars_etl
from notebooks.duckdb_etl import run as duckdb_etl
from notebooks.modin_etl import run as modin_etl
from notebooks.spark_etl import run as spark_etl

etl_report = {}

def etl_pandas():
    start = time.time()

    pandas_etl()

    end = time.time()
    etl_report["pandas"] = (start, end)

def etl_polars():
    start = time.time()

    polars_etl()
    
    end = time.time()
    etl_report["polars"] = (start, end)

def etl_spark():
    start = time.time()
    
    spark_etl()
    
    end = time.time()
    etl_report["spark"] = (start, end)

def etl_duckdb():
    start = time.time()
    
    duckdb_etl()
    
    end = time.time()
    etl_report["duckdb"] = (start, end)

def etl_modin():
    start = time.time()
    
    modin_etl()
    
    end = time.time()
    etl_report["modin"] = (start, end)

def main():
    print("Starting ETL benchmark...\n")
    etl_modin()
    print("Modin ETL completed.")
    etl_pandas()
    print("Pandas ETL completed.")
    etl_polars()
    print("Polars ETL completed.")
    etl_duckdb()
    print("DuckDB ETL completed.")
    etl_spark()
    print("Spark ETL completed.")
    
    print("=== ETL Benchmark Report ===")
    for tool, (start, end) in etl_report.items():
        print(f"{tool.capitalize():<7}: Start: {time.strftime('%H:%M:%S', time.localtime(start))}, End: {time.strftime('%H:%M:%S', time.localtime(end))}, Duration: {end-start:.2f} sec")

if __name__ == "__main__":
    main()