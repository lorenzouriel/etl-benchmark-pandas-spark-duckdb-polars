import pandas as pd
import numpy as np

N = 50_000_000
df = pd.DataFrame({
    "order_id": np.arange(1, N+1),
    "customer_id": np.random.randint(1, 100_000, size=N),
    "product": np.random.choice(["Laptop", "Shirt", "Coffee", "Keyboard", "Phone"], size=N),
    "category": np.random.choice(["Electronics", "Clothing", "Groceries"], size=N),
    "quantity": np.random.randint(1, 5, size=N),
    "price": np.random.randint(5, 1000, size=N),
    "order_date": pd.to_datetime("2022-01-01") + pd.to_timedelta(np.random.randint(0, 365, size=N), unit="D")
})
df.to_csv("data/orders_large.csv", index=False)