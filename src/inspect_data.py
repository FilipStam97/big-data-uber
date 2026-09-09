import pandas as pd

path = "data/raw/fhvhv_tripdata_2021-01.parquet"

df = pd.read_parquet(path)

print("Shape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)

print("\nFirst 5 rows:")
print(df.head())

print("\nMissing values:")
print(df.isna().sum())