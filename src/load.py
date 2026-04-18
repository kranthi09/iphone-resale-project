import os
from extract import create_spark_session, extract_csv
from transform import transform

def load(df, output_path):
    # Convert to pandas and write — bypasses winutils issue on Windows
    df_pandas = df.toPandas()
    os.makedirs(output_path, exist_ok=True)
    out_file = os.path.join(output_path, "cleaned_iphone_listings.csv")
    df_pandas.to_csv(out_file, index=False)
    print(f"Data written to: {out_file}")
    print(f"Total rows written: {len(df_pandas)}")

if __name__ == "__main__":
    spark = create_spark_session()
    df_raw = extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")
    df_clean = transform(df_raw)
    load(df_clean, "data/cleaned_iphone_listings")

    # Verify
    import pandas as pd
    df_verify = pd.read_csv("data/cleaned_iphone_listings/cleaned_iphone_listings.csv")
    print(f"\nVerification row count: {len(df_verify)}")
    print(df_verify.head(3))