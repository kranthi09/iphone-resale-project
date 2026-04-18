from extract import create_spark_session, extract_csv
from transform import transform
from load import load

if __name__ == "__main__":
    print("=== iPhone Resale ETL Pipeline ===")
    
    print("\n[1/3] Extracting...")
    spark = create_spark_session()
    df_raw = extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")
    print(f"    Extracted {df_raw.count()} rows")

    print("\n[2/3] Transforming...")
    df_clean = transform(df_raw)
    print(f"    Columns after transform: {len(df_clean.columns)}")

    print("\n[3/3] Loading...")
    load(df_clean, "data/cleaned_iphone_listings")

    print("\n=== Pipeline complete ===")