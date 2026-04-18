from pyspark.sql import functions as F

def transform(df):

    # Fix model_family typo
    df = df.withColumn("model_family",
        F.when(F.col("model_family") == "iPhone 12Pro Max", "iPhone 12 Pro Max")
        .otherwise(F.col("model_family"))
    )

    # Fill nulls
    df = df.fillna({
        "storage_gb_numeric": 0,
        "wasPrice": 0,
        "price_discount_pct": 0,
        "available": 0,
        "sold": 0,
        "us_state": "Unknown"
    })

    # Add condition group column
    df = df.withColumn("condition_group",
        F.when(F.col("condition").isin("New", "Open Box"), "New / Open Box")
        .when(F.col("condition").isin(
            "Excellent - Refurbished",
            "Very Good - Refurbished",
            "Good - Refurbished"), "Refurbished")
        .when(F.col("condition") == "Used", "Used")
        .otherwise("For Parts")
    )

    # Add price_per_gb derived column
    df = df.withColumn("price_per_gb",
        F.when(F.col("storage_gb_numeric") > 0,
            F.round(F.col("price") / F.col("storage_gb_numeric"), 4))
        .otherwise(None)
    )

    # Add is_discounted flag
    df = df.withColumn("is_discounted",
        F.when(F.col("wasPrice") > 0, True).otherwise(False)
    )

    return df

if __name__ == "__main__":
    from extract import create_spark_session, extract_csv

    spark = create_spark_session()
    df = extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")
    df_clean = transform(df)

    print("Schema after transform:")
    df_clean.printSchema()

    print("\nCondition groups:")
    df_clean.groupBy("condition_group").count().show()

    print("\nSample rows:")
    df_clean.select("model_family", "price", "condition_group", "price_per_gb", "is_discounted").show(5)