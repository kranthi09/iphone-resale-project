from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    return SparkSession.builder \
        .appName("iPhone Resale Pipeline") \
        .master("local[*]") \
        .getOrCreate()

def extract_csv(spark, filepath):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("nullValue", "") \
        .load(filepath)
    return df

if __name__ == "__main__":
    spark = create_spark_session()
    df = extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")
    print(f"Total rows: {df.count()}")
    print(f"Columns: {df.columns}")
    df.printSchema()