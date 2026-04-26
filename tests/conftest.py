import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from extract import create_spark_session, extract_csv
from transform import transform

@pytest.fixture(scope="session")
def spark():
    session = create_spark_session()
    yield session
    session.stop()

@pytest.fixture(scope="session")
def df_raw(spark):
    return extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")

@pytest.fixture(scope="session")
def df_clean(spark, df_raw):
    return transform(df_raw)

@pytest.fixture(scope="session")
def spark_kpis(df_clean):
    """Single source of truth — all expected values computed by PySpark."""
    from pyspark.sql import functions as F

    total     = df_clean.count()
    avg_price = round(df_clean.agg(F.avg("price")).collect()[0][0], 2)
    sum_sold  = int(df_clean.agg(F.sum("sold")).collect()[0][0])
    top_state = (df_clean.groupBy("us_state")
                         .count()
                         .orderBy(F.desc("count"))
                         .first()["us_state"])

    return {
        "total":     total,
        "avg_price": avg_price,
        "sum_sold":  sum_sold,
        "top_state": top_state,
    }