import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from extract import create_spark_session, extract_csv
from transform import transform

@pytest.fixture(scope="session")
def spark():
    return create_spark_session()

@pytest.fixture(scope="session")
def df_raw(spark):
    return extract_csv(spark, "data/ecommerce_iphone_resale_market_intelligence_usa_2026.csv")

@pytest.fixture(scope="session")
def df_clean(spark, df_raw):
    return transform(df_raw)

# --- EXTRACT TESTS ---
def test_row_count(df_raw):
    assert df_raw.count() == 2371

def test_column_count(df_raw):
    assert len(df_raw.columns) == 16

def test_no_null_prices(df_raw):
    null_prices = df_raw.filter(df_raw.price.isNull()).count()
    assert null_prices == 0

# --- TRANSFORM TESTS ---
def test_transform_adds_columns(df_clean):
    assert "condition_group" in df_clean.columns
    assert "price_per_gb" in df_clean.columns
    assert "is_discounted" in df_clean.columns

def test_model_typo_fixed(df_clean):
    typo_count = df_clean.filter(
        df_clean.model_family == "iPhone 12Pro Max"
    ).count()
    assert typo_count == 0

def test_no_null_condition_group(df_clean):
    nulls = df_clean.filter(df_clean.condition_group.isNull()).count()
    assert nulls == 0

def test_price_range(df_clean):
    min_price = df_clean.agg({"price": "min"}).collect()[0][0]
    max_price = df_clean.agg({"price": "max"}).collect()[0][0]
    assert min_price >= 50
    assert max_price <= 6000

def test_condition_groups_valid(df_clean):
    valid_groups = {"New / Open Box", "Refurbished", "Used", "For Parts"}
    actual_groups = set(
        row[0] for row in df_clean.select("condition_group").distinct().collect()
    )
    assert actual_groups == valid_groups

# --- LOAD TESTS ---
def test_output_file_exists():
    assert os.path.exists("data/cleaned_iphone_listings/cleaned_iphone_listings.csv")

def test_output_row_count():
    df = pd.read_csv("data/cleaned_iphone_listings/cleaned_iphone_listings.csv")
    assert len(df) == 2371

def test_output_column_count():
    df = pd.read_csv("data/cleaned_iphone_listings/cleaned_iphone_listings.csv")
    assert len(df.columns) == 19