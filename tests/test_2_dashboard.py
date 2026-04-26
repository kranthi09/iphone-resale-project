from playwright.sync_api import sync_playwright
import time
import os

BASE_URL = "http://127.0.0.1:5000"


# ── Helper ────────────────────────────────────────────────────────
def get_page(playwright, url=BASE_URL):
    headless = os.getenv("CI", "false").lower() == "true"
    browser  = playwright.chromium.launch(headless=headless)
    page     = browser.new_page()
    page.goto(url)
    page.wait_for_load_state("networkidle")
    return browser, page


# ═══════════════════════════════════════════════════
# 1. VISUAL PRESENCE
# ═══════════════════════════════════════════════════

def test_dashboard_loads():
    with sync_playwright() as p:
        browser, page = get_page(p)
        assert "iPhone Resale Dashboard" in page.title()
        print("✅ Dashboard loaded")
        browser.close()


def test_all_kpi_cards_visible():
    with sync_playwright() as p:
        browser, page = get_page(p)
        cards = page.locator(".kpi-card")
        assert cards.count() == 3
        for i in range(3):
            assert cards.nth(i).is_visible()
        print("✅ All 3 KPI cards visible")
        browser.close()


def test_bar_chart_renders():
    with sync_playwright() as p:
        browser, page = get_page(p)
        assert page.locator("#barChart").is_visible()
        print("✅ Bar chart rendered")
        browser.close()


def test_donut_chart_renders():
    with sync_playwright() as p:
        browser, page = get_page(p)
        assert page.locator("#donutChart").is_visible()
        print("✅ Donut chart rendered")
        browser.close()


def test_map_renders():
    with sync_playwright() as p:
        browser, page = get_page(p)
        assert page.locator("#map").is_visible()
        print("✅ Map rendered")
        browser.close()


def test_filter_dropdowns_visible():
    with sync_playwright() as p:
        browser, page = get_page(p)
        assert page.locator('[data-testid="filter-model"]').is_visible()
        assert page.locator('[data-testid="filter-state"]').is_visible()
        print("✅ Filter dropdowns visible")
        browser.close()


# ═══════════════════════════════════════════════════
# 2. KPI VALIDATION — expected values from PySpark
# ═══════════════════════════════════════════════════

def test_kpi_total_listings(spark_kpis):
    expected = f"{spark_kpis['total']/1000:.3f}K"
    with sync_playwright() as p:
        browser, page = get_page(p)
        displayed = page.locator('[data-testid="kpi-total"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


def test_kpi_avg_price(spark_kpis):
    expected = f"{spark_kpis['avg_price']:,.2f}"
    with sync_playwright() as p:
        browser, page = get_page(p)
        displayed = page.locator('[data-testid="kpi-avg-price"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


def test_kpi_sum_sold(spark_kpis):
    expected = f"{spark_kpis['sum_sold']//1000}K"
    with sync_playwright() as p:
        browser, page = get_page(p)
        displayed = page.locator('[data-testid="kpi-sum-sold"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


# ═══════════════════════════════════════════════════
# 3. CROSS-VISUAL CONSISTENCY
# ═══════════════════════════════════════════════════

def test_kpi_total_matches_spark(spark_kpis):
    assert spark_kpis["total"] == 2371
    print(f"✅ PySpark count {spark_kpis['total']} consistent with source data")


# ═══════════════════════════════════════════════════
# 4. FILTER TESTS — expected values from PySpark
# ═══════════════════════════════════════════════════

def test_filter_by_model(df_clean):
    from pyspark.sql import functions as F
    model          = "iPhone 12 Pro"
    expected_count = df_clean.filter(F.col("model_family") == model).count()
    expected       = f"{expected_count/1000:.3f}K"

    with sync_playwright() as p:
        browser, page = get_page(p)
        page.select_option('[data-testid="filter-model"]', label=model)
        page.wait_for_load_state("networkidle")
        displayed = page.locator('[data-testid="kpi-total"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


def test_filter_by_state(df_clean):
    from pyspark.sql import functions as F
    state          = "TX"
    expected_count = df_clean.filter(F.col("us_state") == state).count()
    expected       = f"{expected_count/1000:.3f}K"

    with sync_playwright() as p:
        browser, page = get_page(p)
        page.select_option('[data-testid="filter-state"]', label=state)
        page.wait_for_load_state("networkidle")
        displayed = page.locator('[data-testid="kpi-total"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


def test_filter_reset(spark_kpis):
    expected = f"{spark_kpis['total']/1000:.3f}K"

    with sync_playwright() as p:
        browser, page = get_page(p)
        page.select_option('[data-testid="filter-model"]', label="iPhone 14 Pro Max")
        page.wait_for_load_state("networkidle")
        page.select_option('[data-testid="filter-model"]', label="All Models")
        page.wait_for_load_state("networkidle")
        displayed = page.locator('[data-testid="kpi-total"]').inner_text().strip()
        print(f"PySpark: {expected} | Dashboard: {displayed}")
        assert displayed == expected
        browser.close()


# ═══════════════════════════════════════════════════
# 5. PERFORMANCE
# ═══════════════════════════════════════════════════

def test_page_load_performance():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()
        start   = time.time()
        page.goto(BASE_URL)
        page.wait_for_load_state("networkidle")
        duration = time.time() - start
        print(f"⏱ Page loaded in {duration:.2f}s")
        assert duration < 5
        browser.close()