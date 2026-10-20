from pathlib import Path
from decimal import Decimal
import pytest
from pyspark.sql import SparkSession, functions as F
from ecommerce.pipeline import read_sources, transform, aggregate

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[2]").appName("ecommerce-tests").config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()

@pytest.fixture(scope="module")
def output(spark):
    raw = read_sources(spark, str(Path(__file__).resolve().parents[1] / "data"))
    out = transform(raw)
    out["master_orders"].cache()
    return raw, out

def test_supplied_data_and_join_cardinality(output):
    raw, out = output
    assert raw["orders"].count() == 9994
    assert out["master_orders"].count() + out["rejected_orders"].count() == 9994
    assert out["master_orders"].select("row_id").distinct().count() == out["master_orders"].count()
    assert out["enriched_products"].count() == 1818

def test_profit_reconciles(output):
    raw, out = output
    assert out["rejected_orders"].count() == 0
    expected = raw["orders"].select(F.round(F.col("profit").cast("decimal(18,6)"), 2).alias("p")).agg(F.sum("p")).first()[0]
    for table in ["profit_aggregate", "profit_by_year", "profit_by_year_category", "profit_by_customer", "profit_by_customer_year"]:
        assert out[table].agg(F.sum("total_profit")).first()[0] == expected

def test_unknown_products_preserved(output):
    _, out = output
    assert out["master_orders"].filter(~F.col("product_found")).count() > 0
    assert out["master_orders"].filter(~F.col("product_found") & (F.col("category") != "Unknown")).count() == 0

def test_rounding_negative_profit_and_year(spark):
    df = spark.createDataFrame([(2017, Decimal("1.24")), (2017, Decimal("-2.01")), (2018, Decimal("3.00"))], "year int, profit decimal(18,2)")
    assert {r.year: r.total_profit for r in aggregate(df,["year"]).collect()} == {2017: Decimal("-0.77"), 2018: Decimal("3.00")}

def test_invalid_and_duplicate_orders_quarantined(output):
    raw, _ = output
    base = raw["orders"].limit(1)
    invalid = base.withColumn("row_id", F.lit(99999)).withColumn("quantity", F.lit(-1))
    mixed = {**raw, "orders": base.unionByName(base).unionByName(invalid)}
    out = transform(mixed)
    assert out["master_orders"].count() == 0
    assert out["rejected_orders"].count() == 3

def test_name_and_contact_quality_flags(output):
    _, out = output
    c = out["enriched_customers"]
    assert c.filter("name_needs_review").count() > 0
    assert c.filter("NOT phone_valid").count() > 0
    assert c.filter(F.col("customer_name") != F.col("customer_name_original")).count() == 0

def test_decimal_rounding_and_bad_dates(output):
    raw, _ = output
    base = raw["orders"].limit(1)
    rounded = base.withColumn("profit", F.lit(-1.235))
    assert transform({**raw, "orders": rounded})["master_orders"].first().profit == Decimal("-1.24")
    bad = base.withColumn("order_date", F.lit("31/02/2017"))
    assert transform({**raw, "orders": bad})["rejected_orders"].count() == 1

def test_sql_report_matches_dataframe(output, spark):
    _, out = output
    out["profit_aggregate"].createOrReplaceTempView("test_profit_aggregate")
    expected = sorted((r.year, r.total_profit) for r in out["profit_by_year"].collect())
    actual = sorted((r.year, r.total_profit) for r in spark.sql("SELECT year, SUM(total_profit) AS total_profit FROM test_profit_aggregate GROUP BY year").collect())
    assert actual == expected
