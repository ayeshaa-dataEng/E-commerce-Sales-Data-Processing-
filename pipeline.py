"""DataFrame-only transformations; SQL is confined to the requested reports."""
import re
from pathlib import Path
import pandas as pd
from pyspark.sql import functions as F, Window


def normalize(df):
    return df.toDF(*[re.sub(r"[^a-z0-9]+", "_", c.lower()).strip("_") for c in df.columns])


def read_sources(spark, root):
    # Excel is a small reference file, parsed on the driver; orders/products use Spark.
    customers = pd.read_excel(str(Path(root) / "Customer.xlsx"), dtype=str).fillna("")
    customers = spark.createDataFrame(customers)
    products = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(root + "/Products.csv")
    orders = spark.read.option("multiLine", True).json(root + "/Orders.json")
    return {"customers": normalize(customers), "products": normalize(products), "orders": normalize(orders)}


def clean_strings(df):
    for name, typ in df.dtypes:
        if typ == "string":
            value = F.trim(F.regexp_replace(F.col(name), r"\s+", " "))
            df = df.withColumn(name, F.when(value != "", value))
    return df


def unique_dimension(df, key):
    """Deterministic canonical row; all conflicting/duplicate source rows remain in audit."""
    columns = sorted(df.columns)
    window = Window.partitionBy(key).orderBy(*[F.col(c).asc_nulls_last() for c in columns])
    return df.filter(F.col(key).isNotNull()).withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def aggregate(master, keys):
    return master.groupBy(*keys).agg(F.sum("profit").cast("decimal(24,2)").alias("total_profit"), F.count("*").alias("order_line_count"))


def transform(raw):
    customers = clean_strings(raw["customers"])
    # Preserve original names rather than guessing corrections to corrupt identities.
    customers = customers.withColumn("customer_name_original", F.col("customer_name"))
    customers = customers.withColumn("name_needs_review", F.col("customer_name").rlike(r"[^\p{L}\s.'-]"))
    customers = customers.withColumn("email_valid", F.coalesce(F.col("email").rlike(r"^[^\s@]+@[^\s@]+\.[^\s@]+$"), F.lit(False)))
    customers = customers.withColumn("phone_valid", F.coalesce(F.col("phone").rlike(r"^[+()0-9 .x-]{7,}$") & ~F.col("phone").startswith("-"), F.lit(False)))
    customers = unique_dimension(customers, "customer_id")
    products_all = clean_strings(raw["products"])
    duplicates = products_all.join(products_all.groupBy("product_id").count().filter("count > 1").select("product_id"), "product_id")
    products = unique_dimension(products_all, "product_id").withColumn("price_per_product", F.expr("try_cast(price_per_product as decimal(18,4))"))
    orders = clean_strings(raw["orders"])
    for c in ["profit", "price", "discount"]:
        orders = orders.withColumn(c, F.expr(f"try_cast({c} as decimal(18,6))"))
    orders = orders.withColumn("quantity", F.expr("try_cast(quantity as int)"))
    orders = orders.withColumn("order_date", F.try_to_timestamp("order_date", F.lit("d/M/yyyy")).cast("date")).withColumn("ship_date", F.try_to_timestamp("ship_date", F.lit("d/M/yyyy")).cast("date"))
    orders = orders.withColumn("profit", F.round("profit", 2).cast("decimal(18,2)"))
    valid = F.col("row_id").isNotNull() & F.col("order_id").isNotNull() & F.col("customer_id").isNotNull() & F.col("product_id").isNotNull() & F.col("order_date").isNotNull() & F.col("profit").isNotNull() & (F.col("quantity") > 0) & F.col("discount").between(0, 1) & (F.col("price") >= 0) & F.col("ship_date").isNotNull() & (F.col("ship_date") >= F.col("order_date"))
    orders = orders.withColumn("_valid", F.coalesce(valid, F.lit(False)))
    # Duplicate row IDs are ambiguous: quarantine every occurrence, never silently drop.
    repeated = orders.groupBy("row_id").count().filter("count > 1").select("row_id").withColumn("_duplicate", F.lit(True))
    orders = orders.join(repeated, "row_id", "left")
    orders = orders.withColumn("rejection_reason", F.when(F.col("_duplicate"), "duplicate_row_id").when(~F.col("_valid"), "invalid_required_field_or_value"))
    rejected = orders.filter(F.col("rejection_reason").isNotNull())
    orders = orders.filter(F.col("rejection_reason").isNull()).drop("_valid", "_duplicate", "rejection_reason")
    c = customers.select("customer_id", "customer_name", "country").withColumn("customer_found", F.lit(True))
    p = products.select("product_id", "category", "sub_category").withColumn("product_found", F.lit(True))
    master = orders.join(c, "customer_id", "left").join(p, "product_id", "left").withColumn("year", F.year("order_date"))
    master = master.withColumn("customer_found", F.coalesce("customer_found", F.lit(False))).withColumn("product_found", F.coalesce("product_found", F.lit(False)))
    # Null categories retain unmatched facts in an explicit unknown bucket.
    for name in ["customer_name", "country", "category", "sub_category"]:
        master = master.withColumn(name, F.coalesce(F.col(name), F.lit("Unknown")))
    return {"enriched_customers": customers, "enriched_products": products,
            "product_duplicate_audit": duplicates, "rejected_orders": rejected,
            "master_orders": master,
            "profit_aggregate": aggregate(master, ["year", "category", "sub_category", "customer_id", "customer_name"]),
            "profit_by_year": aggregate(master, ["year"]),
            "profit_by_year_category": aggregate(master, ["year", "category"]),
            "profit_by_customer": aggregate(master, ["customer_id", "customer_name"]),
            "profit_by_customer_year": aggregate(master, ["customer_id", "customer_name", "year"])}


def write_tables(raw, results, schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", schema):
        raise ValueError("Use schema or catalog.schema with simple identifiers")
    # Schema must already exist; callers control permissions and destination.
    for name, df in {**{"raw_" + k: v for k, v in raw.items()}, **results}.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(schema + "." + name)
