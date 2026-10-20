# Databricks notebook source
# MAGIC %pip install -e ..
# COMMAND ----------
from ecommerce.pipeline import read_sources, transform, write_tables

dbutils.widgets.text("input_path", "/Volumes/main/default/ecommerce_input")
dbutils.widgets.text("output_schema", "main.default")
root = dbutils.widgets.get("input_path").rstrip("/")
schema = dbutils.widgets.get("output_schema")
raw = read_sources(spark, root)
results = transform(raw)
# Full refresh, idempotent for unchanged input. Writes replace named tables.
write_tables(raw, results, schema)
for name in ["master_orders", "profit_aggregate", "rejected_orders"]:
    display(results[name])
