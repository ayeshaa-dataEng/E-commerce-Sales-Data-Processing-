# E-commerce Sales Data Pipeline

A Databricks and PySpark batch pipeline that combines customer, product, and order data into an enriched order master and profit reporting tables. The project includes data quality checks, unit tests, Databricks notebooks, and a GitHub Actions test workflow.

## Technology stack

- Python and PySpark
- Databricks and Delta Lake
- Unity Catalog volumes and tables
- pandas and openpyxl for the small Excel reference dataset
- pytest and GitHub Actions
- Spark SQL for the four reporting queries

## Data flow

1. Read customer Excel, product CSV, and order JSON files.
2. Write raw source tables with normalized column names.
3. Clean customer and product reference data and resolve duplicate dimension keys.
4. Validate order lines and quarantine invalid records.
5. Join orders with customer and product details using left joins.
6. Calculate year-based and customer/product profit aggregates.
7. Write results as Delta tables and expose SQL reporting queries.

## Repository structure

```text
.github/workflows/tests.yml       Automated test workflow
src/ecommerce/pipeline.py         Reusable PySpark pipeline
notebooks/01_pipeline.py          Databricks source notebook
notebooks/01_pipeline.ipynb       Jupyter notebook version
notebooks/02_sql_reports.sql      Databricks SQL reporting notebook
sql/profit_reports.sql           Reporting queries
tests/test_pipeline.py           Unit and source-data integration tests
data/                           Input data directory
pyproject.toml                  Package configuration and dependencies
data_profile.json               Supplied dataset profile
VALIDATION.md                   Local validation results and limitations
```

## Input datasets

| File | Format | Contents |
|---|---|---|
| `Customer.xlsx` | Excel | Customer identity, contact and geographic attributes |
| `Products.csv` | CSV | Product identity, category, subcategory and reference price |
| `Orders.json` | JSON array | Order lines, dates, quantities, prices, discounts and profit |

Place the files in `data/` for the local tests, or in a Unity Catalog volume for Databricks execution. The tests expect the supplied assessment dataset.

Keep customer contact details and any restricted source files out of a public repository unless their publication is authorized. Use anonymized sample data for public demonstrations.

## Output tables

| Table | Purpose |
|---|---|
| `raw_customers` | Customer source data |
| `raw_products` | Product source data |
| `raw_orders` | Order source data |
| `enriched_customers` | Customer records with name/contact quality flags |
| `enriched_products` | One deterministic canonical record per product ID |
| `product_duplicate_audit` | All source records belonging to repeated product IDs |
| `rejected_orders` | Invalid or duplicate order lines with rejection reasons |
| `master_orders` | Order information, rounded profit, year, customer name/country and product category/subcategory |
| `profit_aggregate` | Profit by year, category, subcategory and customer |
| `profit_by_year` | Annual profit |
| `profit_by_year_category` | Annual profit by product category |
| `profit_by_customer` | Customer profit |
| `profit_by_customer_year` | Customer profit by year |

## Local setup and tests

The local tests were run with Python 3.12, Java 17 and PySpark 3.5.3. Use Python 3.10–3.12 and Java 17 with `JAVA_HOME` configured.

```bash
python -m venv .venv
```

Activate the environment:

```bash
# macOS / Linux
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1
```

Install the package and run tests from the repository root:

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
```

The suite covers source row counts, join cardinality, profit reconciliation, unmatched products, negative profit, decimal rounding, invalid dates, duplicate order IDs at the row grain, customer quality flags, and SQL/DataFrame reconciliation for the annual report.

**Latest local result: 8 tests passed.** Databricks workspace execution and Delta table writes have not been verified by the local test run.

## Run in Databricks

1. Add the repository to your Databricks workspace as a Git folder.
2. Select a runtime compatible with Spark 3.5 and the APIs used in the pipeline.
3. Create or select a Unity Catalog schema and input volume that you can access.
4. Upload the three source files to the volume, for example `/Volumes/main/default/ecommerce_input`.
5. Open `notebooks/01_pipeline.py` and run the package installation cell. If the relative path does not resolve, install from the actual workspace repository root:

   ```python
   %pip install -e /Workspace/Repos/<user>/<repository>
   ```

6. Set the notebook widgets:

   | Widget | Example | Meaning |
   |---|---|---|
   | `input_path` | `/Volumes/main/default/ecommerce_input` | Directory containing the input files |
   | `output_schema` | `main.ecommerce` | Existing destination catalog/schema |

7. Run the remaining cells to build the Delta tables.
8. Run `notebooks/02_sql_reports.sql` or `sql/profit_reports.sql`. Replace `main.default` in the queries with your output catalog/schema.

The Excel reader uses Python file access on the driver, so the volume path must be accessible to pandas. This pipeline uses full refresh: each run overwrites its named destination tables. Use a dedicated schema.

## Profit reports

The PySpark pipeline produces all four reports. Equivalent SQL queries are provided for:

- Profit by year
- Profit by year and product category
- Profit by customer
- Profit by customer and year

The detailed aggregate also includes product subcategory. Customer grouping includes both customer ID and name to avoid merging different customers who share a name.

## Data quality and business rules

- **Order grain:** `Row ID` identifies an order line. `Order ID` may appear on several lines.
- **Profit:** Use the supplied profit value, round each line to two decimal places with Spark's half-up rounding, then aggregate using decimal arithmetic. Negative profit is valid.
- **Dates:** Parse day/month/year dates. Missing or invalid dates and shipping dates earlier than order dates cause rejection.
- **Validation:** Reject missing required keys, invalid numeric values, non-positive quantities and discounts outside the range 0–1.
- **Duplicate orders:** Quarantine every occurrence of a repeated Row ID.
- **Duplicate products:** Select one canonical record per product ID using deterministic lexicographic ordering. Retain duplicate source records in the audit table. Reference price/state belong to the selected record; they are not a historical pricing model.
- **Missing references:** Preserve valid order lines with left joins. Mark unmatched references with `customer_found` or `product_found` and use `Unknown` for missing descriptive attributes.
- **Customer names:** Normalize whitespace and flag suspicious characters without guessing corrections. The `customer_name_original` column retains the whitespace-normalized source name; the raw table retains the original source value.
- **Contact checks:** Email and phone flags are basic syntax checks, not verification of ownership or deliverability.

### Supplied dataset profile

| Metric | Count |
|---|---:|
| Order lines | 9,994 |
| Customers | 793 |
| Product source records | 1,851 |
| Unique product IDs | 1,818 |
| Order lines with missing product references | 204 |
| Distinct missing product IDs | 44 |

All supplied order lines passed the implemented validation rules. Missing product references are retained and flagged.

## Scalability and operational scope

The pipeline uses Spark DataFrame operations for fact processing and aggregation. Only the small customer Excel reference file is read through pandas on the driver. Explicit dimension deduplication prevents joins from multiplying order lines.

The supplied JSON is a multiline array. For larger deployments, use partitionable formats such as newline-delimited JSON or Parquet. Production extensions could include Auto Loader, incremental Delta MERGE, orchestration, input versioning, and quality thresholds.

Each Delta table write is atomic individually; the complete set of output writes is not one transaction. Downstream consumers should use results only after the job succeeds. Incremental ingestion, CDC, scheduling and historical dimension tracking are outside the current implementation.

## Continuous integration

`.github/workflows/tests.yml` installs Python 3.11, Java 17 and the development dependencies, then runs pytest on pushes and pull requests.
