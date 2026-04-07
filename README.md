# Amazon Sales Intelligence Pipeline

**Azure · Databricks · Microsoft Fabric · Power BI**

[![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0078D4?logo=microsoft-azure)](https://azure.microsoft.com/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark-FF3621?logo=databricks)](https://databricks.com/)
[![Fabric](https://img.shields.io/badge/Microsoft-Fabric-742774?logo=microsoft)](https://fabric.microsoft.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python)](https://python.org/)

---

## What This Project Does

This is an end-to-end data pipeline that compares Amazon sales data from 2022 and 2023 — cleaning it, running statistical tests, training ML models, and pushing results to a Power BI dashboard. The whole thing is automated through Microsoft Fabric.

The core question it tries to answer: **did revenue behaviour actually change between 2022 and 2023, or is it just noise?**

Short answer from the data: it changed. Significantly.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│           MICROSOFT FABRIC PIPELINE              │
└─────────────────────┬────────────────────────────┘
                      │
        ┌─────────────▼─────────────┐
        │   AZURE DATA LAKE (ADLS)  │
        │  bronze/ silver/ gold/    │
        └─────────────┬─────────────┘
                      │
   ┌──────────────────┼──────────────────┐
   ▼                  ▼                  ▼
BRONZE  ──────▶    SILVER  ──────▶    GOLD
(ingest)          (clean)         ┌──────────┐
                                  │Gold_Stats│
                                  │Gold_ML   │
                                  └────┬─────┘
                                       │
                                  Power BI Dashboard
```

**Fabric pipeline flow:**
```
[Bronze] ──✅──▶ [Silver] ──✅──▶ [Gold_Stats]
                          └──✅──▶ [Gold_ML]
                          └──❌──▶ [Email Alert]   ← fires on Bronze failure
```

---

## Medallion Architecture

| Layer | Tables | What happens here |
|-------|--------|-------------------|
| 🥉 Bronze | `bronze.current_raw`, `bronze.previous_raw` | Raw files land here, no changes |
| 🥈 Silver | `silver.current_clean`, `silver.previous_clean` | Cleaned, typed, deduplicated |
| 🥇 Gold | `gold.statistical_audit`, `gold.ml_model_comparison` | Stats, drift detection, ML results |
| 📋 Metadata | `metadeta.pipeline_logs` | Every step logged with timestamp + status |

---

## Tech Stack

| | Tool | Used for |
|-|------|----------|
| Storage | Azure Data Lake Storage Gen2 | Holding raw and processed files |
| Compute | Azure Databricks + Unity Catalog | Running PySpark notebooks |
| Orchestration | Microsoft Fabric Pipelines | Scheduling and chaining notebooks |
| Processing | PySpark, Spark MLlib | Transforms and ML model training |
| Stats | SciPy, NumPy, Seaborn | T-test, ANOVA, distribution plots |
| Secrets | Azure Key Vault | No hardcoded credentials anywhere |
| Dashboard | Power BI (Fabric workspace) | Visualising results |

---

## Notebooks

### `utils.py` — shared config and helpers

Everything shared across notebooks lives here and gets loaded with `%run ./utils`. This includes the ADLS paths, the logging function, and a couple of utility functions:

- `get_latest_files()` — scans the bronze folder and returns the two most recent files. This is how the pipeline avoids hardcoded filenames — just drop a new file in and it gets picked up automatically.
- `read_dynamic(path)` — reads CSV, JSON, or Parquet depending on the file extension.
- `write_log(step, status, message)` — appends a row to the pipeline logs table with a UUID and timestamp.

---

### `Bronze_ingestion.py` — raw ingestion

Grabs the two latest files from ADLS, adds an ingestion timestamp, runs a quick schema check (warns if column overlap drops below 80%), then saves to the bronze tables.

```python
path_new, path_old = get_latest_files()
df_new = read_dynamic(path_new)

# schema similarity check
similarity = len(c1 & c2) / len(c1 | c2)

df_new.write.mode("overwrite").saveAsTable("amazon_project.bronze.current_raw")
df_old.write.mode("overwrite").saveAsTable("amazon_project.bronze.previous_raw")
```

---

### `Silver_cleaning.py` — cleaning and validation

Reads from bronze, applies the same cleaning logic to both datasets, and saves to silver. If a dataset comes out empty after cleaning, it raises an exception before anything gets written.

```python
def clean_and_cast(df):
    df = df.dropDuplicates()
    df = df.withColumn("price", col("price").cast("double")) \
           .withColumn("rating", col("rating").cast("double")) \
           .withColumn("total_revenue", col("total_revenue").cast("double"))
           # all 13 columns explicitly cast
    df = df.dropna(subset=["price", "quantity_sold", "order_id"])
    return df
```

---

### `Gold_Stats.py` — statistics and drift detection

This is the main analysis notebook. It reads both silver tables and runs:

- **Descriptive stats** — mean, std dev, skewness, kurtosis on `total_revenue`
- **Precision** — `1/σ` as a consistency measure (lower = more volatile)
- **Welch's t-test** — tests whether the revenue difference between years is real or random
- **ANOVA** — checks if revenue varies significantly across product categories (within each year)
- **Correlation drift** — Pearson's r comparison to see if feature relationships shifted
- **Visual EDA** — histograms, box plots, scatter plots, heatmaps

The drift flag is simple: if `p_value < 0.05`, mark `is_drifted = True`.

---

### `Gold_ML.py` — model training and comparison

Trains three regression models on both datasets to predict `total_revenue` from price, quantity, discount, rating, and review count. Uses an 80/20 train-test split and evaluates with R².

Models: Linear Regression, Decision Tree, Random Forest (all from Spark MLlib).

---

## Results

### Statistical

| Metric | 2022 | 2023 |
|--------|------|------|
| Avg Revenue | $657.52 | $765.80 |
| Std Deviation | $527.85 | $660.54 |
| Skewness | 0.98 | 1.42 |
| Kurtosis | 0.27 | 3.06 |
| Max Revenue | $2,499 | $7,362 |
| P-Value | — | 9.77 × 10⁻⁹¹ |
| Drifted | — | **True** |

Revenue went up ~16% but also got a lot more unpredictable. The kurtosis jumping from 0.27 to 3.06 is mostly driven by a few very large orders that didn't exist in the 2022 data at all.

### ML Models

| Model | 2022 R² | 2023 R² |
|-------|---------|---------|
| Linear Regression | 0.877 | 0.832 |
| Decision Tree | 0.963 | 0.908 |
| Random Forest | **0.963** | **0.909** |

Random Forest comes out on top both years. The drop in 2023 scores is a direct result of the distribution shift — the models were never trained on orders above $2,500, so the new $7,000+ orders create large errors.

---

## Power BI Dashboard

Connected to the Gold layer via SQL Warehouse. Shows:
- R² score comparison across all three models (Current vs. Previous)
- Revenue stats table (avg, std dev, skewness, kurtosis, p_value, is_drifted)
- Correlation change charts by feature pair

Refreshes automatically after each pipeline run.

---

## How to Run

**One-time setup** — run `New_Notebook` to create the Unity Catalog schemas:

```sql
CREATE CATALOG IF NOT EXISTS amazon_project;
CREATE SCHEMA IF NOT EXISTS amazon_project.bronze
  MANAGED LOCATION 'abfss://main@amazonstore.dfs.core.windows.net/bronze';
-- repeat for silver, gold, metadeta
```

**Upload data** to ADLS:
```
main/bronze/
  ├── amazon_2023_modified.csv   ← current
  └── amazon_2022.csv            ← previous
```

**Run manually (Databricks):**
```
utils → Bronze_ingestion → Silver_cleaning → Gold_Stats + Gold_ML
```

**Run via Fabric:**
```
Open Amazon_pipeline → Click Run
```

**Check results:**
```sql
SELECT * FROM amazon_project.metadeta.pipeline_logs ORDER BY timestamp DESC;
SELECT * FROM amazon_project.gold.ml_model_comparison ORDER BY r2_score DESC;
```

---

## Project Structure

```
amazon-sales-pipeline/
├── notebooks/
│   ├── utils.py
│   ├── Bronze_ingestion.py
│   ├── Silver_cleaning.py
│   ├── Gold_Stats.py
│   └── Gold_ML.py
├── setup/
│   └── New_Notebook.py
└── data/
    ├── amazon_2022.csv          # 24,926 rows
    └── amazon_2023_modified.csv # 25,074 rows
```

---

## Dataset

13 columns: `order_id`, `order_date`, `product_id`, `product_category`, `price`, `discount_percent`, `quantity_sold`, `customer_region`, `payment_method`, `rating`, `review_count`, `discounted_price`, `total_revenue`
