# 🛒 Amazon Sales Intelligence Pipeline
### End-to-End Data Engineering & ML Platform | Azure · Databricks · Microsoft Fabric · Power BI

[![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Storage-0078D4?logo=microsoft-azure)](https://azure.microsoft.com/)
[![Databricks](https://img.shields.io/badge/Databricks-PySpark-FF3621?logo=databricks)](https://databricks.com/)
[![Fabric](https://img.shields.io/badge/Microsoft-Fabric-742774?logo=microsoft)](https://fabric.microsoft.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python)](https://python.org/)

---

## 📌 Project Summary

A production-grade data pipeline that ingests, cleans, and analyses two years of Amazon sales data (2022 vs. 2023) to answer one core business question:

> **"Has there been a statistically significant and meaningful shift in revenue and product behaviour between 2022 and 2023?"**

The pipeline applies the **Medallion Architecture** (Bronze → Silver → Gold), orchestrated end-to-end via **Microsoft Fabric**, with results surfaced in a **Power BI dashboard** showing statistical drift, ML model comparisons, and correlation changes.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MICROSOFT FABRIC PIPELINE                        │
│                    (Orchestration + Triggers)                       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
          ┌────────────────▼────────────────┐
          │     AZURE DATA LAKE STORAGE     │
          │   abfss://main@amazonstore/     │
          │  landing/ · bronze/ · silver/  │
          │           gold/ · logs/        │
          └────────────────┬────────────────┘
                           │
   ┌───────────────────────┼───────────────────────┐
   │                       │                       │
   ▼                       ▼                       ▼
┌──────────────┐   ┌───────────────┐   ┌──────────────────────────┐
│   🥉 BRONZE  │──▶│   🥈 SILVER   │──▶│        🥇 GOLD           │
│   Notebook   │   │   Notebook    │   │  ┌───────────────────┐   │
│              │   │               │   │  │   Gold_Stats      │   │
│ • Ingest raw │   │ • Dedup       │   │  │  (Stats + Drift)  │   │
│   CSV/JSON/  │   │ • Type cast   │   │  └───────────────────┘   │
│   Parquet    │   │ • Null drop   │   │  ┌───────────────────┐   │
│ • Schema     │   │ • Validate    │   │  │    Gold_ML        │   │
│   validation │   │               │   │  │  (ML + R² Score)  │   │
│ • Sliding    │   │ Unity Catalog │   │  └───────────────────┘   │
│   window     │   │ (current_clean│   │                          │
│   ingestion  │   │  prev_clean)  │   │  Unity Catalog           │
└──────────────┘   └───────────────┘   │  (gold layer tables)     │
                                       └──────────┬───────────────┘
                                                  │
                                     ┌────────────▼────────────┐
                                     │  📊 POWER BI DASHBOARD  │
                                     │  · R² model comparison  │
                                     │  · Revenue drift table  │
                                     │  · Correlation charts   │
                                     └─────────────────────────┘
```

---

## 🧱 Medallion Architecture

| Layer | Table Location | Purpose |
|-------|---------------|---------|
| 🥉 **Bronze** | `amazon_project.bronze.current_raw` / `previous_raw` | Raw ingestion — no transformation, exactly as received |
| 🥈 **Silver** | `amazon_project.silver.current_clean` / `previous_clean` | Cleaned, typed, validated, deduplicated data |
| 🥇 **Gold** | `amazon_project.gold.statistical_audit` / `ml_model_comparison` / `correlation_drift` | Business-ready aggregations, statistics, and ML results |
| 📋 **Metadata** | `amazon_project.metadeta.pipeline_logs` | Audit log of every pipeline step with timestamps |

---

## ⚙️ Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| **Cloud Storage** | Azure Data Lake Storage Gen2 (ADLS) | Raw file storage (`abfss://`) |
| **Compute** | Azure Databricks (Unity Catalog) | Distributed PySpark processing |
| **Orchestration** | Microsoft Fabric Pipelines | DAG-based pipeline scheduling & triggers |
| **Processing** | PySpark (Spark ML + SQL) | Data transformation & ML training |
| **Statistics** | SciPy, NumPy, Seaborn, Matplotlib | Hypothesis testing & visualisation |
| **ML** | Spark MLlib | LinearRegression, DecisionTree, RandomForest |
| **Secrets** | Azure Key Vault (`kv-scope`) | Secure credential management |
| **Visualisation** | Microsoft Power BI | Interactive dashboard |
| **Catalog** | Databricks Unity Catalog | Governed data access across layers |

---

## ✨ Key Features

- 🔄 **Incremental / Sliding Window Ingestion** — `get_latest_files()` auto-detects the two most recent files in the bronze folder, enabling automated daily or weekly refreshes without manual intervention
- 🛡️ **Schema Validation at Ingest** — 80% column-similarity check at Bronze layer flags schema drift before it propagates downstream
- 📋 **Full Audit Logging** — every notebook step writes a record to `metadeta.pipeline_logs` (step name, status, message, UUID, timestamp)
- 🔧 **Centralised Config** (`utils` notebook) — all ADLS paths, logging functions, and file readers in one place, imported via `%run ./utils`
- 🔌 **Multi-format Reader** — `read_dynamic()` transparently handles CSV, JSON, and Parquet without code changes
- 📊 **Statistical Drift Detection** — automated p-value threshold (`< 0.05`) flags revenue distribution changes as `is_drifted = True/False`
- 🤖 **Three-Model ML Comparison** — Linear Regression, Decision Tree, and Random Forest evaluated side-by-side on both years
- 📈 **Live Power BI Dashboard** — connected to Gold layer SQL Warehouse; auto-refreshes with each pipeline run

---

## 🔄 Pipeline Walkthrough

### 1 · Bronze Layer — Raw Ingestion

**Notebook:** `Bronze_ingestion.py` | **Triggered by:** Microsoft Fabric on file arrival

```python
# Auto-detect latest 2 files from ADLS bronze folder (sliding window)
path_new, path_old = get_latest_files()

# Read dynamically — supports CSV / JSON / Parquet
df_new = read_dynamic(path_new)
df_old = read_dynamic(path_old)

# Add ingestion timestamp for lineage tracking
df_new = df_new.withColumn("ingestion_time", current_timestamp())

# Schema similarity check — warns if < 80% columns match
similarity = len(c1 & c2) / len(c1 | c2)

# Write to Unity Catalog bronze tables
df_new.write.mode("overwrite").saveAsTable("amazon_project.bronze.current_raw")
df_old.write.mode("overwrite").saveAsTable("amazon_project.bronze.previous_raw")
```

**Key design decision:** The sliding window approach means the pipeline always compares the **two most recent datasets** — no hardcoded filenames. Drop a new file into ADLS, and the pipeline automatically picks it up as the new "current" dataset.

---

### 2 · Silver Layer — Cleaning & Standardisation

**Notebook:** `Silver_cleaning.py` | **Triggered by:** Bronze success

```python
def clean_and_cast(df):
    df = df.dropDuplicates()                              # Remove exact duplicates
    df = df.withColumn("order_id",   col("order_id").cast("int"))
           .withColumn("price",      col("price").cast("double"))
           .withColumn("rating",     col("rating").cast("double"))
           .withColumn("total_revenue", col("total_revenue").cast("double"))
           # ... all 13 columns explicitly typed

    df = df.dropna(subset=["price", "quantity_sold", "order_id"])  # Drop critical nulls
    return df

# Validation gate — reject empty DataFrames before saving
if curr_clean.count() == 0:
    raise Exception("Current dataset is empty after cleaning!")

curr_clean.write.saveAsTable("amazon_project.silver.current_clean")
prev_clean.write.saveAsTable("amazon_project.silver.previous_clean")
```

**Columns processed:** `order_id`, `order_date`, `product_id`, `product_category`, `price`, `discount_percent`, `quantity_sold`, `customer_region`, `payment_method`, `rating`, `review_count`, `discounted_price`, `total_revenue`

---

### 3a · Gold Layer — Statistical Analysis

**Notebook:** `Gold_Stats.py` | **Triggered by:** Silver success

Reads `amazon_project.silver.current_clean` and `previous_clean`, then computes:

#### Descriptive Statistics
```python
def get_stats(df, label):
    return df.select(
        lit(label).alias("dataset"),
        mean("total_revenue").alias("avg_revenue"),
        stddev("total_revenue").alias("std_dev"),
        skewness("total_revenue").alias("skewness"),
        kurtosis("total_revenue").alias("kurtosis")
    )
```

#### Precision Analysis
```python
precision = 1 / np.std(revenue_array)
# Higher precision → more consistent data (2022: 0.001895)
# Lower precision → more volatile data  (2023: 0.001514)
```

#### Hypothesis Testing (Welch's t-Test)
```python
from scipy.stats import ttest_ind
t_stat, p_value = ttest_ind(curr_revenue, prev_revenue)
is_drifted = p_value < 0.05   # Boolean drift flag
```

#### ANOVA — Cross-Category Test
```python
from scipy.stats import f_oneway
# Tests: are revenue differences across product categories significant?
# Result: p ≈ 0.72 (not significant → categories are balanced)
```

#### Correlation Drift (Pearson's r)
```python
corr_drift = corr_curr - corr_prev   # Change in feature relationships year-over-year
```

**Visual EDA produced:** Revenue histogram + KDE + normal curve overlay, price box plots, scatter plots (price vs. quantity), category bar charts, correlation heatmaps

---

### 3b · Gold Layer — Machine Learning

**Notebook:** `Gold_ML.py` | **Runs in parallel with Gold_Stats**

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

features = ["price", "quantity_sold", "discount_percent", "rating", "review_count"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Train / test split: 80% / 20%
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Evaluate with R² score
evaluator = RegressionEvaluator(metricName="r2", labelCol="total_revenue")
```

---

### 4 · Orchestration — Microsoft Fabric Pipeline

The `Amazon_pipeline` in Microsoft Fabric connects all notebooks in a dependency-aware DAG:

```
[Bronze] ──✅──▶ [Silver] ──✅──▶ [Gold_Stats]
                         │
                         └──✅──▶ [Gold_ML]
                         │
                         └──❌──▶ [Office 365 Email Alert]   ← on Bronze failure
```

- **Green arrows (✅):** Run on success
- **Red arrow (❌):** Triggers an Office 365 email alert if Bronze ingestion fails
- Gold_Stats and Gold_ML run **in parallel** after Silver completes

---

## 📊 Statistical Results

| Metric | 2022 (Previous) | 2023 (Current) | Change |
|--------|----------------|----------------|--------|
| Avg Revenue | $657.52 | **$765.80** | +16.5% |
| Std Deviation | $527.85 | **$660.54** | +25.1% |
| Skewness | 0.98 | **1.42** | +44.8% ↑ |
| Kurtosis | 0.27 | **3.06** | +11× ↑↑ |
| Max Revenue | $2,499.55 | **$7,362.26** | +194% |
| Precision (1/σ) | 0.001895 | 0.001514 | More volatile |
| **P-Value** | — | **9.77 × 10⁻⁹¹** | Drift confirmed |
| **is_drifted** | — | **TRUE** | ✅ |

> **Interpretation:** Revenue grew +16.5% but became significantly more volatile. The $7,362 max order (3× the 2022 maximum) is the single biggest driver of kurtosis spike and drift detection. The p-value of `9.77e-91` is statistically indistinguishable from zero — the shift is real, not random.

---

## 🤖 ML Model Results

| Model | 2022 R² | 2023 R² | Change |
|-------|---------|---------|--------|
| Linear Regression | 0.8769 | 0.8322 | -4.5% |
| Decision Tree | 0.9625 | 0.9077 | -5.5% |
| **Random Forest** | **0.9626** | **0.9086** | -5.4% |

**Why Random Forest wins:** Ensemble of 100 trees averages out the impact of extreme revenue outliers ($7,362 orders). No single tree dominates, making it more robust under data drift than a single Decision Tree or a linear model.

**Why all R² scores drop in 2023:** The detected data drift directly causes performance degradation — higher variance (σ²) increases SS_total, and the extreme outlier increases SS_residual, both reducing R².

---

## 📈 Power BI Dashboard

Dashboard (`Statistical_Analysis` workspace in Microsoft Fabric) includes:

- **Bar chart** — Average R² score by model and dataset (Current vs. Previous)
- **Revenue Table** — avg_revenue, std_dev, skewness, kurtosis, is_drifted, p_value
- **Correlation Change charts** — feature-by-feature drift by feature_1 and feature_2
- **Tooltip overlay** — hover over any bar to see exact model/dataset/score

Dashboard auto-refreshes on each pipeline run via the Gold layer SQL Warehouse connection.

---

## 🚀 How to Run the Project

### Prerequisites

```bash
# Azure resources required:
# · Azure Data Lake Storage Gen2 account (name: amazonstore, container: main)
# · Azure Databricks workspace with Unity Catalog enabled
# · Azure Key Vault with secret: container-secret
# · Microsoft Fabric workspace with Databricks linked service
```

### Step 1 — Set Up Unity Catalog

Run `New_Notebook` once to bootstrap the catalog:

```sql
CREATE CATALOG IF NOT EXISTS amazon_project;

CREATE SCHEMA IF NOT EXISTS amazon_project.bronze
  MANAGED LOCATION 'abfss://main@amazonstore.dfs.core.windows.net/bronze';

CREATE SCHEMA IF NOT EXISTS amazon_project.silver
  MANAGED LOCATION 'abfss://main@amazonstore.dfs.core.windows.net/silver';

CREATE SCHEMA IF NOT EXISTS amazon_project.gold
  MANAGED LOCATION 'abfss://main@amazonstore.dfs.core.windows.net/gold';

CREATE TABLE IF NOT EXISTS amazon_project.metadeta.pipeline_logs (
    id STRING, step STRING, status STRING, message STRING, timestamp TIMESTAMP
);
```

### Step 2 — Configure Key Vault

```python
# In Databricks, create a secret scope linked to Azure Key Vault:
dbutils.secrets.listScopes()   # Verify scope "kv-scope" exists
key = dbutils.secrets.get(scope="kv-scope", key="container-secret")
```

### Step 3 — Upload Data

```
ADLS Container: main/
└── bronze/
    ├── amazon_2023_modified.csv   ← "current" (newest file)
    └── amazon_2022.csv            ← "previous" (second newest)
```

### Step 4 — Run the Pipeline

**Option A — Manual (Databricks):**
```
Run in order: utils → Bronze_ingestion → Silver_cleaning → Gold_Stats + Gold_ML
```

**Option B — Automated (Microsoft Fabric):**
```
Open Amazon_pipeline → Click Run
Pipeline auto-executes: Bronze → Silver → [Gold_Stats ‖ Gold_ML]
```

### Step 5 — View Results

```sql
-- Check pipeline logs
SELECT * FROM amazon_project.metadeta.pipeline_logs ORDER BY timestamp DESC;

-- View statistical report
SELECT * FROM amazon_project.gold.statistical_audit;

-- View ML comparison
SELECT * FROM amazon_project.gold.ml_model_comparison ORDER BY r2_score DESC;
```

---

## 📁 Project Structure

```
amazon-sales-pipeline/
│
├── notebooks/
│   ├── utils.py                  # Config, logging, file detection helpers
│   ├── Bronze_ingestion.py       # Raw ingestion + schema validation
│   ├── Silver_cleaning.py        # Cleaning, casting, deduplication
│   ├── Gold_Stats.py             # Stats, drift detection, EDA, ANOVA, t-test
│   └── Gold_ML.py                # LinearReg / DecisionTree / RandomForest
│
├── setup/
│   └── New_Notebook.py           # One-time Unity Catalog + schema bootstrap
│
├── data/
│   ├── amazon_2022.csv           # Previous dataset (24,926 rows)
│   └── amazon_2023_modified.csv  # Current dataset (25,074 rows)
│
└── README.md
```

---

## 📄 Dataset

| Field | Details |
|-------|---------|
| Source | Amazon sales records |
| Columns | `order_id`, `order_date`, `product_id`, `product_category`, `price`, `discount_percent`, `quantity_sold`, `customer_region`, `payment_method`, `rating`, `review_count`, `discounted_price`, `total_revenue` |
| 2022 rows | 24,926 |
| 2023 rows | 25,074 |
| Format | CSV (Bronze) → Delta/Parquet (Silver/Gold) |


