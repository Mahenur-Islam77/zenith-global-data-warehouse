  # Zenith Global — End-to-End Data Warehouse (Medallion Architecture)

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Data Sources](#3-data-sources)
4. [Bronze Layer — Raw Ingestion](#4-bronze-layer--raw-ingestion)
5. [Data Quality Framework](#5-data-quality-framework)
6. [Silver Layer — Cleansed & Standardized](#6-silver-layer--cleansed--standardized)
7. [Gold Layer — Star Schema (Business-Ready)](#7-gold-layer--star-schema-business-ready)
8. [Entity Relationship Diagrams](#8-entity-relationship-diagrams)
9. [Data Lineage & Flow](#9-data-lineage--flow)
10. [How to Run](#10-how-to-run)
11. [Project Structure](#11-project-structure)
12. [Tech Stack](#12-tech-stack)

---

## 1. Project Overview

**Zenith Global** is a fictional multinational cycling company. This project builds a full **data warehouse** from scratch using the **Medallion Architecture** (Bronze → Silver → Gold) on **Microsoft SQL Server**.

It demonstrates a complete data engineering lifecycle:

| Phase | What It Does |
|-------|-------------|
| **Ingestion** | Bulk-load 9 raw CSV files from two source systems (CRM + ERP) into bronze tables |
| **Data Quality** | Run 100+ automated checks across completeness, uniqueness, validity, consistency, referential integrity, and timeliness |
| **Transformation** | Cleanse, standardize, deduplicate, and integrate data into silver tables via stored procedures |
| **Modeling** | Expose business-ready dimension and fact views as a star schema in the gold layer |

**Scale:** ~117,000 source records across 9 datasets, processed through 7 SQL scripts totalling 2,700+ lines of code.

---

## 2. Architecture

### Medallion Architecture Overview

```
 SOURCE SYSTEMS                 DATA WAREHOUSE (SQL Server)
 ══════════════      ════════════════════════════════════════════════════════════

                     ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  CRM (3 CSVs)  ───> │              │    │              │    │              │
                     │    BRONZE    │───>│    SILVER    │───>│     GOLD     │
  ERP (6 CSVs)  ───> │  (Raw Data)  │    │ (Cleansed)   │    │ (Star Schema)│
                     │              │    │              │    │              │
                     └──────────────┘    └──────────────┘    └──────────────┘
                       BULK INSERT        Stored Procs          VIEWs
                       (load as-is)       (transform)        (no ETL needed)
                                     ▲
                                     │
                              ┌──────────────┐
                              │  DQ CHECKS   │
                              │ (100+ rules) │
                              └──────────────┘
```

| Layer | Schema | Object Type | Purpose |
|-------|--------|-------------|---------|
| **Bronze** | `bronze.*` | Tables | 1:1 copy of source data — no transformations |
| **Silver** | `silver.*` | Tables | Cleansed, standardized, deduplicated, integrated |
| **Gold** | `gold.*` | Views | Business-ready star schema for analytics & BI |

---

## 3. Data Sources

The warehouse integrates **two independent source systems**:

### 3.1 CRM — Customer Relationship Management (3 files)

| File | Rows | Description |
|------|------|-------------|
| `crm_customer_info.csv` | 25,301 | Customer demographics — name, gender, marital status, birthdate |
| `crm_product_data.csv` | 5,000 | Product catalog — name, cost, product line |
| `crm_sales_order.csv` | 30,000 | Transactional sales orders — quantities, prices, dates |

### 3.2 ERP — Enterprise Resource Planning (6 files)

| File | Rows | Description |
|------|------|-------------|
| `erp_category_map.csv` | 18 | Reference table — product category/subcategory hierarchy |
| `erp_product_specs.csv` | 5,000 | Product specifications — subcategory, maintenance, product line |
| `erp_stores.csv` | 100 | Store directory — name, type (Flagship/Retail/Online/Outlet), region |
| `erp_returns.csv` | 27,000 | Return transactions — reason, amount |
| `erp_spatial_data.csv` | 25,000 | Customer geography — country, city |
| `erp_territory.csv` | 17 | Reference table — city-country-continent mapping |

### Source System Relationship Map

```
  CRM SYSTEM                              ERP SYSTEM
  ──────────                              ──────────

  ┌───────────────────┐
  │ crm_customer_info │─── customer_id ──────────>┌──────────────────┐
  │                   │                           │ erp_spatial_data │──── city ───>┌───────────────┐
  │  PK: customer_id  │                           │                  │              │ erp_territory │
  └───────┬───────────┘                           └──────────────────┘              │               │
          │                                                                         │ PK: city      │
          │ customer_id                                                             └───────────────┘
          │
  ┌───────┴───────────┐                           ┌──────────────────┐
  │ crm_sales_order   │── order_number ──────────>│  erp_returns     │
  │                   │                           │                  │
  │  PK: order_number │                           │  PK: return_id   │
  └───────┬───────────┘                           └──────────────────┘
          │
          │ product_id
          │
  ┌───────┴───────────┐                           ┌──────────────────┐
  │ crm_product_data  │─── product_id ───────────>│ erp_product_specs│── subcategory ──>┌──────────────────┐
  │                   │                           │                  │                  │ erp_category_map │
  │  PK: product_id   │                           │  PK: product_id  │                  │                  │
  └───────────────────┘                           └──────────────────┘                  │ PK: category_id  │
                                                                                        └──────────────────┘
                                                  ┌──────────────────┐
                                                  │   erp_stores     │  (standalone)
                                                  │  PK: store_id    │
                                                  └──────────────────┘
```

---

## 4. Bronze Layer — Raw Ingestion

**Script:** `ddl_structure_bronze.sql` + `stor_proc_&_load_bronze_layout.sql`

The bronze layer loads all 9 CSV files into SQL Server tables using `BULK INSERT` with zero transformations. Data is stored exactly as received from the source systems.

| Bronze Table | Source File | Columns |
|---|---|---|
| `bronze.crm_customer_info` | crm_customer_info.csv | `customer_id`, `customer_number`, `first_name`, `last_name`, `marital_status`, `gender`, `birthdate`, `create_date` |
| `bronze.crm_product_data` | crm_product_data.csv | `product_id`, `product_number`, `product_name`, `cost`, `product_line`, `start_data` |
| `bronze.crm_sales_order` | crm_sales_order.csv | `order_number`, `product_id`, `customer_id`, `order_date`, `quantity`, `price`, `shipping_date`, `due_date`, `sales_amount`,`store_id` |
| `bronze.erp_category_map` | erp_category_map.csv | `category_id`, `category`, `subcategory` |
| `bronze.erp_product_specs` | erp_product_specs.csv | `product_id`, `subcategory`, `maintenance_required`, `product_line` |
| `bronze.erp_stores` | erp_stores.csv | `store_id`, `store_name`, `store_type`, `region` |
| `bronze.erp_returns` | erp_returns.csv | `return_id`, `order_number`, `return_date`, `return_reason`, `return_amount` |
| `bronze.erp_spatial_data` | erp_spatial_data.csv | `customer_id`, `country`, `city` |
| `bronze.erp_territory` | erp_territory.csv | `city`, `country`, `continent` |

**Load method:** Stored procedure `bronze.load_bronze` truncates all tables then bulk-inserts fresh data. Execution is idempotent — safe to re-run.

---

## 5. Data Quality Framework

**Scripts:** `dq_checks_bronze_crm.sql` + `dq_checks_bronze_erp.sql`

Over **100 individual checks** organized by category:

### 5.1 Check Categories

| Category | What It Catches | Example |
|---|---|---|
| **Completeness** | NULLs, empty strings, missing values | `first_name` is blank for 2+ customers |
| **Uniqueness** | Duplicate primary/business keys | Same `customer_id` appearing twice |
| **Validity** | Invalid codes, formats, ranges | `marital_status = 'D'` instead of `'Divorced'` |
| **Consistency** | Cross-field logic violations | `create_date` before `birthdate` |
| **Referential Integrity** | Orphan foreign keys | Sales order referencing non-existent `product_id` |
| **Timeliness** | Future dates, stale records | `birthdate` in the year 2010 (underage customer) |

### 5.2 Key Data Quality Issues Discovered

```
  ISSUE TYPE                TABLE                    FINDING
  ──────────────────────────────────────────────────────────────────────────

  Whitespace               crm_customer_info        Leading/trailing spaces in first_name,
                                                    gender ('  Female  ')

  Inconsistent Codes       crm_customer_info        marital_status uses abbreviations
                                                    ('D', 'S') mixed with full values
                                                    ('Divorced', 'Single')

  Invalid Values           crm_customer_info        gender = 'n/a' instead of NULL/Unknown

  Case Inconsistency       crm_customer_info        'chris', 'michael' (lowercase)
                                                    vs 'Chris', 'Michael'

  Missing Data             crm_product_data         NULL cost on some products

  Negative Amounts         erp_returns              return_amount = -267

  Country-City Mismatch    erp_spatial_data         country='Germany', city='Sydney'
                                                    (Sydney is in Australia)

  Naming Mismatch          erp_product_specs        Plural subcategories ('Tires', 'Helmets')
                           vs erp_category_map      vs singular ('Tire', 'Helmet')

  Duplicate Names          erp_stores               Same store_name with different store_ids

  Calculated Mismatch      crm_sales_order          sales_amount != quantity * price
```

### 5.3 Check ID Convention

Every check has a unique ID for traceability:

```
  A1a   →  Table A (crm_customer_info), Category 1 (Completeness), Check a
  C4a   →  Table C (crm_sales_order), Category 4 (Consistency), Check a
  H3a   →  Table H (erp_returns), Category 3 (Validity), Check a
  K1    →  Cross-source referential integrity check 1
```

---

## 6. Silver Layer — Cleansed & Standardized

**Scripts:** `etl_bronze_to_silver_crm.sql` + `etl_bronze_to_silver_erp.sql`

### 6.1 Transformation Summary

```
  BRONZE (raw)                  TRANSFORMATION                    SILVER (clean)
  ═══════════                   ══════════════                    ══════════════

  'Jane   '              ──>    LTRIM / RTRIM               ──>  'Jane'
  'michael'              ──>    Proper Case                 ──>  'Michael'
  'D'                    ──>    Code Mapping                ──>  'Divorced'
  'n/a'                  ──>    Standardize                 ──>  'Unknown'
  -267                   ──>    ABS()                       ──>  267
  Germany + Sydney       ──>    Territory Lookup Correction  ──>  Australia + Sydney
  'Tires'                ──>    Singular Normalization      ──>  'Tire'
  qty*price != amount    ──>    Recalculate                 ──>  qty * price
  Duplicate rows         ──>    ROW_NUMBER() dedup          ──>  Single row per PK
  NULL primary key       ──>    Filter out                  ──>  Excluded
  start_data (typo)      ──>    Rename column               ──>  start_date
```

### 6.2 Table-by-Table Transformations

| Silver Table | Source | Key Transformations |
|---|---|---|
| `silver.crm_customer_info` | bronze.crm_customer_info | Dedup on `customer_id`; trim all strings; proper-case names; map `D→Divorced`, `S→Single`, `M→Married`; replace `n/a` gender → `Unknown`; blank names → `Unknown` |
| `silver.crm_product_data` | bronze.crm_product_data | Dedup on `product_id`; trim all strings; NULL out invalid cost (≤0); standardize `product_line`; rename `start_data` → `start_date` |
| `silver.crm_sales_order` | bronze.crm_sales_order | Dedup on `order_number`; trim strings; NULL out invalid qty/price (≤0); NULL illogical dates; recalculate `sales_amount = quantity * price` |
| `silver.erp_category_map` | bronze.erp_category_map | Dedup on `category_id`; trim strings; blanks → `Unknown` |
| `silver.erp_product_specs` | bronze.erp_product_specs | Dedup on `product_id`; normalize plural subcategories → singular (`Tires→Tire`, `Helmets→Helmet`, `Road Bikes→Road`, `Mountain Bikes→Mountain`, `Jerseys→Jersey`); standardize `maintenance_required` |
| `silver.erp_stores` | bronze.erp_stores | Dedup on `store_id`; trim strings; standardize `store_type` and `region` |
| `silver.erp_returns` | bronze.erp_returns | Dedup on `return_id`; trim strings; `ABS()` negative `return_amount`; standardize `return_reason` |
| `silver.erp_spatial_data` | bronze.erp_spatial_data + bronze.erp_territory | Dedup on `customer_id`; **correct country using territory lookup** (city → true country); trim strings |
| `silver.erp_territory` | bronze.erp_territory | Dedup on `city`; trim strings; blanks → `Unknown` |

All silver tables include a `dwh_load_date` audit column.

### 6.3 Load Method

```sql
EXEC silver.load_silver_crm;   -- loads 3 CRM silver tables
EXEC silver.load_silver_erp;   -- loads 6 ERP silver tables
```

Both procedures: truncate → insert (full refresh), with TRY/CATCH error handling and per-table timing.

---

## 7. Gold Layer — Star Schema (Business-Ready)

**Script:** `ddl_gold_layer_views.sql`

The gold layer is implemented entirely as **SQL Views** — no ETL procedure needed. Views always reflect the latest silver data automatically.

### 7.1 Star Schema Diagram

```
                          ┌─────────────────────────-┐
                          │    gold.dim_customer     │
                          │───────────────────────── │
                          │ customer_id         (PK) │
                          │ customer_number          │
                          │ first_name               │
                          │ last_name                │
                          │ full_name          (der) │
                          │ marital_status           │
                          │ gender                   │
                          │ birthdate                │
                          │ age                (der) │
                          │ age_group          (der) │
                          │ create_date              │
                          │ city                     │
                          │ country                  │
                          │ continent                │
                          └────────────┬────────────-┘
                                       │
                                  customer_id
                                       │
┌─────────────────────────┐    ┌───────┴─────────────────┐    ┌─────────────────────────┐
│    gold.dim_product     │    │    gold.fact_sales      │    │   gold.fact_returns     │
│─────────────────────────│    │─────────────────────────│    │─────────────────────────│
│ product_id         (PK) │    │ order_number       (PK) │    │ return_id          (PK) │
│ product_number          │    │ product_id         (FK) │    │ order_number       (FK) │
│ product_name            │    │ customer_id        (FK) │    │ return_date             │
│ cost                    │    │ order_date              │    │ return_year        (der)│
│ product_line            │    │ shipping_date           │    │ return_month       (der)│
│ start_date              │◄───┤ due_date                ├───►│ return_month_name  (der)│
│ category                │    │ order_year         (der)│    │ return_reason           │
│ subcategory             │    │ order_month        (der)│    │ return_amount           │
│ maintenance_required    │    │ order_month_name   (der)│    └─────────────────────────┘
└─────────────────────────┘    │ quantity                │
                               │ price                   │
                               │ sales_amount            │    ┌─────────────────────────┐
                               │ days_to_ship       (der)│    │    gold.dim_store       │
                               │ days_to_due        (der)│    │─────────────────────────│
                               │ delivery_status    (der)│    │ store_id           (PK) │
                               └─────────────────────────┘    │ store_name              │
                                                              │ store_type              │
                                       (der) = derived field  │ region                  │
                                       (PK)  = primary key    └─────────────────────────┘
                                       (FK)  = foreign key        (standalone dimension)
```

### 7.2 Dimension Views

| View | Silver Sources Joined | Derived Fields |
|---|---|---|
| **`gold.dim_customer`** | `crm_customer_info` + `erp_spatial_data` + `erp_territory` | `full_name`, `age` (exact), `age_group` (Under 20 / 20-29 / 30-39 / 40-49 / 50-59 / 60+), `city`, `country`, `continent` |
| **`gold.dim_product`** | `crm_product_data` + `erp_product_specs` + `erp_category_map` | `category`, `subcategory`, `maintenance_required` |
| **`gold.dim_store`** | `erp_stores` | Clean pass-through |

### 7.3 Fact Views

| View | Silver Source | Derived Fields |
|---|---|---|
| **`gold.fact_sales`** | `crm_sales_order` | `order_year`, `order_month`, `order_month_name`, `days_to_ship`, `days_to_due`, `delivery_status` (On Time / Late) |
| **`gold.fact_returns`** | `erp_returns` | `return_year`, `return_month`, `return_month_name` |

### 7.4 Join Paths for BI Tools

```sql
-- Full sales analysis with customer and product context
SELECT *
FROM gold.fact_sales      fs
JOIN gold.dim_customer     dc ON fs.customer_id = dc.customer_id
JOIN gold.dim_product      dp ON fs.product_id  = dp.product_id;

-- Returns analysis linked back to the original sale
SELECT *
FROM gold.fact_returns     fr
JOIN gold.fact_sales       fs ON fr.order_number = fs.order_number
JOIN gold.dim_customer     dc ON fs.customer_id  = dc.customer_id;
```

---

## 8. Entity Relationship Diagrams

### 8.1 Bronze Layer — Source Tables (as loaded)

```
  crm_customer_info              crm_sales_order               crm_product_data
  ┌──────────────────┐           ┌──────────────────┐          ┌──────────────────┐
  │ customer_id (PK) │◄──────────┤ customer_id (FK) │          │ product_id  (PK) │
  │ customer_number  │           │ order_number (PK)│         │ product_number   │
  │ first_name       │           │ product_id  (FK) ├─────────►│ product_name     │
  │ last_name        │           │ order_date       │          │ cost             │
  │ marital_status   │           │ quantity         │          │ product_line     │
  │ gender           │           │ price            │          │ start_data       │
  │ birthdate        │           │ shipping_date    │          └────────┬─────────┘
  │ create_date      │           │ due_date         │                   │
  └────────┬─────────┘           │ sales_amount     │            product_id
           │                     └────────┬─────────┘                   │
      customer_id                         │                    ┌────────┴─────────┐
           │                        order_number               │ erp_product_specs│
  ┌────────┴─────────┐                    │                    │ product_id  (PK) │
  │ erp_spatial_data  │           ┌───────┴──────────┐         │ subcategory      │──────┐
  │ customer_id (PK) │           │  erp_returns      │         │ maintenance_req  │      │
  │ country          │           │  return_id  (PK)  │         │ product_line     │  subcategory
  │ city             │───┐       │  order_number(FK) │         └──────────────────┘      │
  └──────────────────┘   │       │  return_date      │                            ┌──────┴───────────┐
                      city│      │  return_reason    │                            │ erp_category_map  │
                         │       │  return_amount    │                            │ category_id (PK)  │
                 ┌───────┴──────┐└──────────────────┘                             │ category          │
                 │erp_territory │                                                 │ subcategory       │
                 │ city    (PK) │          ┌──────────────────┐                   └──────────────────┘
                 │ country      │          │   erp_stores     │
                 │ continent    │          │ store_id    (PK) │
                 └──────────────┘          │ store_name       │
                                           │ store_type       │
                                           │ region           │
                                           └──────────────────┘
```

### 8.2 Gold Layer — Star Schema (Final Model)

```
                           ┌──────────────────┐
                           │  dim_customer    │
                           │  ────────────    │
                           │  customer_id (PK)│
                           │  + 13 attributes │
                           └────────┬─────────┘
                                    │ 1
                                    │
                                    │ *
  ┌──────────────────┐     ┌────────┴─────────┐     ┌──────────────────┐
  │  dim_product     │     │   fact_sales     │     │  fact_returns    │
  │  ────────────    │  1  │   ──────────     │  1  │  ────────────    │
  │  product_id (PK) │◄────┤  order_number(PK)├────►│  return_id (PK)  │
  │  + 8 attributes  │  *  │  + 14 attributes │  *  │  + 7 attributes  │
  └──────────────────┘     └──────────────────┘     └──────────────────┘

  ┌──────────────────┐
  │  dim_store       │       1 = one side of relationship
  │  ────────────    │       * = many side of relationship
  │  store_id  (PK)  │
  │  + 3 attributes  │       Cardinality:
  └──────────────────┘         1 customer  → * sales orders
                               1 product   → * sales orders
                               1 order     → * returns
```

---

## 9. Data Lineage & Flow

### 9.1 End-to-End Pipeline

```
  ┌──────────┐     ┌──────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────┐
  │          │     │          │     │              │     │               │     │          │
  │  CSV     │────>│  BRONZE  │────>│  DQ CHECKS   │────>│    SILVER     │────>│   GOLD   │
  │  Files   │     │  Tables  │     │  (validate)  │     │    Tables     │     │  Views   │
  │          │     │          │     │              │     │               │     │          │
  └──────────┘     └──────────┘     └──────────────┘     └───────────────┘     └──────────┘
   9 files          9 tables         100+ checks          9 tables              5 views
   2 systems        BULK INSERT      6 categories         Stored Procs          Star Schema
```

### 9.2 Column-Level Lineage: dim_customer

```
  BRONZE                          SILVER                         GOLD
  ──────                          ──────                         ────

  crm_customer_info               crm_customer_info              dim_customer
  ├─ customer_id          ──────> ├─ customer_id         ──────> ├─ customer_id
  ├─ customer_number      ──────> ├─ customer_number     ──────> ├─ customer_number
  ├─ first_name     (trim+case)─> ├─ first_name          ──────> ├─ first_name
  ├─ last_name      (trim+case)─> ├─ last_name           ──────> ├─ last_name
  │                                                       (concat)├─ full_name ★
  ├─ marital_status    (mapped)─> ├─ marital_status      ──────> ├─ marital_status
  ├─ gender          (cleaned)──> ├─ gender              ──────> ├─ gender
  ├─ birthdate          ──────>   ├─ birthdate           ──────> ├─ birthdate
  │                                                    (derived) ├─ age ★
  │                                                    (derived) ├─ age_group ★
  ├─ create_date        ──────>   ├─ create_date         ──────> ├─ create_date
                                                                 │
  erp_spatial_data                erp_spatial_data               │
  ├─ country     (corrected)───>  ├─ country             ──────> ├─ country
  ├─ city               ──────>  ├─ city                ──────> ├─ city
                                                                 │
  erp_territory                   erp_territory                  │
  ├─ continent          ──────>  ├─ continent           ──────> ├─ continent

  ★ = field created in the gold layer (derived / calculated)
```

### 9.3 Column-Level Lineage: dim_product

```
  BRONZE                          SILVER                         GOLD
  ──────                          ──────                         ────

  crm_product_data                crm_product_data               dim_product
  ├─ product_id         ──────>   ├─ product_id          ──────> ├─ product_id
  ├─ product_number     ──────>   ├─ product_number      ──────> ├─ product_number
  ├─ product_name       ──────>   ├─ product_name        ──────> ├─ product_name
  ├─ cost          (validated)──> ├─ cost                ──────> ├─ cost
  ├─ product_line       ──────>   ├─ product_line        ──────> ├─ product_line
  ├─ start_data     (renamed)──>  ├─ start_date          ──────> ├─ start_date
                                                                 │
  erp_product_specs               erp_product_specs              │
  ├─ subcategory  (normalized)──> ├─ subcategory         ──┐     │
  ├─ maintenance_required ─────>  ├─ maintenance_required──┼───> ├─ maintenance_required
                                                           │     │
  erp_category_map                erp_category_map         │     │
  ├─ category           ──────>  ├─ category      <──(join)┘──> ├─ category
  ├─ subcategory        ──────>  ├─ subcategory          ──────> ├─ subcategory
```

---

## 10. How to Run

### Prerequisites

- Microsoft SQL Server (2016 or later)
- A database with the CSV files accessible at the paths in the scripts

### Execution Order

```
  Step 1:  ddl_structure_bronze.sql              Create bronze tables
  Step 2:  stor_proc_&_load_bronze_layout.sql    Create & run bronze load proc
           EXEC bronze.load_bronze;

  Step 3:  dq_checks_bronze_crm.sql              Run CRM quality checks
  Step 4:  dq_checks_bronze_erp.sql              Run ERP quality checks

  Step 5:  etl_bronze_to_silver_crm.sql          Create silver tables & CRM proc
           EXEC silver.load_silver_crm;

  Step 6:  etl_bronze_to_silver_erp.sql          Create silver tables & ERP proc
           EXEC silver.load_silver_erp;

  Step 7:  ddl_gold_layer_views.sql              Create gold star schema views
```

### Refresh Pipeline (after initial setup)

```sql
-- Re-run these three commands to fully refresh the warehouse:
EXEC bronze.load_bronze;          -- reload raw data
EXEC silver.load_silver_crm;      -- refresh CRM silver
EXEC silver.load_silver_erp;      -- refresh ERP silver
-- Gold views refresh automatically (they are views, not tables)
```

---

## 11. Project Structure

```
zenith-global/
│
├── datasets/
│   ├── source_crm/
│   │   ├── crm_customer_info.csv        (25,301 rows)
│   │   ├── crm_product_data.csv         ( 5,000 rows)
│   │   └── crm_sales_order.csv          (30,000 rows)
│   │
│   └── source_erp/
│       ├── erp_category_map.csv         (    18 rows)
│       ├── erp_product_specs.csv        ( 5,000 rows)
│       ├── erp_stores.csv               (   100 rows)
│       ├── erp_returns.csv              (27,000 rows)
│       ├── erp_spatial_data.csv         (25,000 rows)
│       └── erp_territory.csv            (    17 rows)
│
├── scripts/
│   ├── ddl_structure_bronze.sql                  Bronze table DDL
│   ├── stor_proc_&_load_bronze_layout.sql        Bronze load stored procedure
│   ├── dq_checks_bronze_crm.sql                  DQ checks (3 CRM tables)
│   ├── dq_checks_bronze_erp.sql                  DQ checks (6 ERP tables + cross-source)
│   ├── etl_bronze_to_silver_crm.sql              Silver ETL (3 CRM tables)
│   ├── etl_bronze_to_silver_erp.sql              Silver ETL (6 ERP tables)
│   └── ddl_gold_layer_views.sql                  Gold star schema views
│
└── README.md                                     This documentation
```

---

## 12. Tech Stack

| Component | Technology |
|---|---|
| **Database** | Microsoft SQL Server |
| **Language** | T-SQL |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Data Model** | Star Schema (Kimball methodology) |
| **Gold Objects** | SQL Views (always current, zero-latency) |
| **Load Pattern** | Full refresh (truncate + insert) |
| **Error Handling** | TRY/CATCH with detailed error reporting |
| **Audit** | `dwh_load_date` timestamp on all silver tables |

---

> **Author:** Built as a portfolio project demonstrating end-to-end data warehouse design, data quality engineering, and ETL development using SQL Server.
