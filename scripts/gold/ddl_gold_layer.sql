/*
===============================================================================
Script Purpose:
    Gold Layer: Business-Ready Dimensional Model (Star Schema as VIEWS)

    This script creates the Gold schema and builds VIEW-based dimension and
    fact objects on top of the Silver layer tables. Because these are views
    (not materialised tables), they always reflect the latest silver data
    without requiring a separate ETL load step.

    Dimensional Model:
      DIMENSIONS
        1. gold.dim_customer   — Customer profile + geography + demographics
        2. gold.dim_product    — Product catalog + category + specs
        3. gold.dim_store      — Store/channel reference

      FACTS
        4. gold.fact_sales     — Grain: one row per sales order
        5. gold.fact_returns   — Grain: one row per return transaction

    Join Paths (for BI tools / analysts):
      fact_sales  -->  dim_customer   ON customer_id
      fact_sales  -->  dim_product    ON product_id
      fact_returns -->  fact_sales    ON order_number

Usage:
    Run this script once to create all views.
    Views are always up-to-date — just query them directly:
        SELECT * FROM gold.dim_customer;
        SELECT * FROM gold.fact_sales;
===============================================================================
*/
USE Zenith_Global_Data_Warehouse;
-- ============================================================================
-- STEP 1: CREATE GOLD SCHEMA (if not exists)
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

-- ============================================================================
-- STEP 2: DIMENSION VIEWS
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 2a. gold.dim_customer
--     Integrates: crm_customer_info + erp_spatial_data + erp_territory
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_customer
AS
SELECT
    ci.customer_id,
    ci.customer_number,
    ci.first_name,
    ci.last_name,
    ci.first_name + ' ' + ci.last_name              AS full_name,
    ci.marital_status,
    ci.gender,
    ci.birthdate,
    -- Age derived from birthdate (NULL-safe)
    CASE
        WHEN ci.birthdate IS NOT NULL
            THEN DATEDIFF(YEAR, ci.birthdate, GETDATE())
                 - CASE
                       WHEN DATEADD(YEAR, DATEDIFF(YEAR, ci.birthdate, GETDATE()), ci.birthdate) > GETDATE()
                       THEN 1
                       ELSE 0
                   END
        ELSE NULL
    END                                              AS age,
    -- Age group segmentation
    CASE
        WHEN ci.birthdate IS NULL THEN 'Unknown'
        WHEN DATEDIFF(YEAR, ci.birthdate, GETDATE()) < 20  THEN 'Under 20'
        WHEN DATEDIFF(YEAR, ci.birthdate, GETDATE()) < 30  THEN '20-29'
        WHEN DATEDIFF(YEAR, ci.birthdate, GETDATE()) < 40  THEN '30-39'
        WHEN DATEDIFF(YEAR, ci.birthdate, GETDATE()) < 50  THEN '40-49'
        WHEN DATEDIFF(YEAR, ci.birthdate, GETDATE()) < 60  THEN '50-59'
        ELSE '60+'
    END                                              AS age_group,
    ci.create_date,
    -- Geography from ERP spatial + territory
    COALESCE(sd.city, 'Unknown')                     AS city,
    COALESCE(sd.country, 'Unknown')                  AS country,
    COALESCE(t.continent, 'Unknown')                 AS continent
FROM silver.crm_customer_info ci
LEFT JOIN silver.erp_spatial_data sd
    ON ci.customer_id = sd.customer_id
LEFT JOIN silver.erp_territory t
    ON sd.city = t.city;
GO

-- ---------------------------------------------------------------------------
-- 2b. gold.dim_product
--     Integrates: crm_product_data + erp_product_specs + erp_category_map
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_product
AS
SELECT
    pd.product_id,
    pd.product_number,
    pd.product_name,
    pd.cost,
    pd.product_line,
    pd.start_date,
    -- Category hierarchy from ERP
    COALESCE(cm.category, 'Unknown')                 AS category,
    COALESCE(cm.subcategory, 'Unknown')              AS subcategory,
    -- Product specs from ERP
    COALESCE(ps.maintenance_required, 'Unknown')     AS maintenance_required
FROM silver.crm_product_data pd
LEFT JOIN silver.erp_product_specs ps
    ON pd.product_id = ps.product_id
LEFT JOIN silver.erp_category_map cm
    ON ps.subcategory = cm.subcategory;
GO

-- ---------------------------------------------------------------------------
-- 2c. gold.dim_store
--     Source: erp_stores
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.dim_store
AS
SELECT
    store_id,
    store_name,
    store_type,
    region
FROM silver.erp_stores;
GO

-- ============================================================================
-- STEP 3: FACT VIEWS
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 3a. gold.fact_sales
--     Grain: one row per sales order
--     Source: crm_sales_order
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_sales
AS
SELECT
    so.order_number,
    so.product_id,
    so.customer_id,
    so.order_date,
    so.shipping_date,
    so.due_date,
    -- Date-derived fields for easier analysis
    YEAR(so.order_date)                              AS order_year,
    MONTH(so.order_date)                             AS order_month,
    DATENAME(MONTH, so.order_date)                   AS order_month_name,
    so.quantity,
    so.price,
    so.sales_amount,
    -- Delivery performance
    DATEDIFF(DAY, so.order_date, so.shipping_date)   AS days_to_ship,
    DATEDIFF(DAY, so.order_date, so.due_date)        AS days_to_due,
    CASE
        WHEN so.shipping_date <= so.due_date THEN 'On Time'
        WHEN so.shipping_date >  so.due_date THEN 'Late'
        ELSE 'Unknown'
    END                                              AS delivery_status
FROM silver.crm_sales_order so;
GO

-- ---------------------------------------------------------------------------
-- 3b. gold.fact_returns
--     Grain: one row per return transaction
--     Source: erp_returns
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW gold.fact_returns
AS
SELECT
    r.return_id,
    r.order_number,
    r.return_date,
    -- Date-derived fields
    YEAR(r.return_date)                              AS return_year,
    MONTH(r.return_date)                             AS return_month,
    DATENAME(MONTH, r.return_date)                   AS return_month_name,
    r.return_reason,
    r.return_amount
FROM silver.erp_returns r;
GO

PRINT '============================================='
PRINT 'GOLD LAYER VIEWS CREATED SUCCESSFULLY'
PRINT '============================================='
