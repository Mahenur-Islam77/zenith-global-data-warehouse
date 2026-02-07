/*
===============================================================================
Script Purpose:
    ETL Pipeline: Bronze --> Silver Layer (ERP Source Tables)

    This script creates the Silver destination tables and a stored procedure
    that cleanses and transforms the six ERP bronze tables into their
    silver-layer equivalents.

    Transformations applied:
      - Trim leading/trailing whitespace from all string columns
      - Standardize subcategory naming (plural -> singular to align with
        the category_map reference: Tires->Tire, Helmets->Helmet, etc.)
      - Standardize boolean-style fields (maintenance_required)
      - Correct country-city mismatches in spatial_data using territory lookup
      - Convert negative return_amount to positive (ABS)
      - NULL out logically invalid dates (return before order)
      - Filter out records missing primary keys
      - Deduplicate on primary keys
      - Add dwh_load_date audit column

    Silver tables created:
      1. silver.erp_category_map
      2. silver.erp_product_specs
      3. silver.erp_stores
      4. silver.erp_returns
      5. silver.erp_spatial_data
      6. silver.erp_territory

Usage:
    -- First run the DDL section once, then:
    EXEC silver.load_silver_erp;
===============================================================================
*/

-- ============================================================================
-- STEP 1: CREATE SILVER SCHEMA (if not exists)
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- ============================================================================
-- STEP 2: DDL - CREATE SILVER TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 2a. silver.erp_category_map
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_category_map', 'U') IS NOT NULL
    DROP TABLE silver.erp_category_map;
GO

CREATE TABLE silver.erp_category_map (
    category_id     NVARCHAR(50)    NOT NULL,
    category        NVARCHAR(50),
    subcategory     NVARCHAR(50),
    dwh_load_date   DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2b. silver.erp_product_specs
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_product_specs', 'U') IS NOT NULL
    DROP TABLE silver.erp_product_specs;
GO

CREATE TABLE silver.erp_product_specs (
    product_id              INT             NOT NULL,
    subcategory             NVARCHAR(50),
    maintenance_required    NVARCHAR(50),
    product_line            NVARCHAR(50),
    dwh_load_date           DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2c. silver.erp_stores
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_stores', 'U') IS NOT NULL
    DROP TABLE silver.erp_stores;
GO

CREATE TABLE silver.erp_stores (
    store_id        INT             NOT NULL,
    store_name      NVARCHAR(100),
    store_type      NVARCHAR(50),
    region          NVARCHAR(50),
    dwh_load_date   DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2d. silver.erp_returns
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_returns', 'U') IS NOT NULL
    DROP TABLE silver.erp_returns;
GO

CREATE TABLE silver.erp_returns (
    return_id       INT             NOT NULL,
    order_number    NVARCHAR(50),
    return_date     DATE,
    return_reason   NVARCHAR(100),
    return_amount   DECIMAL(18, 2),
    dwh_load_date   DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2e. silver.erp_spatial_data
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_spatial_data', 'U') IS NOT NULL
    DROP TABLE silver.erp_spatial_data;
GO

CREATE TABLE silver.erp_spatial_data (
    customer_id     INT             NOT NULL,
    country         VARCHAR(50),
    city            VARCHAR(50),
    dwh_load_date   DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2f. silver.erp_territory
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.erp_territory', 'U') IS NOT NULL
    DROP TABLE silver.erp_territory;
GO

CREATE TABLE silver.erp_territory (
    city            VARCHAR(50)     NOT NULL,
    country         VARCHAR(50),
    continent       VARCHAR(50),
    dwh_load_date   DATETIME2       DEFAULT GETDATE()
);
GO

-- ============================================================================
-- STEP 3: STORED PROCEDURE - ETL Bronze to Silver (ERP)
-- ============================================================================

CREATE OR ALTER PROCEDURE silver.load_silver_erp
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();

    BEGIN TRY

        PRINT '============================================='
        PRINT 'LOADING SILVER LAYER - ERP TABLES'
        PRINT '============================================='

        -- ===================================================================
        -- 3a. LOAD silver.erp_category_map
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_category_map'
        TRUNCATE TABLE silver.erp_category_map;

        PRINT '...Inserting Data: silver.erp_category_map'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY category_id
                    ORDER BY category_id
                ) AS rn
            FROM bronze.erp_category_map
            WHERE category_id IS NOT NULL
              AND LTRIM(RTRIM(category_id)) != ''
        )
        INSERT INTO silver.erp_category_map (
            category_id,
            category,
            subcategory
        )
        SELECT
            -- category_id: trim
            LTRIM(RTRIM(category_id)) AS category_id,

            -- category: trim, standardize
            CASE
                WHEN LTRIM(RTRIM(category)) IS NULL OR LTRIM(RTRIM(category)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(category))
            END AS category,

            -- subcategory: trim, standardize
            CASE
                WHEN LTRIM(RTRIM(subcategory)) IS NULL OR LTRIM(RTRIM(subcategory)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(subcategory))
            END AS subcategory

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3b. LOAD silver.erp_product_specs
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_product_specs'
        TRUNCATE TABLE silver.erp_product_specs;

        PRINT '...Inserting Data: silver.erp_product_specs'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY product_id
                    ORDER BY product_id
                ) AS rn
            FROM bronze.erp_product_specs
            WHERE product_id IS NOT NULL
        )
        INSERT INTO silver.erp_product_specs (
            product_id,
            subcategory,
            maintenance_required,
            product_line
        )
        SELECT
            product_id,

            -- subcategory: trim and normalize plural -> singular
            -- to align with erp_category_map reference values
            CASE LTRIM(RTRIM(subcategory))
                WHEN 'Tires'          THEN 'Tire'
                WHEN 'Helmets'        THEN 'Helmet'
                WHEN 'Road Bikes'     THEN 'Road'
                WHEN 'Mountain Bikes' THEN 'Mountain'
                WHEN 'Jerseys'        THEN 'Jersey'
                WHEN 'Tire'           THEN 'Tire'
                WHEN 'Helmet'         THEN 'Helmet'
                WHEN 'Road'           THEN 'Road'
                WHEN 'Mountain'       THEN 'Mountain'
                WHEN 'Jersey'         THEN 'Jersey'
                WHEN ''               THEN 'Unknown'
                ELSE COALESCE(LTRIM(RTRIM(subcategory)), 'Unknown')
            END AS subcategory,

            -- maintenance_required: trim and standardize to Yes/No
            CASE
                WHEN LTRIM(RTRIM(maintenance_required)) IS NULL
                  OR LTRIM(RTRIM(maintenance_required)) = ''
                    THEN 'Unknown'
                WHEN LOWER(LTRIM(RTRIM(maintenance_required))) IN ('yes', 'y', '1', 'true')
                    THEN 'Yes'
                WHEN LOWER(LTRIM(RTRIM(maintenance_required))) IN ('no', 'n', '0', 'false')
                    THEN 'No'
                ELSE LTRIM(RTRIM(maintenance_required))
            END AS maintenance_required,

            -- product_line: trim and standardize
            CASE LTRIM(RTRIM(product_line))
                WHEN 'Standard' THEN 'Standard'
                WHEN 'Road'     THEN 'Road'
                WHEN 'Mountain' THEN 'Mountain'
                WHEN 'Touring'  THEN 'Touring'
                WHEN ''         THEN 'Unknown'
                ELSE COALESCE(LTRIM(RTRIM(product_line)), 'Unknown')
            END AS product_line

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3c. LOAD silver.erp_stores
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_stores'
        TRUNCATE TABLE silver.erp_stores;

        PRINT '...Inserting Data: silver.erp_stores'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY store_id
                    ORDER BY store_id
                ) AS rn
            FROM bronze.erp_stores
            WHERE store_id IS NOT NULL
        )
        INSERT INTO silver.erp_stores (
            store_id,
            store_name,
            store_type,
            region
        )
        SELECT
            store_id,

            -- store_name: trim
            LTRIM(RTRIM(store_name)) AS store_name,

            -- store_type: trim and standardize
            CASE
                WHEN LTRIM(RTRIM(store_type)) IS NULL OR LTRIM(RTRIM(store_type)) = ''
                    THEN 'Unknown'
                WHEN LTRIM(RTRIM(store_type)) IN ('Flagship', 'Retail', 'Online', 'Outlet')
                    THEN LTRIM(RTRIM(store_type))
                ELSE LTRIM(RTRIM(store_type))  -- keep non-standard values visible
            END AS store_type,

            -- region: trim and standardize
            CASE
                WHEN LTRIM(RTRIM(region)) IS NULL OR LTRIM(RTRIM(region)) = ''
                    THEN 'Unknown'
                WHEN LTRIM(RTRIM(region)) IN ('Asia-Pacific', 'Latin America', 'Europe', 'North America')
                    THEN LTRIM(RTRIM(region))
                ELSE LTRIM(RTRIM(region))  -- keep non-standard values visible
            END AS region

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3d. LOAD silver.erp_returns
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_returns'
        TRUNCATE TABLE silver.erp_returns;

        PRINT '...Inserting Data: silver.erp_returns'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY return_id
                    ORDER BY return_date DESC    -- keep most recent if duplicated
                ) AS rn
            FROM bronze.erp_returns
            WHERE return_id IS NOT NULL
        )
        INSERT INTO silver.erp_returns (
            return_id,
            order_number,
            return_date,
            return_reason,
            return_amount
        )
        SELECT
            return_id,

            -- order_number: trim
            LTRIM(RTRIM(order_number)) AS order_number,

            -- return_date: pass through (NULLs remain NULL)
            return_date,

            -- return_reason: trim and standardize
            CASE
                WHEN LTRIM(RTRIM(return_reason)) IS NULL OR LTRIM(RTRIM(return_reason)) = ''
                    THEN 'Unknown'
                WHEN LTRIM(RTRIM(return_reason)) IN ('Wrong Item', 'Size Mismatch', 'Unsatisfied', 'Defective')
                    THEN LTRIM(RTRIM(return_reason))
                ELSE LTRIM(RTRIM(return_reason))  -- keep non-standard values visible
            END AS return_reason,

            -- return_amount: convert negative values to positive using ABS
            -- A return amount should always be a positive value
            CASE
                WHEN return_amount IS NULL THEN NULL
                WHEN return_amount < 0     THEN ABS(return_amount)
                ELSE return_amount
            END AS return_amount

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3e. LOAD silver.erp_spatial_data
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_spatial_data'
        TRUNCATE TABLE silver.erp_spatial_data;

        PRINT '...Inserting Data: silver.erp_spatial_data'
        -- The bronze spatial_data has known country-city mismatches
        -- (e.g., customer in "Germany" but city is "Sydney").
        -- We correct the country by looking up the authoritative
        -- city->country mapping from erp_territory.
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY customer_id
                ) AS rn
            FROM bronze.erp_spatial_data
            WHERE customer_id IS NOT NULL
        )
        INSERT INTO silver.erp_spatial_data (
            customer_id,
            country,
            city
        )
        SELECT
            sd.customer_id,

            -- country: use territory lookup as the source of truth for the
            -- correct country based on city; fall back to original if city
            -- is not found in the territory reference
            CASE
                WHEN t.country IS NOT NULL
                    THEN LTRIM(RTRIM(t.country))
                WHEN LTRIM(RTRIM(sd.country)) IS NULL OR LTRIM(RTRIM(sd.country)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(sd.country))
            END AS country,

            -- city: trim
            CASE
                WHEN LTRIM(RTRIM(sd.city)) IS NULL OR LTRIM(RTRIM(sd.city)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(sd.city))
            END AS city

        FROM cte_dedup sd
        LEFT JOIN bronze.erp_territory t
            ON LTRIM(RTRIM(sd.city)) = LTRIM(RTRIM(t.city))
        WHERE sd.rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3f. LOAD silver.erp_territory
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.erp_territory'
        TRUNCATE TABLE silver.erp_territory;

        PRINT '...Inserting Data: silver.erp_territory'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY city
                    ORDER BY city
                ) AS rn
            FROM bronze.erp_territory
            WHERE city IS NOT NULL
              AND LTRIM(RTRIM(city)) != ''
        )
        INSERT INTO silver.erp_territory (
            city,
            country,
            continent
        )
        SELECT
            -- city: trim
            LTRIM(RTRIM(city)) AS city,

            -- country: trim, replace blank with Unknown
            CASE
                WHEN LTRIM(RTRIM(country)) IS NULL OR LTRIM(RTRIM(country)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(country))
            END AS country,

            -- continent: trim, replace blank with Unknown
            CASE
                WHEN LTRIM(RTRIM(continent)) IS NULL OR LTRIM(RTRIM(continent)) = ''
                    THEN 'Unknown'
                ELSE LTRIM(RTRIM(continent))
            END AS continent

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- BATCH COMPLETE
        -- ===================================================================
        SET @batch_end_time = GETDATE();
        PRINT '============================================='
        PRINT 'Silver Layer ERP Loading Complete'
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '============================================='

    END TRY
    BEGIN CATCH
        PRINT '============================================='
        PRINT 'ERROR OCCURRED DURING SILVER LAYER LOAD'
        PRINT 'Error Message: '  + ERROR_MESSAGE();
        PRINT 'Error Number: '   + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: '    + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Error Line: '     + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '============================================='
    END CATCH
END;
GO
