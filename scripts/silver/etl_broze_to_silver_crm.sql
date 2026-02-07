/*
===============================================================================
Script Purpose:
    ETL Pipeline: Bronze --> Silver Layer (CRM Source Tables)

    This script creates the Silver schema, destination tables, and a stored
    procedure that cleanses and transforms the three CRM bronze tables into
    their silver-layer equivalents.

    Transformations applied:
      - Trim leading/trailing whitespace from all string columns
      - Standardize casing (proper case for names, consistent codes)
      - Map abbreviated/invalid codes to full standardized values
      - Replace 'n/a', blanks, and NULLs with 'Unknown' where appropriate
      - Recalculate derived fields (sales_amount)
      - Filter out records missing primary keys
      - Deduplicate on primary keys (keep latest record)
      - Rename misnamed columns (start_data -> start_date)

    Silver tables created:
      1. silver.crm_customer_info
      2. silver.crm_product_data
      3. silver.crm_sales_order

Usage:
    -- First run the DDL section once, then:
    EXEC silver.load_silver_crm;
===============================================================================

*/
-- ============================================================================
-- STEP 1: CREATE SILVER SCHEMA (if not exists)
-- ============================================================================
USE Zenith_Global_Data_Warehouse;

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
END
GO

-- ============================================================================
-- STEP 2: DDL - CREATE SILVER TABLES
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 2a. silver.crm_customer_info
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_customer_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_customer_info;
GO

CREATE TABLE silver.crm_customer_info (
    customer_id         INT             NOT NULL,
    customer_number     NVARCHAR(50)    NOT NULL,
    first_name          NVARCHAR(50),
    last_name           NVARCHAR(50),
    marital_status      NVARCHAR(50),
    gender              NVARCHAR(50),
    birthdate           DATE,
    create_date         DATE,
    dwh_load_date       DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2b. silver.crm_product_data
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_product_data', 'U') IS NOT NULL
    DROP TABLE silver.crm_product_data;
GO

CREATE TABLE silver.crm_product_data (
    product_id          INT             NOT NULL,
    product_number      NVARCHAR(50),
    product_name        NVARCHAR(100),
    cost                DECIMAL(18,2),
    product_line        NVARCHAR(50),
    start_date          DATE,           -- renamed from 'start_data' (bronze typo)
    dwh_load_date       DATETIME2       DEFAULT GETDATE()
);
GO

-- ---------------------------------------------------------------------------
-- 2c. silver.crm_sales_order
-- ---------------------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_order', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_order;
GO

CREATE TABLE silver.crm_sales_order (
    order_number        NVARCHAR(50)    NOT NULL,
    product_id          INT,
    customer_id         INT,
    order_date          DATE,
    quantity            INT,
    price               DECIMAL(18,2),
    shipping_date       DATE,
    due_date            DATE,
    sales_amount        DECIMAL(18,2),
    dwh_load_date       DATETIME2       DEFAULT GETDATE()
);
GO

-- ============================================================================
-- STEP 3: STORED PROCEDURE - ETL Bronze to Silver (CRM)
-- ============================================================================

CREATE OR ALTER PROCEDURE silver.load_silver_crm
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME;
    DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
    SET @batch_start_time = GETDATE();

    BEGIN TRY

        PRINT '============================================='
        PRINT 'LOADING SILVER LAYER - CRM TABLES'
        PRINT '============================================='

        -- ===================================================================
        -- 3a. LOAD silver.crm_customer_info
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.crm_customer_info'
        TRUNCATE TABLE silver.crm_customer_info;

        PRINT '...Inserting Data: silver.crm_customer_info'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY create_date DESC    -- keep most recent record
                ) AS rn
            FROM bronze.crm_customer_info
            WHERE customer_id IS NOT NULL         -- drop records with no PK
        )
        INSERT INTO silver.crm_customer_info (
            customer_id,
            customer_number,
            first_name,
            last_name,
            marital_status,
            gender,
            birthdate,
            create_date
        )
        SELECT
            customer_id,

            -- customer_number: trim whitespace
            LTRIM(RTRIM(customer_number)) AS customer_number,

            -- first_name: trim, proper case, replace blanks with Unknown
            CASE
                WHEN LTRIM(RTRIM(first_name)) IS NULL OR LTRIM(RTRIM(first_name)) = ''
                    THEN 'Unknown'
                ELSE UPPER(LEFT(LTRIM(RTRIM(first_name)), 1))
                     + LOWER(SUBSTRING(LTRIM(RTRIM(first_name)), 2, LEN(LTRIM(RTRIM(first_name)))))
            END AS first_name,

            -- last_name: trim, proper case, replace blanks with Unknown
            CASE
                WHEN LTRIM(RTRIM(last_name)) IS NULL OR LTRIM(RTRIM(last_name)) = ''
                    THEN 'Unknown'
                ELSE UPPER(LEFT(LTRIM(RTRIM(last_name)), 1))
                     + LOWER(SUBSTRING(LTRIM(RTRIM(last_name)), 2, LEN(LTRIM(RTRIM(last_name)))))
            END AS last_name,

            -- marital_status: standardize abbreviations and unknowns
            CASE LTRIM(RTRIM(marital_status))
                WHEN 'M'       THEN 'Married'
                WHEN 'S'       THEN 'Single'
                WHEN 'D'       THEN 'Divorced'
                WHEN 'Married' THEN 'Married'
                WHEN 'Single'  THEN 'Single'
                WHEN 'Divorced'THEN 'Divorced'
                WHEN ''        THEN 'Unknown'
                ELSE COALESCE(LTRIM(RTRIM(marital_status)), 'Unknown')
            END AS marital_status,

            -- gender: standardize n/a and blanks
            CASE
                WHEN LTRIM(RTRIM(gender)) IS NULL OR LTRIM(RTRIM(gender)) = ''
                    THEN 'Unknown'
                WHEN LOWER(LTRIM(RTRIM(gender))) = 'n/a'
                    THEN 'Unknown'
                WHEN LOWER(LTRIM(RTRIM(gender))) = 'male'
                    THEN 'Male'
                WHEN LOWER(LTRIM(RTRIM(gender))) = 'female'
                    THEN 'Female'
                ELSE LTRIM(RTRIM(gender))
            END AS gender,

            -- birthdate: pass through (NULLs remain NULL)
            birthdate,

            -- create_date: pass through (NULLs remain NULL)
            create_date

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3b. LOAD silver.crm_product_data
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.crm_product_data'
        TRUNCATE TABLE silver.crm_product_data;

        PRINT '...Inserting Data: silver.crm_product_data'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY product_id
                    ORDER BY start_data DESC     -- keep most recent record
                ) AS rn
            FROM bronze.crm_product_data
            WHERE product_id IS NOT NULL          -- drop records with no PK
        )
        INSERT INTO silver.crm_product_data (
            product_id,
            product_number,
            product_name,
            cost,
            product_line,
            start_date
        )
        SELECT
            product_id,

            -- product_number: trim
            LTRIM(RTRIM(product_number)) AS product_number,

            -- product_name: trim
            LTRIM(RTRIM(product_name)) AS product_name,

            -- cost: keep NULL as-is (no fabricated defaults); flag zero/negatives as NULL
            CASE
                WHEN cost IS NULL THEN NULL
                WHEN cost <= 0    THEN NULL
                ELSE cost
            END AS cost,

            -- product_line: trim and standardize
            CASE LTRIM(RTRIM(product_line))
                WHEN 'Standard' THEN 'Standard'
                WHEN 'Road'     THEN 'Road'
                WHEN 'Mountain' THEN 'Mountain'
                WHEN 'Touring'  THEN 'Touring'
                WHEN ''         THEN 'Unknown'
                ELSE COALESCE(LTRIM(RTRIM(product_line)), 'Unknown')
            END AS product_line,

            -- start_data -> start_date (rename the column)
            start_data AS start_date

        FROM cte_dedup
        WHERE rn = 1;

        SET @end_time = GETDATE();
        PRINT '   Rows loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR);
        PRINT '   Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '********************'

        -- ===================================================================
        -- 3c. LOAD silver.crm_sales_order
        -- ===================================================================
        SET @start_time = GETDATE();
        PRINT '...Truncating Table: silver.crm_sales_order'
        TRUNCATE TABLE silver.crm_sales_order;

        PRINT '...Inserting Data: silver.crm_sales_order'
        ;WITH cte_dedup AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY order_number
                    ORDER BY order_date DESC      -- keep most recent record
                ) AS rn
            FROM bronze.crm_sales_order
            WHERE order_number IS NOT NULL         -- drop records with no PK
              AND LTRIM(RTRIM(order_number)) != ''
        )
        INSERT INTO silver.crm_sales_order (
            order_number,
            product_id,
            customer_id,
            order_date,
            quantity,
            price,
            shipping_date,
            due_date,
            sales_amount
        )
        SELECT
            -- order_number: trim
            LTRIM(RTRIM(order_number)) AS order_number,

            product_id,

            customer_id,

            -- order_date: pass through
            order_date,

            -- quantity: replace NULL/invalid with NULL
            CASE
                WHEN quantity IS NULL OR quantity <= 0 THEN NULL
                ELSE quantity
            END AS quantity,

            -- price: replace NULL/invalid with NULL
            CASE
                WHEN price IS NULL OR price <= 0 THEN NULL
                ELSE price
            END AS price,

            -- shipping_date: NULL out if before order_date (data error)
            CASE
                WHEN shipping_date < order_date THEN NULL
                ELSE shipping_date
            END AS shipping_date,

            -- due_date: NULL out if before order_date (data error)
            CASE
                WHEN due_date < order_date THEN NULL
                ELSE due_date
            END AS due_date,

            -- sales_amount: recalculate from quantity * price for consistency
            -- If either is invalid, keep original sales_amount as fallback
            CASE
                WHEN quantity IS NOT NULL AND quantity > 0
                 AND price IS NOT NULL AND price > 0
                    THEN CAST(quantity * price AS DECIMAL(18,2))
                ELSE sales_amount
            END AS sales_amount

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
        PRINT 'Silver Layer CRM Loading Complete'
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
