/*
===============================================================================
Script Purpose:
    Data Quality Checks for the Bronze Layer - CRM Source Tables

    This script runs comprehensive data quality checks on:
      1. bronze.crm_customer_info
      2. bronze.crm_product_data
      3. bronze.crm_sales_order

    Categories of checks:
      - Completeness   (NULLs, blanks, missing values)
      - Uniqueness      (duplicate primary/business keys)
      - Validity        (invalid codes, out-of-range values, format issues)
      - Consistency     (cross-field logic, calculated field mismatches)
      - Referential     (orphan foreign keys across CRM tables)
      - Timeliness      (future dates, suspicious date ranges)

Usage:
    Run each section independently or execute the full script.
===============================================================================
*/
USE Zenith_Global_Data_Warehouse;
-- ============================================================================
-- A. DATA QUALITY CHECKS: bronze.crm_customer_info
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.crm_customer_info'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- A1. COMPLETENESS: Check for NULL or empty values in each column
-- ---------------------------------------------------------------------------

-- Check NULL/empty customer_id (primary key - should never be null)
SELECT 'A1a: NULL customer_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE customer_id IS NULL;

-- Check NULL/empty customer_number
SELECT 'A1b: NULL/empty customer_number' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE customer_number IS NULL OR LTRIM(RTRIM(customer_number)) = '';

-- Check NULL/empty first_name
SELECT 'A1c: NULL/empty/blank first_name' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE first_name IS NULL OR LTRIM(RTRIM(first_name)) = '';

-- Check NULL/empty last_name
SELECT 'A1d: NULL/empty last_name' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE last_name IS NULL OR LTRIM(RTRIM(last_name)) = '';

-- Check NULL marital_status
SELECT 'A1e: NULL/empty marital_status' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE marital_status IS NULL OR LTRIM(RTRIM(marital_status)) = '';

-- Check NULL/empty gender
SELECT 'A1f: NULL/empty gender' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE gender IS NULL OR LTRIM(RTRIM(gender)) = '';

-- Check NULL birthdate
SELECT 'A1g: NULL birthdate' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE birthdate IS NULL;

-- Check NULL create_date
SELECT 'A1h: NULL create_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE create_date IS NULL;

-- ---------------------------------------------------------------------------
-- A2. UNIQUENESS: Duplicate primary and business keys
-- ---------------------------------------------------------------------------

-- Duplicate customer_id (should be unique)
SELECT 'A2a: Duplicate customer_id' AS check_name, customer_id, COUNT(*) AS dup_count
FROM bronze.crm_customer_info
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Duplicate customer_number (should be unique)
SELECT 'A2b: Duplicate customer_number' AS check_name, customer_number, COUNT(*) AS dup_count
FROM bronze.crm_customer_info
GROUP BY customer_number
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- A3. VALIDITY: Whitespace, unwanted characters, inconsistent values
-- ---------------------------------------------------------------------------

-- Leading/trailing spaces in first_name
SELECT 'A3a: first_name with leading/trailing spaces' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE first_name != LTRIM(RTRIM(first_name));

-- Leading/trailing spaces in last_name
SELECT 'A3b: last_name with leading/trailing spaces' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE last_name != LTRIM(RTRIM(last_name));

-- Leading/trailing spaces in gender
SELECT 'A3c: gender with leading/trailing spaces' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE gender != LTRIM(RTRIM(gender));

-- Inconsistent casing in first_name (not proper case)
SELECT 'A3d: first_name not in proper case' AS check_name, first_name, COUNT(*) AS cnt
FROM bronze.crm_customer_info
WHERE first_name IS NOT NULL
  AND LTRIM(RTRIM(first_name)) != ''
  AND LTRIM(RTRIM(first_name)) COLLATE Latin1_General_BIN
      != UPPER(LEFT(LTRIM(RTRIM(first_name)),1)) + LOWER(SUBSTRING(LTRIM(RTRIM(first_name)),2,LEN(LTRIM(RTRIM(first_name)))))
GROUP BY first_name;

-- Check all distinct marital_status values (expected: Single, Married, Divorced)
SELECT 'A3e: Distinct marital_status values' AS check_name,
       LTRIM(RTRIM(marital_status)) AS marital_status_trimmed,
       COUNT(*) AS cnt
FROM bronze.crm_customer_info
GROUP BY LTRIM(RTRIM(marital_status));

-- Invalid/abbreviated marital_status (not in expected full set)
SELECT 'A3f: Invalid marital_status codes' AS check_name, marital_status, COUNT(*) AS cnt
FROM bronze.crm_customer_info
WHERE LTRIM(RTRIM(marital_status)) NOT IN ('Single', 'Married', 'Divorced')
  AND marital_status IS NOT NULL
  AND LTRIM(RTRIM(marital_status)) != ''
GROUP BY marital_status;

-- Check all distinct gender values (expected: Male, Female)
SELECT 'A3g: Distinct gender values' AS check_name,
       LTRIM(RTRIM(gender)) AS gender_trimmed,
       COUNT(*) AS cnt
FROM bronze.crm_customer_info
GROUP BY LTRIM(RTRIM(gender));

-- Invalid gender values
SELECT 'A3h: Invalid gender values' AS check_name, gender, COUNT(*) AS cnt
FROM bronze.crm_customer_info
WHERE LTRIM(RTRIM(gender)) NOT IN ('Male', 'Female')
  AND gender IS NOT NULL
  AND LTRIM(RTRIM(gender)) != ''
GROUP BY gender;

-- customer_number format check (expected pattern: CUSTnnnnn)
SELECT 'A3i: customer_number not matching CUST##### pattern' AS check_name,
       customer_number, COUNT(*) AS cnt
FROM bronze.crm_customer_info
WHERE customer_number NOT LIKE 'CUST[0-9][0-9][0-9][0-9][0-9]'
GROUP BY customer_number;

-- ---------------------------------------------------------------------------
-- A4. TIMELINESS / RANGE: Date validations
-- ---------------------------------------------------------------------------

-- Birthdate in the future
SELECT 'A4a: birthdate in the future' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE birthdate > GETDATE();

-- Unreasonably old birthdate (before 1920)
SELECT 'A4b: birthdate before 1920' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE birthdate < '1920-01-01';

-- Suspiciously young customers (birthdate after 2010 = under ~16)
SELECT 'A4c: birthdate after 2010 (very young)' AS check_name,
       customer_id, birthdate
FROM bronze.crm_customer_info
WHERE birthdate > '2010-01-01';

-- create_date in the future
SELECT 'A4d: create_date in the future' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE create_date > GETDATE();

-- create_date before birthdate (account created before person was born)
SELECT 'A4e: create_date before birthdate' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_customer_info
WHERE create_date IS NOT NULL
  AND birthdate IS NOT NULL
  AND create_date < birthdate;

-- ---------------------------------------------------------------------------
-- A5. CONSISTENCY: Cross-field checks
-- ---------------------------------------------------------------------------

-- customer_id should match the numeric portion of customer_number
SELECT 'A5a: customer_id does not match customer_number suffix' AS check_name,
       customer_id, customer_number
FROM bronze.crm_customer_info
WHERE customer_number IS NOT NULL
  AND TRY_CAST(REPLACE(customer_number, 'CUST', '') AS INT) != customer_id;


-- ============================================================================
-- B. DATA QUALITY CHECKS: bronze.crm_product_data
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.crm_product_data'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- B1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL product_id
SELECT 'B1a: NULL product_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_id IS NULL;

-- NULL/empty product_number
SELECT 'B1b: NULL/empty product_number' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_number IS NULL OR LTRIM(RTRIM(product_number)) = '';

-- NULL/empty product_name
SELECT 'B1c: NULL/empty product_name' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_name IS NULL OR LTRIM(RTRIM(product_name)) = '';

-- NULL cost (missing pricing data)
SELECT 'B1d: NULL cost' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE cost IS NULL;

-- List products with NULL cost
SELECT 'B1d_detail: Products with NULL cost' AS check_name,
       product_id, product_number, product_name
FROM bronze.crm_product_data
WHERE cost IS NULL;

-- NULL/empty product_line
SELECT 'B1e: NULL/empty product_line' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_line IS NULL OR LTRIM(RTRIM(product_line)) = '';

-- NULL start_data (note: column name typo in DDL)
SELECT 'B1f: NULL start_data' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE start_data IS NULL;

-- ---------------------------------------------------------------------------
-- B2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate product_id
SELECT 'B2a: Duplicate product_id' AS check_name, product_id, COUNT(*) AS dup_count
FROM bronze.crm_product_data
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Duplicate product_number
SELECT 'B2b: Duplicate product_number' AS check_name, product_number, COUNT(*) AS dup_count
FROM bronze.crm_product_data
GROUP BY product_number
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- B3. VALIDITY: Value range and format checks
-- ---------------------------------------------------------------------------

-- Negative or zero cost
SELECT 'B3a: Negative or zero cost' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE cost IS NOT NULL AND cost <= 0;

-- Extremely high cost outliers (> 5000)
SELECT 'B3b: Cost exceeds 5000 (outlier check)' AS check_name,
       product_id, product_name, cost
FROM bronze.crm_product_data
WHERE cost > 5000;

-- Check all distinct product_line values
SELECT 'B3c: Distinct product_line values' AS check_name,
       LTRIM(RTRIM(product_line)) AS product_line_trimmed,
       COUNT(*) AS cnt
FROM bronze.crm_product_data
GROUP BY LTRIM(RTRIM(product_line));

-- Invalid product_line (expected: Standard, Road, Mountain, Touring)
SELECT 'B3d: Invalid product_line' AS check_name, product_line, COUNT(*) AS cnt
FROM bronze.crm_product_data
WHERE LTRIM(RTRIM(product_line)) NOT IN ('Standard', 'Road', 'Mountain', 'Touring')
  AND product_line IS NOT NULL
  AND LTRIM(RTRIM(product_line)) != ''
GROUP BY product_line;

-- Leading/trailing spaces in product_name
SELECT 'B3e: product_name with leading/trailing spaces' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_name != LTRIM(RTRIM(product_name));

-- Leading/trailing spaces in product_line
SELECT 'B3f: product_line with leading/trailing spaces' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE product_line != LTRIM(RTRIM(product_line));

-- Product number format check (expected: XX-XX-### pattern)
SELECT 'B3g: product_number not matching XX-XX-### pattern' AS check_name,
       product_number
FROM bronze.crm_product_data
WHERE product_number NOT LIKE '[A-Z][A-Z]-[A-Z][A-Z]-[0-9][0-9][0-9]';

-- ---------------------------------------------------------------------------
-- B4. TIMELINESS: Date checks
-- ---------------------------------------------------------------------------

-- start_data in the future
SELECT 'B4a: start_data in the future' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE start_data > GETDATE();

-- start_data unreasonably old (before 2000)
SELECT 'B4b: start_data before 2000' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_product_data
WHERE start_data < '2000-01-01';

-- ---------------------------------------------------------------------------
-- B5. CONSISTENCY: Product number prefix vs product_line alignment
-- ---------------------------------------------------------------------------

-- BK prefix should generally map to bike product lines (Road, Mountain, Touring)
SELECT 'B5a: BK-prefix products with Standard product_line' AS check_name,
       product_id, product_number, product_name, product_line
FROM bronze.crm_product_data
WHERE product_number LIKE 'BK-%'
  AND LTRIM(RTRIM(product_line)) = 'Standard';

-- Non-BK prefix products in bike lines (Road, Mountain, Touring)
SELECT 'B5b: Non-bike prefix in bike product_line' AS check_name,
       product_id, product_number, product_name, product_line
FROM bronze.crm_product_data
WHERE product_number NOT LIKE 'BK-%'
  AND LTRIM(RTRIM(product_line)) IN ('Road', 'Mountain', 'Touring');


-- ============================================================================
-- C. DATA QUALITY CHECKS: bronze.crm_sales_order
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.crm_sales_order'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- C1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL/empty order_number
SELECT 'C1a: NULL/empty order_number' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE order_number IS NULL OR LTRIM(RTRIM(order_number)) = '';

-- NULL product_id
SELECT 'C1b: NULL product_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE product_id IS NULL;

-- NULL customer_id
SELECT 'C1c: NULL customer_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE customer_id IS NULL;

-- NULL order_date
SELECT 'C1d: NULL order_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE order_date IS NULL;

-- NULL quantity
SELECT 'C1e: NULL quantity' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE quantity IS NULL;

-- NULL price
SELECT 'C1f: NULL price' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE price IS NULL;

-- NULL shipping_date
SELECT 'C1g: NULL shipping_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE shipping_date IS NULL;

-- NULL due_date
SELECT 'C1h: NULL due_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE due_date IS NULL;

-- NULL sales_amount
SELECT 'C1i: NULL sales_amount' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE sales_amount IS NULL;

-- ---------------------------------------------------------------------------
-- C2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate order_number (should be unique per order)
SELECT 'C2a: Duplicate order_number' AS check_name, order_number, COUNT(*) AS dup_count
FROM bronze.crm_sales_order
GROUP BY order_number
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- C3. VALIDITY: Value range and format checks
-- ---------------------------------------------------------------------------

-- Negative or zero quantity
SELECT 'C3a: Quantity <= 0' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE quantity <= 0;

-- Negative or zero price
SELECT 'C3b: Price <= 0' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE price <= 0;

-- Negative sales_amount
SELECT 'C3c: Negative sales_amount' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE sales_amount < 0;

-- order_number format check (expected: SO followed by digits)
SELECT 'C3d: order_number not matching SO##### pattern' AS check_name,
       order_number
FROM bronze.crm_sales_order
WHERE order_number NOT LIKE 'SO[0-9][0-9][0-9][0-9][0-9]';

-- ---------------------------------------------------------------------------
-- C4. CONSISTENCY: Calculated field and cross-field logic
-- ---------------------------------------------------------------------------

-- sales_amount != quantity * price (mismatch in calculated field)
SELECT 'C4a: sales_amount != quantity * price' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE sales_amount != (quantity * price);

-- Detail of mismatches
SELECT 'C4a_detail: sales_amount mismatches' AS check_name,
       order_number, quantity, price, sales_amount,
       (quantity * price) AS expected_sales_amount,
       sales_amount - (quantity * price) AS difference
FROM bronze.crm_sales_order
WHERE sales_amount != (quantity * price);

-- ---------------------------------------------------------------------------
-- C5. TIMELINESS: Date logic and range checks
-- ---------------------------------------------------------------------------

-- order_date in the future
SELECT 'C5a: order_date in the future' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE order_date > GETDATE();

-- shipping_date before order_date (shipped before ordered)
SELECT 'C5b: shipping_date < order_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE shipping_date < order_date;

-- due_date before order_date
SELECT 'C5c: due_date < order_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE due_date < order_date;

-- shipping_date after due_date (shipped late)
SELECT 'C5d: shipping_date > due_date (late shipment)' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE shipping_date > due_date;

-- Unreasonably old order_date (before 2020)
SELECT 'C5e: order_date before 2020' AS check_name, COUNT(*) AS issue_count
FROM bronze.crm_sales_order
WHERE order_date < '2020-01-01';

-- Check date range distribution
SELECT 'C5f: Order date range' AS check_name,
       MIN(order_date) AS earliest_order,
       MAX(order_date) AS latest_order,
       MIN(shipping_date) AS earliest_ship,
       MAX(shipping_date) AS latest_ship
FROM bronze.crm_sales_order;

-- ---------------------------------------------------------------------------
-- C6. REFERENTIAL INTEGRITY: Orphan foreign keys
-- ---------------------------------------------------------------------------

-- Orders with product_id not found in crm_product_data
SELECT 'C6a: Orphan product_id (not in crm_product_data)' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.crm_sales_order so
LEFT JOIN bronze.crm_product_data pd ON so.product_id = pd.product_id
WHERE pd.product_id IS NULL;

-- Orders with customer_id not found in crm_customer_info
SELECT 'C6b: Orphan customer_id (not in crm_customer_info)' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.crm_sales_order so
LEFT JOIN bronze.crm_customer_info ci ON so.customer_id = ci.customer_id
WHERE ci.customer_id IS NULL;


-- ============================================================================
-- D. SUMMARY: Row counts for all three bronze CRM tables
-- ============================================================================

SELECT 'D1: Row counts' AS check_name,
       (SELECT COUNT(*) FROM bronze.crm_customer_info)   AS customer_info_rows,
       (SELECT COUNT(*) FROM bronze.crm_product_data)    AS product_data_rows,
       (SELECT COUNT(*) FROM bronze.crm_sales_order)     AS sales_order_rows;

