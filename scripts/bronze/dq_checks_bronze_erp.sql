/*
===============================================================================
Script Purpose:
    Data Quality Checks for the Bronze Layer - ERP Source Tables

    This script runs comprehensive data quality checks on:
      1. bronze.erp_category_map
      2. bronze.erp_product_specs
      3. bronze.erp_stores
      4. bronze.erp_returns
      5. bronze.erp_spatial_data
      6. bronze.erp_territory

    Categories of checks:
      - Completeness   (NULLs, blanks, missing values)
      - Uniqueness      (duplicate primary/business keys)
      - Validity        (invalid codes, out-of-range values, format issues)
      - Consistency     (cross-field logic, cross-table mismatches)
      - Referential     (orphan foreign keys across ERP and CRM tables)
      - Timeliness      (future dates, suspicious date ranges)

Usage:
    Run each section independently or execute the full script.
===============================================================================
*/

-- ============================================================================
-- E. DATA QUALITY CHECKS: bronze.erp_category_map
-- ============================================================================
USE Zenith_Global_Data_Warehouse;
PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_category_map'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- E1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL/empty category_id (primary key)
SELECT 'E1a: NULL/empty category_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_category_map
WHERE category_id IS NULL OR LTRIM(RTRIM(category_id)) = '';

-- NULL/empty category
SELECT 'E1b: NULL/empty category' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_category_map
WHERE category IS NULL OR LTRIM(RTRIM(category)) = '';

-- NULL/empty subcategory
SELECT 'E1c: NULL/empty subcategory' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_category_map
WHERE subcategory IS NULL OR LTRIM(RTRIM(subcategory)) = '';

-- ---------------------------------------------------------------------------
-- E2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate category_id
SELECT 'E2a: Duplicate category_id' AS check_name, category_id, COUNT(*) AS dup_count
FROM bronze.erp_category_map
GROUP BY category_id
HAVING COUNT(*) > 1;

-- Duplicate category + subcategory combination
SELECT 'E2b: Duplicate category+subcategory' AS check_name,
       category, subcategory, COUNT(*) AS dup_count
FROM bronze.erp_category_map
GROUP BY category, subcategory
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- E3. VALIDITY: Value and format checks
-- ---------------------------------------------------------------------------

-- Check all distinct category values
SELECT 'E3a: Distinct category values' AS check_name,
       LTRIM(RTRIM(category)) AS category_trimmed, COUNT(*) AS cnt
FROM bronze.erp_category_map
GROUP BY LTRIM(RTRIM(category));

-- Check all distinct subcategory values
SELECT 'E3b: Distinct subcategory values' AS check_name,
       LTRIM(RTRIM(subcategory)) AS subcategory_trimmed, COUNT(*) AS cnt
FROM bronze.erp_category_map
GROUP BY LTRIM(RTRIM(subcategory));

-- Leading/trailing whitespace in any column
SELECT 'E3c: Whitespace issues in category_map' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_category_map
WHERE category    != LTRIM(RTRIM(category))
   OR subcategory != LTRIM(RTRIM(subcategory))
   OR category_id != LTRIM(RTRIM(category_id));

-- category_id format check (expected pattern: XX_XX e.g., AC_BO)
SELECT 'E3d: category_id not matching XX_XX pattern' AS check_name, category_id
FROM bronze.erp_category_map
WHERE category_id NOT LIKE '[A-Z][A-Z]_[A-Z][A-Z]';

-- Verify category_id prefix aligns with category name
-- AC=Accessories, BI=Bikes, CL=Clothing, CO=Components
SELECT 'E3e: category_id prefix does not match category' AS check_name,
       category_id, category
FROM bronze.erp_category_map
WHERE (LEFT(category_id, 2) = 'AC' AND LTRIM(RTRIM(category)) != 'Accessories')
   OR (LEFT(category_id, 2) = 'BI' AND LTRIM(RTRIM(category)) != 'Bikes')
   OR (LEFT(category_id, 2) = 'CL' AND LTRIM(RTRIM(category)) != 'Clothing')
   OR (LEFT(category_id, 2) = 'CO' AND LTRIM(RTRIM(category)) != 'Components');


-- ============================================================================
-- F. DATA QUALITY CHECKS: bronze.erp_product_specs
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_product_specs'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- F1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL product_id
SELECT 'F1a: NULL product_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_product_specs
WHERE product_id IS NULL;

-- NULL/empty subcategory
SELECT 'F1b: NULL/empty subcategory' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_product_specs
WHERE subcategory IS NULL OR LTRIM(RTRIM(subcategory)) = '';

-- NULL/empty maintenance_required
SELECT 'F1c: NULL/empty maintenance_required' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_product_specs
WHERE maintenance_required IS NULL OR LTRIM(RTRIM(maintenance_required)) = '';

-- NULL/empty product_line
SELECT 'F1d: NULL/empty product_line' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_product_specs
WHERE product_line IS NULL OR LTRIM(RTRIM(product_line)) = '';

-- ---------------------------------------------------------------------------
-- F2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate product_id
SELECT 'F2a: Duplicate product_id' AS check_name, product_id, COUNT(*) AS dup_count
FROM bronze.erp_product_specs
GROUP BY product_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- F3. VALIDITY: Value checks
-- ---------------------------------------------------------------------------

-- Distinct subcategory values
SELECT 'F3a: Distinct subcategory values' AS check_name,
       LTRIM(RTRIM(subcategory)) AS subcategory_trimmed, COUNT(*) AS cnt
FROM bronze.erp_product_specs
GROUP BY LTRIM(RTRIM(subcategory));

-- Distinct maintenance_required values (expected: Yes, No)
SELECT 'F3b: Distinct maintenance_required values' AS check_name,
       LTRIM(RTRIM(maintenance_required)) AS maint_trimmed, COUNT(*) AS cnt
FROM bronze.erp_product_specs
GROUP BY LTRIM(RTRIM(maintenance_required));

-- Invalid maintenance_required values
SELECT 'F3c: Invalid maintenance_required values' AS check_name,
       maintenance_required, COUNT(*) AS cnt
FROM bronze.erp_product_specs
WHERE LTRIM(RTRIM(maintenance_required)) NOT IN ('Yes', 'No')
  AND maintenance_required IS NOT NULL
  AND LTRIM(RTRIM(maintenance_required)) != ''
GROUP BY maintenance_required;

-- Distinct product_line values (expected: Standard, Road, Mountain, Touring)
SELECT 'F3d: Distinct product_line values' AS check_name,
       LTRIM(RTRIM(product_line)) AS product_line_trimmed, COUNT(*) AS cnt
FROM bronze.erp_product_specs
GROUP BY LTRIM(RTRIM(product_line));

-- Invalid product_line
SELECT 'F3e: Invalid product_line' AS check_name, product_line, COUNT(*) AS cnt
FROM bronze.erp_product_specs
WHERE LTRIM(RTRIM(product_line)) NOT IN ('Standard', 'Road', 'Mountain', 'Touring')
  AND product_line IS NOT NULL
  AND LTRIM(RTRIM(product_line)) != ''
GROUP BY product_line;

-- Leading/trailing whitespace
SELECT 'F3f: Whitespace issues in product_specs' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_product_specs
WHERE subcategory          != LTRIM(RTRIM(subcategory))
   OR maintenance_required != LTRIM(RTRIM(maintenance_required))
   OR product_line         != LTRIM(RTRIM(product_line));

-- ---------------------------------------------------------------------------
-- F4. CONSISTENCY: Subcategory naming vs category_map
-- ---------------------------------------------------------------------------

-- Subcategory values in product_specs use PLURAL names (e.g. "Tires", "Helmets")
-- while category_map uses SINGULAR (e.g. "Tire", "Helmet") — flag mismatches
SELECT 'F4a: Subcategory not found in category_map (exact match)' AS check_name,
       ps.subcategory, COUNT(*) AS cnt
FROM bronze.erp_product_specs ps
LEFT JOIN bronze.erp_category_map cm
    ON LTRIM(RTRIM(ps.subcategory)) = LTRIM(RTRIM(cm.subcategory))
WHERE cm.subcategory IS NULL
GROUP BY ps.subcategory;

-- ---------------------------------------------------------------------------
-- F5. REFERENTIAL INTEGRITY
-- ---------------------------------------------------------------------------

-- product_id not found in crm_product_data
SELECT 'F5a: Orphan product_id (not in crm_product_data)' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_product_specs ps
LEFT JOIN bronze.crm_product_data pd ON ps.product_id = pd.product_id
WHERE pd.product_id IS NULL;

-- product_line conflict between product_specs and crm_product_data
SELECT 'F5b: product_line mismatch between product_specs and product_data' AS check_name,
       ps.product_id,
       ps.product_line  AS specs_product_line,
       pd.product_line  AS crm_product_line
FROM bronze.erp_product_specs ps
INNER JOIN bronze.crm_product_data pd ON ps.product_id = pd.product_id
WHERE LTRIM(RTRIM(ps.product_line)) != LTRIM(RTRIM(pd.product_line));


-- ============================================================================
-- G. DATA QUALITY CHECKS: bronze.erp_stores
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_stores'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- G1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL store_id
SELECT 'G1a: NULL store_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE store_id IS NULL;

-- NULL/empty store_name
SELECT 'G1b: NULL/empty store_name' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE store_name IS NULL OR LTRIM(RTRIM(store_name)) = '';

-- NULL/empty store_type
SELECT 'G1c: NULL/empty store_type' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE store_type IS NULL OR LTRIM(RTRIM(store_type)) = '';

-- NULL/empty region
SELECT 'G1d: NULL/empty region' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE region IS NULL OR LTRIM(RTRIM(region)) = '';

-- ---------------------------------------------------------------------------
-- G2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate store_id
SELECT 'G2a: Duplicate store_id' AS check_name, store_id, COUNT(*) AS dup_count
FROM bronze.erp_stores
GROUP BY store_id
HAVING COUNT(*) > 1;

-- Duplicate store_name (different store_ids, same name)
SELECT 'G2b: Duplicate store_name' AS check_name,
       store_name, COUNT(*) AS dup_count
FROM bronze.erp_stores
GROUP BY store_name
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- G3. VALIDITY: Value checks
-- ---------------------------------------------------------------------------

-- Distinct store_type values (expected: Flagship, Retail, Online, Outlet)
SELECT 'G3a: Distinct store_type values' AS check_name,
       LTRIM(RTRIM(store_type)) AS store_type_trimmed, COUNT(*) AS cnt
FROM bronze.erp_stores
GROUP BY LTRIM(RTRIM(store_type));

-- Invalid store_type
SELECT 'G3b: Invalid store_type' AS check_name, store_type, COUNT(*) AS cnt
FROM bronze.erp_stores
WHERE LTRIM(RTRIM(store_type)) NOT IN ('Flagship', 'Retail', 'Online', 'Outlet')
  AND store_type IS NOT NULL
  AND LTRIM(RTRIM(store_type)) != ''
GROUP BY store_type;

-- Distinct region values
SELECT 'G3c: Distinct region values' AS check_name,
       LTRIM(RTRIM(region)) AS region_trimmed, COUNT(*) AS cnt
FROM bronze.erp_stores
GROUP BY LTRIM(RTRIM(region));

-- Invalid region (expected: Asia-Pacific, Latin America, Europe, North America)
SELECT 'G3d: Invalid region' AS check_name, region, COUNT(*) AS cnt
FROM bronze.erp_stores
WHERE LTRIM(RTRIM(region)) NOT IN ('Asia-Pacific', 'Latin America', 'Europe', 'North America')
  AND region IS NOT NULL
  AND LTRIM(RTRIM(region)) != ''
GROUP BY region;

-- Leading/trailing whitespace
SELECT 'G3e: Whitespace issues in erp_stores' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE store_name != LTRIM(RTRIM(store_name))
   OR store_type != LTRIM(RTRIM(store_type))
   OR region     != LTRIM(RTRIM(region));

-- Negative or zero store_id
SELECT 'G3f: store_id <= 0' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_stores
WHERE store_id <= 0;


-- ============================================================================
-- H. DATA QUALITY CHECKS: bronze.erp_returns
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_returns'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- H1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL return_id (primary key)
SELECT 'H1a: NULL return_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_id IS NULL;

-- NULL/empty order_number
SELECT 'H1b: NULL/empty order_number' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE order_number IS NULL OR LTRIM(RTRIM(order_number)) = '';

-- NULL return_date
SELECT 'H1c: NULL return_date' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_date IS NULL;

-- NULL/empty return_reason
SELECT 'H1d: NULL/empty return_reason' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_reason IS NULL OR LTRIM(RTRIM(return_reason)) = '';

-- NULL return_amount
SELECT 'H1e: NULL return_amount' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_amount IS NULL;

-- ---------------------------------------------------------------------------
-- H2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate return_id
SELECT 'H2a: Duplicate return_id' AS check_name, return_id, COUNT(*) AS dup_count
FROM bronze.erp_returns
GROUP BY return_id
HAVING COUNT(*) > 1;

-- Multiple returns for the same order_number
SELECT 'H2b: Multiple returns per order_number' AS check_name,
       order_number, COUNT(*) AS return_count
FROM bronze.erp_returns
GROUP BY order_number
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- H3. VALIDITY: Value and format checks
-- ---------------------------------------------------------------------------

-- Negative return_amount (should be positive for a return)
SELECT 'H3a: Negative return_amount' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_amount < 0;

-- Detail of negative return_amounts
SELECT 'H3a_detail: Negative return_amount records' AS check_name,
       return_id, order_number, return_amount
FROM bronze.erp_returns
WHERE return_amount < 0;

-- Zero return_amount
SELECT 'H3b: Zero return_amount' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_amount = 0;

-- Distinct return_reason values (expected: Wrong Item, Size Mismatch, Unsatisfied, Defective)
SELECT 'H3c: Distinct return_reason values' AS check_name,
       LTRIM(RTRIM(return_reason)) AS reason_trimmed, COUNT(*) AS cnt
FROM bronze.erp_returns
GROUP BY LTRIM(RTRIM(return_reason));

-- Invalid return_reason
SELECT 'H3d: Invalid return_reason' AS check_name, return_reason, COUNT(*) AS cnt
FROM bronze.erp_returns
WHERE LTRIM(RTRIM(return_reason)) NOT IN ('Wrong Item', 'Size Mismatch', 'Unsatisfied', 'Defective')
  AND return_reason IS NOT NULL
  AND LTRIM(RTRIM(return_reason)) != ''
GROUP BY return_reason;

-- order_number format check (expected: SO followed by digits)
SELECT 'H3e: order_number not matching SO##### pattern' AS check_name, order_number
FROM bronze.erp_returns
WHERE order_number NOT LIKE 'SO[0-9][0-9][0-9][0-9][0-9]';

-- Leading/trailing whitespace
SELECT 'H3f: Whitespace issues in erp_returns' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE order_number  != LTRIM(RTRIM(order_number))
   OR return_reason != LTRIM(RTRIM(return_reason));

-- ---------------------------------------------------------------------------
-- H4. TIMELINESS: Date checks
-- ---------------------------------------------------------------------------

-- return_date in the future
SELECT 'H4a: return_date in the future' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_date > GETDATE();

-- return_date before 2020 (unreasonably old)
SELECT 'H4b: return_date before 2020' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_returns
WHERE return_date < '2020-01-01';

-- Check return_date range
SELECT 'H4c: Return date range' AS check_name,
       MIN(return_date) AS earliest_return,
       MAX(return_date) AS latest_return
FROM bronze.erp_returns;

-- ---------------------------------------------------------------------------
-- H5. REFERENTIAL INTEGRITY
-- ---------------------------------------------------------------------------

-- return order_number not found in crm_sales_order
SELECT 'H5a: Orphan order_number (not in crm_sales_order)' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_returns r
LEFT JOIN bronze.crm_sales_order so ON LTRIM(RTRIM(r.order_number)) = LTRIM(RTRIM(so.order_number))
WHERE so.order_number IS NULL;

-- ---------------------------------------------------------------------------
-- H6. CONSISTENCY: Return amount vs original sale
-- ---------------------------------------------------------------------------

-- return_amount exceeds original sales_amount
SELECT 'H6a: return_amount > original sales_amount' AS check_name,
       r.return_id, r.order_number, r.return_amount, so.sales_amount
FROM bronze.erp_returns r
INNER JOIN bronze.crm_sales_order so
    ON LTRIM(RTRIM(r.order_number)) = LTRIM(RTRIM(so.order_number))
WHERE r.return_amount > so.sales_amount;

-- return_date before order_date
SELECT 'H6b: return_date before order_date' AS check_name,
       r.return_id, r.order_number, r.return_date, so.order_date
FROM bronze.erp_returns r
INNER JOIN bronze.crm_sales_order so
    ON LTRIM(RTRIM(r.order_number)) = LTRIM(RTRIM(so.order_number))
WHERE r.return_date < so.order_date;


-- ============================================================================
-- I. DATA QUALITY CHECKS: bronze.erp_spatial_data
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_spatial_data'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- I1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL customer_id
SELECT 'I1a: NULL customer_id' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_spatial_data
WHERE customer_id IS NULL;

-- NULL/empty country
SELECT 'I1b: NULL/empty country' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_spatial_data
WHERE country IS NULL OR LTRIM(RTRIM(country)) = '';

-- NULL/empty city
SELECT 'I1c: NULL/empty city' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_spatial_data
WHERE city IS NULL OR LTRIM(RTRIM(city)) = '';

-- ---------------------------------------------------------------------------
-- I2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate customer_id (each customer should have one location)
SELECT 'I2a: Duplicate customer_id' AS check_name, customer_id, COUNT(*) AS dup_count
FROM bronze.erp_spatial_data
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- I3. VALIDITY: Value checks
-- ---------------------------------------------------------------------------

-- Distinct country values
SELECT 'I3a: Distinct country values' AS check_name,
       LTRIM(RTRIM(country)) AS country_trimmed, COUNT(*) AS cnt
FROM bronze.erp_spatial_data
GROUP BY LTRIM(RTRIM(country));

-- Distinct city values
SELECT 'I3b: Distinct city values' AS check_name,
       LTRIM(RTRIM(city)) AS city_trimmed, COUNT(*) AS cnt
FROM bronze.erp_spatial_data
GROUP BY LTRIM(RTRIM(city));

-- Leading/trailing whitespace
SELECT 'I3c: Whitespace issues in erp_spatial_data' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_spatial_data
WHERE country != LTRIM(RTRIM(country))
   OR city    != LTRIM(RTRIM(city));

-- ---------------------------------------------------------------------------
-- I4. CONSISTENCY: City-Country alignment vs territory reference
-- ---------------------------------------------------------------------------

-- Country-City pair in spatial_data does NOT match erp_territory lookup
-- (e.g., customer says "Germany, Sydney" but territory says Sydney is in Australia)
SELECT 'I4a: City-Country mismatch vs territory reference' AS check_name,
       sd.customer_id, sd.country AS spatial_country, sd.city AS spatial_city,
       t.country AS territory_country
FROM bronze.erp_spatial_data sd
LEFT JOIN bronze.erp_territory t
    ON LTRIM(RTRIM(sd.city)) = LTRIM(RTRIM(t.city))
WHERE t.city IS NOT NULL
  AND LTRIM(RTRIM(sd.country)) != LTRIM(RTRIM(t.country));

-- City in spatial_data not found in territory reference at all
SELECT 'I4b: City not found in territory reference' AS check_name,
       sd.city, COUNT(*) AS cnt
FROM bronze.erp_spatial_data sd
LEFT JOIN bronze.erp_territory t
    ON LTRIM(RTRIM(sd.city)) = LTRIM(RTRIM(t.city))
WHERE t.city IS NULL
GROUP BY sd.city;

-- Country in spatial_data not found in territory reference
SELECT 'I4c: Country not found in territory reference' AS check_name,
       sd.country, COUNT(*) AS cnt
FROM bronze.erp_spatial_data sd
LEFT JOIN bronze.erp_territory t
    ON LTRIM(RTRIM(sd.country)) = LTRIM(RTRIM(t.country))
WHERE t.country IS NULL
GROUP BY sd.country;

-- ---------------------------------------------------------------------------
-- I5. REFERENTIAL INTEGRITY
-- ---------------------------------------------------------------------------

-- customer_id not found in crm_customer_info
SELECT 'I5a: Orphan customer_id (not in crm_customer_info)' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_spatial_data sd
LEFT JOIN bronze.crm_customer_info ci ON sd.customer_id = ci.customer_id
WHERE ci.customer_id IS NULL;

-- Customers in crm_customer_info with no spatial data
SELECT 'I5b: Customers missing spatial data' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.crm_customer_info ci
LEFT JOIN bronze.erp_spatial_data sd ON ci.customer_id = sd.customer_id
WHERE sd.customer_id IS NULL;


-- ============================================================================
-- J. DATA QUALITY CHECKS: bronze.erp_territory
-- ============================================================================

PRINT '============================================='
PRINT 'DQ CHECKS: bronze.erp_territory'
PRINT '============================================='

-- ---------------------------------------------------------------------------
-- J1. COMPLETENESS: NULL / empty checks
-- ---------------------------------------------------------------------------

-- NULL/empty city
SELECT 'J1a: NULL/empty city' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_territory
WHERE city IS NULL OR LTRIM(RTRIM(city)) = '';

-- NULL/empty country
SELECT 'J1b: NULL/empty country' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_territory
WHERE country IS NULL OR LTRIM(RTRIM(country)) = '';

-- NULL/empty continent
SELECT 'J1c: NULL/empty continent' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_territory
WHERE continent IS NULL OR LTRIM(RTRIM(continent)) = '';

-- ---------------------------------------------------------------------------
-- J2. UNIQUENESS: Duplicate keys
-- ---------------------------------------------------------------------------

-- Duplicate city (each city should appear once in the reference)
SELECT 'J2a: Duplicate city' AS check_name, city, COUNT(*) AS dup_count
FROM bronze.erp_territory
GROUP BY city
HAVING COUNT(*) > 1;

-- Duplicate city+country combination
SELECT 'J2b: Duplicate city+country' AS check_name, city, country, COUNT(*) AS dup_count
FROM bronze.erp_territory
GROUP BY city, country
HAVING COUNT(*) > 1;

-- ---------------------------------------------------------------------------
-- J3. VALIDITY: Value checks
-- ---------------------------------------------------------------------------

-- Distinct continent values
SELECT 'J3a: Distinct continent values' AS check_name,
       LTRIM(RTRIM(continent)) AS continent_trimmed, COUNT(*) AS cnt
FROM bronze.erp_territory
GROUP BY LTRIM(RTRIM(continent));

-- Distinct country values
SELECT 'J3b: Distinct country values' AS check_name,
       LTRIM(RTRIM(country)) AS country_trimmed, COUNT(*) AS cnt
FROM bronze.erp_territory
GROUP BY LTRIM(RTRIM(country));

-- Leading/trailing whitespace
SELECT 'J3c: Whitespace issues in erp_territory' AS check_name, COUNT(*) AS issue_count
FROM bronze.erp_territory
WHERE city      != LTRIM(RTRIM(city))
   OR country   != LTRIM(RTRIM(country))
   OR continent != LTRIM(RTRIM(continent));

-- ---------------------------------------------------------------------------
-- J4. CONSISTENCY: Country-Continent alignment
-- ---------------------------------------------------------------------------

-- Cross-check: countries belong to expected continents
SELECT 'J4a: Possible country-continent mismatch' AS check_name,
       country, continent, COUNT(*) AS cnt
FROM bronze.erp_territory
GROUP BY country, continent
ORDER BY country;
-- (Manual review: verify Australia=Oceania, USA=North America, etc.)

-- Same country mapped to different continents
SELECT 'J4b: Country mapped to multiple continents' AS check_name,
       country, COUNT(DISTINCT continent) AS continent_count
FROM bronze.erp_territory
GROUP BY country
HAVING COUNT(DISTINCT continent) > 1;


-- ============================================================================
-- K. CROSS-SOURCE REFERENTIAL INTEGRITY SUMMARY
-- ============================================================================

PRINT '============================================='
PRINT 'CROSS-SOURCE REFERENTIAL INTEGRITY'
PRINT '============================================='

-- Products in crm_product_data without matching erp_product_specs
SELECT 'K1: Products in CRM without ERP specs' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.crm_product_data pd
LEFT JOIN bronze.erp_product_specs ps ON pd.product_id = ps.product_id
WHERE ps.product_id IS NULL;

-- Products in erp_product_specs without matching crm_product_data
SELECT 'K2: Products in ERP specs without CRM product' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_product_specs ps
LEFT JOIN bronze.crm_product_data pd ON ps.product_id = pd.product_id
WHERE pd.product_id IS NULL;

-- Sales orders referencing products that have no ERP specs
SELECT 'K3: Sales orders with products missing ERP specs' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.crm_sales_order so
LEFT JOIN bronze.erp_product_specs ps ON so.product_id = ps.product_id
WHERE ps.product_id IS NULL;

-- Returns referencing orders that do not exist in sales
SELECT 'K4: Returns with orphan order_numbers' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_returns r
LEFT JOIN bronze.crm_sales_order so
    ON LTRIM(RTRIM(r.order_number)) = LTRIM(RTRIM(so.order_number))
WHERE so.order_number IS NULL;

-- Customers in spatial_data not in customer_info
SELECT 'K5: Spatial data customers not in CRM' AS check_name,
       COUNT(*) AS issue_count
FROM bronze.erp_spatial_data sd
LEFT JOIN bronze.crm_customer_info ci ON sd.customer_id = ci.customer_id
WHERE ci.customer_id IS NULL;

-- Store regions not represented in territory continents
SELECT 'K6: Store regions not in territory continents' AS check_name,
       s.region
FROM bronze.erp_stores s
LEFT JOIN bronze.erp_territory t
    ON LTRIM(RTRIM(s.region)) = LTRIM(RTRIM(t.continent))
WHERE t.continent IS NULL
GROUP BY s.region;


-- ============================================================================
-- L. SUMMARY: Row counts for all ERP bronze tables
-- ============================================================================

SELECT 'L1: Row counts' AS check_name,
       (SELECT COUNT(*) FROM bronze.erp_category_map)   AS category_map_rows,
       (SELECT COUNT(*) FROM bronze.erp_product_specs)   AS product_specs_rows,
       (SELECT COUNT(*) FROM bronze.erp_stores)          AS stores_rows,
       (SELECT COUNT(*) FROM bronze.erp_returns)         AS returns_rows,
       (SELECT COUNT(*) FROM bronze.erp_spatial_data)    AS spatial_data_rows,
       (SELECT COUNT(*) FROM bronze.erp_territory)       AS territory_rows;

PRINT '============================================='
PRINT 'ALL ERP DQ CHECKS COMPLETE'
PRINT '============================================='
