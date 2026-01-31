/*
-------------------------------------------------------------------------------
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
-------------------------------------------------------------------------------
*/

-------------------------------------------------------------------------------
-- 1. CRM_CUSTOMER_INFO table
-------------------------------------------------------------------------------

-- DROP the table if the table already EXISTS in the DATABASE
IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL   -- here 'U' means user defined 
    DROP TABLE bronze.crm_customer_info;
GO 
-- CREATE crm_customer_info TABLE
CREATE TABLE bronze.crm_customer_info(
    customer_id             INT,                --unique id from the CRM
    customer_number         NVARCHAR(50),       -- the business identifier
    first_name              NVARCHAR(50),       
    last_name               NVARCHAR(50),
    marital_status          NVARCHAR(50),
    gender                  NVARCHAR(50),
    birthdate               DATE,               -- properly use data type DATE
    create_date             DATE                -- properly use date type DATE (profile created)
);
GO

-------------------------------------------------------------------------------
-- 2. CRM_SPATIAL_DATA table
-------------------------------------------------------------------------------

-- DROP table if the table already EXISTS in the DATABASE
IF OBJECT_ID('bronze.crm_spatial_data','U') IS NOT NULL
    DROP TABLE bronze.crm_spatial_data;
GO
-- CREATE crm_spatial_data 
CREATE TABLE bronze.crm_spatial_data(
    customer_id             INT,
    country                 VARCHAR(50),
    city                    VARCHAR(50)
);

GO
-------------------------------------------------------------------------------
-- 3. CRM_TERRITORY table
-------------------------------------------------------------------------------
-- DROP table if the table already EXISTS in the DATABASE
IF OBJECT_ID('bronze.crm_territory','U') IS NOT NULL
    DROP TABLE bronze.crm_territory; 
GO
-- CREATE crm_territory TABLE
CREATE TABLE bronze.crm_territory(
    city                    VARCHAR(50),
    country                 VARCHAR(50),
    continent               VARCHAR(50)
);

GO
-------------------------------------------------------------------------------
-- 4. ERP_SALES_ORDER table
-------------------------------------------------------------------------------
-- DROP the table if the table already ESISTS in the DATABASE 
IF OBJECT_ID('bronze.erp_sales_order', 'U') IS NOT NULL
    DROP TABLE bronze.erp_sales_order; 
GO 

-- CREATE bronze.erp_sales_order TABLE
CREATE TABLE bronze.erp_sales_order(
    order_number            NVARCHAR(50),      -- Unique Order Identifier (e.g., SO50000)
    product_id              INT,               -- Links to Product Dimension
    customer_id             INT,               -- Links to Customer Dimension
    order_date              DATE,              -- Date of purchase
    quantity                INT,               -- Number of units sold
    price                   DECIMAL(18, 2),    -- Unit price
    shipping_date           DATE,              -- Date item was shipped
    due_date                DATE,              -- Payment/Delivery due date
    sales_amount            DECIMAL(18, 2),    -- Total transaction value (Qty * Price)
);

GO
-------------------------------------------------------------------------------
-- 5. CRM_PRODUCT_DATA table
-------------------------------------------------------------------------------

-- DROP the table if the table already ESISTS in the DATABASE 
IF OBJECT_ID('bronze.erp_product_data', 'U') IS NOT NULL
    DROP TABLE bronze.erp_product_data; 
GO 

-- CREATE erp_product_data TABLE
CREATE TABLE bronze.erp_product_data(
    product_id              INT, 
    product_number          NVARCHAR(50),
    product_name            NVARCHAR(100),
    category_id             INT,
    cost                    DECIMAL(18,2),
    product_line            NVARCHAR(50),
    start_data              DATE
);

GO
-------------------------------------------------------------------------------
-- 6. ERP_PRODUCT_SPECS table
-------------------------------------------------------------------------------

-- DROP the table if the table already ESISTS in the DATABASE 
IF OBJECT_ID('bronze.erp_product_specs', 'U') IS NOT NULL
    DROP TABLE bronze.erp_product_specs; 
GO 

-- CREATE erp_product_specs TABLE
CREATE TABLE bronze.erp_product_specs(
    product_id              INT,
    subcategory             NVARCHAR(50),
    maintenance_required    NVARCHAR(50),
    product_line            NVARCHAR(50)
);

GO
-------------------------------------------------------------------------------
-- 7. CRM_CATEGORY_MAP table
-------------------------------------------------------------------------------

IF OBJECT_ID('bronze.erp_category_map', 'U') IS NOT NULL 
DROP TABLE bronze.erp_category_map;
GO

CREATE TABLE bronze.erp_category_map (
    category_id          INT,
    category             NVARCHAR(50),
    subcategory          NVARCHAR(50),
    maintenance_required NVARCHAR(10)
);

GO
-----------------------------------------------------------
-- 8. ERP_STORES table
-----------------------------------------------------------
IF OBJECT_ID('bronze.erp_stores', 'U') IS NOT NULL DROP TABLE bronze.erp_stores;
CREATE TABLE bronze.erp_stores (
    store_id     INT,
    store_name   NVARCHAR(100),
    store_type   NVARCHAR(50), -- e.g., Retail, Outlet, Online
    region       NVARCHAR(50)
);
GO

-----------------------------------------------------------
-- 9. ERP_RETURNS table
-----------------------------------------------------------

IF OBJECT_ID('bronze.erp_returns', 'U') IS NOT NULL DROP TABLE bronze.erp_returns;
CREATE TABLE bronze.erp_returns (
    return_id      INT,
    order_number   NVARCHAR(50),
    return_date    DATE,
    return_reason  NVARCHAR(100),
    return_amount  DECIMAL(18, 2)
);

