
/*
--------------------------------------------------------------
Create Database and Schemas for Bronze, Silver and Gold Layer
--------------------------------------------------------------

## Script Purpose: 
    This script create a new database named "Zenith_Global_Data_Warehouse". But first it will check 
    wheather the database already exists. If the database exists, it will be dropped and be recreated. 
    In addition, the script set three distinct schemas for bronze, silver and gold layer. 

## Warning: 
    Running this script will drop the entire database 'Zenith_Global_Data_Warehouse' if it exists. 
    All the data in the database will be removed permanently. So, ensure that you have proper backups 
    befor running this script.
*/



-- Use master database
USE master;
GO

-- Drop database if it exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Zenith_Global_Data_Warehouse')
BEGIN
    ALTER DATABASE Zenith_Global_Data_Warehouse 
    SET SINGLE_USER 
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE Zenith_Global_Data_Warehouse;
END
GO

-- Create database
CREATE DATABASE Zenith_Global_Data_Warehouse;
GO

-- Use the new database
USE Zenith_Global_Data_Warehouse
GO

-- Create schemas for Bronze, Silver, Gold layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
