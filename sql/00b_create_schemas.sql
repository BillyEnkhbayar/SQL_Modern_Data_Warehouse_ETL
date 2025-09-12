/*===============================================================
 Data with Billy — Modern DW Project
 File:            sql/00b_create_schemas.sql
 Layer:           Util
 Purpose:         Create core schemas (bronze/silver/gold/util) used by the DW.
 Author:          Bilegsed (Billy) Enkhbayar
 Created:         2025-09-11
 Last Updated:    2025-09-11
 Tested On:       SQL Server 2022 / SSMS 21
 Prereqs:         Database DW_credit_card_fraud exists; user has CREATE SCHEMA.
 Inputs:          None
 Outputs:         Schemas: bronze, silver, gold, util
 Run Order:       00_create_db -> 00b_create_schemas -> 01_bronze_load -> 02_silver_clean -> 03_gold_marts
 Runtime Notes:   Idempotent (no-op if schema already exists). Non-destructive.
 Rollback:        DROP SCHEMA <name>;  -- only succeeds if schema is empty.
----------------------------------------------------------------
 Quick Start:
 1) USE DW_credit_card_fraud;
 2) Execute this script (batch-by-batch if needed).
================================================================*/

-- Ensure you are in the right DB (adjust if you renamed it.)
IF DB_ID('DW_credit_card_fraud') IS NULL
    THROW 50000, 'Database DW_credit_card_fraud does not exist. Run 00_create_db.sql first.', 1;
GO
USE DW_credit_card_fraud;
GO

-- Create schemas if they do not already exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'gold')   EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'util')   EXEC('CREATE SCHEMA util');
GO

-- (Optional) Show what was created / already present
PRINT 'Schemas present:';
SELECT s.name AS schema_name
FROM sys.schemas s
WHERE s.name IN (N'bronze', N'silver', N'gold', N'util')
ORDER BY s.name;

-- Creating a role and permissions are crucial in a real project. Since the purpose of this project is demonstrate my skills, 
-- I am going to ignore setting roles and permissions.
