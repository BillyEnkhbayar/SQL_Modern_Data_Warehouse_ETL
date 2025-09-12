/*===============================================================
 Data with Billy — Modern DW Project
 File:            sql/00_create_db.sql
 Layer:           Util
 Purpose:         Creates DW_credit_card_fraud and switches context.
 Author:          Bilegsed (Billy) Enkhbayar
 Created:         2025-09-11
 Last Updated:    2025-09-11
 Tested On:       SQL Server 2022 / SSMS 21
 Prereqs:         Sysadmin/DB Creator permissions.
 Inputs:          None
 Outputs:         Database: DW_credit_card_fraud
 Run Order:       00 -> 01_bronze_load -> 02_silver_clean -> 03_gold_marts
 Runtime Notes:   Idempotent create (no-op if DB exists). Optional reset block commented.
 Rollback:        DROP DATABASE DW_credit_card_fraud (see optional block below)
----------------------------------------------------------------
 Quick Start:
 1) Run this file first.
 2) If you need a clean slate, uncomment the reset block.
================================================================*/


-- Check if the database 'DW_credit_card_fraud' exists. 
-- If it does not exist, create it.
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DW_credit_card_fraud')
BEGIN
    CREATE DATABASE DW_credit_card_fraud;
END;
GO

-- Switch context to use the 'DW_credit_card_fraud' database.
USE DW_credit_card_fraud;
GO

-- Optional: Drop and recreate the database.
-- Uncomment this section if you want to reset the database completely. (Ctrl K + Ctrl U)
-- 1. Check if the database exists.
-- 2. Set it to SINGLE_USER mode and roll back any active connections.
-- 3. Drop the existing database.
-- 4. Recreate the database.
-- 5. Switch context to the new database.

/*
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DW_credit_card_fraud') 
BEGIN
    ALTER DATABASE DW_credit_card_fraud SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DW_credit_card_fraud;
END;
GO
CREATE DATABASE DW_credit_card_fraud;
GO
USE DW_credit_card_fraud;
*/
