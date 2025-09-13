/*===============================================================
 Data with Billy — Modern DW Project
 File:            sql/01_bronze_tables_create.sql
 Layer:           Bronze
 Purpose:         Create bronze tables for CSV (transactions), JSON (customers),
                  and Parquet (events) with lineage columns.
 Author:          Bilegsed (Billy) Enkhbayar
 Created:         2025-09-11
 Tested On:       SQL Server 2022 / SSMS 21
 Prereqs:         Database + schemas exist (bronze). If not, create them first.
 Inputs:          (structure only; ingestion runs in separate scripts)
 Outputs:         bronze.transactions_csv_raw
                  bronze.customers_json_raw
                  bronze.events_parquet_raw
 Run Order:       00_create_db -> 00b_create_schemas -> 01_bronze_tables_create
 Runtime Notes:   Idempotent CREATE IF NOT EXISTS style.
 Rollback:        DROP TABLE bronze.<table_name>;
----------------------------------------------------------------
 Quick Start:
 1) USE DW_credit_card_fraud;
 2) Run this script to create all bronze tables.
================================================================*/

-- Ensure database exists and set context ( I do this, just in case)
IF DB_ID('DW_credit_card_fraud') IS NULL
    THROW 50000, 'Database DW_credit_card_fraud does not exist. Run 00_create_db.sql first.', 1;
USE DW_credit_card_fraud;

-- Ensure schema exists (same for this too)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'bronze') EXEC('CREATE SCHEMA bronze');

---------------------------------------------------------------
-- A) CSV source → bronze.transactions_csv_raw
-- Based on columns observed in transactions_2mo.csv:
-- TransactionID, Time, V1..V28, Amount, Class, CustomerID, MCC,
-- Country, Currency, CardLast4, SessionID, TransTS
---------------------------------------------------------------

IF OBJECT_ID('bronze.transactions_csv_raw') IS NULL
BEGIN
    CREATE TABLE bronze.transactions_csv_raw
    (
        TransactionID    BIGINT            NULL,
        [Time]           INT               NULL,
        V1               FLOAT             NULL,
        V2               FLOAT             NULL,
        V3               FLOAT             NULL,
        V4               FLOAT             NULL,
        V5               FLOAT             NULL,
        V6               FLOAT             NULL,
        V7               FLOAT             NULL,
        V8               FLOAT             NULL,
        V9               FLOAT             NULL,
        V10              FLOAT             NULL,
        V11              FLOAT             NULL,
        V12              FLOAT             NULL,
        V13              FLOAT             NULL,
        V14              FLOAT             NULL,
        V15              FLOAT             NULL,
        V16              FLOAT             NULL,
        V17              FLOAT             NULL,
        V18              FLOAT             NULL,
        V19              FLOAT             NULL,
        V20              FLOAT             NULL,
        V21              FLOAT             NULL,
        V22              FLOAT             NULL,
        V23              FLOAT             NULL,
        V24              FLOAT             NULL,
        V25              FLOAT             NULL,
        V26              FLOAT             NULL,
        V27              FLOAT             NULL,
        V28              FLOAT             NULL,
        Amount           DECIMAL(18,2)     NULL,
        Class            BIT               NULL,   -- 0/1 flag
        CustomerID       BIGINT            NULL,
        MCC              INT               NULL,
        Country          NVARCHAR(50)      NULL,
        Currency         NVARCHAR(10)      NULL,   -- e.g., 'USD'
        CardLast4        INT               NULL,   -- last 4 digits
        SessionID        NVARCHAR(100)     NULL,
        TransTS          DATETIME2(3)      NULL,   -- transaction timestamp
        -- lineage
        _load_file       NVARCHAR(4000)    NULL,
        _load_ts         DATETIME2(3)      NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

---------------------------------------------------------------
-- B) JSON source → bronze.customers_json_raw
-- Store raw JSON per row; shred later in Silver.
---------------------------------------------------------------

IF OBJECT_ID('bronze.customers_json_raw') IS NULL
BEGIN
    CREATE TABLE bronze.customers_json_raw
    (
        json_doc    NVARCHAR(MAX)  NOT NULL,
        _load_file  NVARCHAR(4000) NULL,
        _load_ts    DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

---------------------------------------------------------------
-- C) csv source → bronze.events_csv_raw
-- Based on columns observed in events_2mo:
-- SessionID, CustomerID, EventType, EventTS, Amount, Channel, IsCardPresent
---------------------------------------------------------------

IF OBJECT_ID('bronze.events_csv_raw') IS  NULL
BEGIN
CREATE TABLE bronze.events_csv_raw
(
    SessionID       NVARCHAR(100)   NULL,
    CustomerID      BIGINT          NULL,
    EventType       NVARCHAR(50)    NULL,    -- e.g., login, add_card, pos_swipe
    EventTS         DATETIME2(3)    NULL,    -- ISO-8601 preferred
    Amount          DECIMAL(18,2)   NULL,
    Channel         NVARCHAR(50)    NULL,    -- ONLINE / POS / APP
    IsCardPresent   BIT             NULL,

    -- lineage
    _load_file      NVARCHAR(4000)  NULL,
    _load_ts        DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME()
);
