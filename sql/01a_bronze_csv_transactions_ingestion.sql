/*===============================================================
 Data with Billy — Modern DW Project
 File:            sql/01a_bronze_csv_transactions_ingest_only.sql
 Layer:           Bronze
 Purpose:         Load transactions_2mo.csv into bronze.transactions_csv_raw.
                  Keeps TransactionID as NVARCHAR (e.g., 'ORD2011797') and
                  appends lineage (_load_file, _load_ts).
 Summary:         1) Stages CSV as text with BULK INSERT (no FORMAT='CSV').
                  2) Handles Windows CRLF, then falls back to LF.
                  3) Cleans/CASTs types on INSERT; TransactionID stays text.
                  4) Requires the target table to already exist.
 Quick Start:     - Edit @csv_path to your file.
                  - Ensure file is "Always keep on this device" (OneDrive) and
                    SQL Server service account has READ permission.
                  - Run as a single batch (no GO before the INSERT).
================================================================*/
SET NOCOUNT ON;

-- Context
IF DB_ID('DW_credit_card_fraud') IS NULL
    THROW 50000, 'DW_credit_card_fraud is missing. Run 00_create_db.sql first.', 1;
USE DW_credit_card_fraud;

-- Require table to exist (created separately)
IF OBJECT_ID('bronze.transactions_csv_raw') IS NULL
    THROW 50000, 'bronze.transactions_csv_raw missing. Run 01_bronze_tables_create.sql first.', 1;

-- 1) EDIT THIS PATH
DECLARE @csv_path NVARCHAR(4000) = N'C:\Users\User\OneDrive\Desktop\Project_SQL_Datawarehouse\transactions_2mo.csv';

-- Build a SINGLE-QUOTED literal once:  N'...'
DECLARE @path_literal NVARCHAR(4000) = N'''' + REPLACE(@csv_path, '''', '''''') + N'''';

-- 2) Stage as text (order MUST match CSV header)
IF OBJECT_ID('tempdb..#csv_stg') IS NOT NULL DROP TABLE #csv_stg;
CREATE TABLE #csv_stg
(
    TransactionID NVARCHAR(4000) NULL,
    [Time]        NVARCHAR(4000) NULL,
    V1  NVARCHAR(4000) NULL, V2  NVARCHAR(4000) NULL, V3  NVARCHAR(4000) NULL, V4  NVARCHAR(4000) NULL, V5  NVARCHAR(4000) NULL,
    V6  NVARCHAR(4000) NULL, V7  NVARCHAR(4000) NULL, V8  NVARCHAR(4000) NULL, V9  NVARCHAR(4000) NULL, V10 NVARCHAR(4000) NULL,
    V11 NVARCHAR(4000) NULL, V12 NVARCHAR(4000) NULL, V13 NVARCHAR(4000) NULL, V14 NVARCHAR(4000) NULL, V15 NVARCHAR(4000) NULL,
    V16 NVARCHAR(4000) NULL, V17 NVARCHAR(4000) NULL, V18 NVARCHAR(4000) NULL, V19 NVARCHAR(4000) NULL, V20 NVARCHAR(4000) NULL,
    V21 NVARCHAR(4000) NULL, V22 NVARCHAR(4000) NULL, V23 NVARCHAR(4000) NULL, V24 NVARCHAR(4000) NULL, V25 NVARCHAR(4000) NULL,
    V26 NVARCHAR(4000) NULL, V27 NVARCHAR(4000) NULL, V28 NVARCHAR(4000) NULL,
    Amount     NVARCHAR(4000) NULL,
    Class      NVARCHAR(4000) NULL,
    CustomerID NVARCHAR(4000) NULL,
    MCC        NVARCHAR(4000) NULL,
    Country    NVARCHAR(4000) NULL,
    Currency   NVARCHAR(4000) NULL,
    CardLast4  NVARCHAR(4000) NULL,
    SessionID  NVARCHAR(4000) NULL,
    TransTS    NVARCHAR(4000) NULL
);

-- 3) BULK INSERT (CRLF, then LF fallback). No FORMAT='CSV'.
BEGIN TRY
    DECLARE @bulk1 NVARCHAR(MAX) =
        N'BULK INSERT #csv_stg FROM ' + @path_literal + N'
          WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''0x0d0a'', TABLOCK);';
    EXEC (@bulk1);
END TRY
BEGIN CATCH
    DECLARE @bulk2 NVARCHAR(MAX) =
        N'BULK INSERT #csv_stg FROM ' + @path_literal + N'
          WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''0x0a'', TABLOCK);';
    EXEC (@bulk2);
END CATCH;

-- 4) Insert to bronze (TransactionID stays text; others cast as needed)
INSERT INTO bronze.transactions_csv_raw
(
    TransactionID, [Time],
    V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28,
    Amount, Class, CustomerID, MCC, Country, Currency, CardLast4, SessionID, TransTS,
    _load_file
)
SELECT
    NULLIF(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TransactionID, '"',''), ',', ''), CHAR(9), ''), CHAR(13), ''), CHAR(10), ''))), ''),
    TRY_CAST([Time] AS INT),

    TRY_CAST(V1  AS FLOAT), TRY_CAST(V2  AS FLOAT), TRY_CAST(V3  AS FLOAT), TRY_CAST(V4  AS FLOAT), TRY_CAST(V5  AS FLOAT),
    TRY_CAST(V6  AS FLOAT), TRY_CAST(V7  AS FLOAT), TRY_CAST(V8  AS FLOAT), TRY_CAST(V9  AS FLOAT), TRY_CAST(V10 AS FLOAT),
    TRY_CAST(V11 AS FLOAT), TRY_CAST(V12 AS FLOAT), TRY_CAST(V13 AS FLOAT), TRY_CAST(V14 AS FLOAT), TRY_CAST(V15 AS FLOAT),
    TRY_CAST(V16 AS FLOAT), TRY_CAST(V17 AS FLOAT), TRY_CAST(V18 AS FLOAT), TRY_CAST(V19 AS FLOAT), TRY_CAST(V20 AS FLOAT),
    TRY_CAST(V21 AS FLOAT), TRY_CAST(V22 AS FLOAT), TRY_CAST(V23 AS FLOAT), TRY_CAST(V24 AS FLOAT), TRY_CAST(V25 AS FLOAT),
    TRY_CAST(V26 AS FLOAT), TRY_CAST(V27 AS FLOAT), TRY_CAST(V28 AS FLOAT),

    TRY_CAST(Amount AS DECIMAL(18,2)),
    CASE WHEN Class IN ('1','true','TRUE','y','Y') THEN 1
         WHEN Class IN ('0','false','FALSE','n','N') THEN 0 ELSE NULL END,
    TRY_CAST(CustomerID AS BIGINT),
    TRY_CAST(MCC        AS INT),
    NULLIF(Country,  ''),
    NULLIF(Currency, ''),
    TRY_CAST(CardLast4 AS INT),
    NULLIF(SessionID, ''),
    TRY_CONVERT(DATETIME2(3), TransTS, 126),

    @csv_path
FROM #csv_stg;

-- 5) Verify
SELECT TOP 10 TransactionID, V1, V2, Amount, Class, CustomerID, MCC, Country, Currency, CardLast4, SessionID, TransTS, _load_file, _load_ts
FROM bronze.transactions_csv_raw
ORDER BY _load_ts DESC;
