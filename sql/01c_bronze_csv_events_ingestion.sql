/*===============================================================
 Data with Billy — Modern DW Project
 File:            sql/01d_bronze_events_csv_ingestion.sql
 Layer:           Bronze
 Purpose:         Ingest events_2mo_fallback.csv into bronze.events_csv_raw
 Summary:         1) Stage text via BULK INSERT (CRLF -> LF fallback)
                  2) Cast/clean into typed columns
                  3) Append lineage (_load_file, _load_ts default)
 Quick Start:     - Edit @csv_path to your file
                  - Ensure SQL Server service account can READ the path
================================================================*/
SET NOCOUNT ON;

-- Context & table check
IF DB_ID('DW_credit_card_fraud') IS NULL
    THROW 50000, 'DW_credit_card_fraud missing. Run 00_create_db.sql first.', 1;
USE DW_credit_card_fraud;

IF OBJECT_ID('bronze.events_csv_raw') IS NULL
    THROW 50000, 'bronze.events_csv_raw missing. Run 01c_bronze_events_csv_table_create.sql first.', 1;

-- 1) EDIT THIS PATH
DECLARE @csv_path NVARCHAR(4000) = N'C:\Users\User\OneDrive\Desktop\Project_SQL_Datawarehouse\events_2mo_fallback.csv';
DECLARE @lit      NVARCHAR(MAX)  = N'''' + REPLACE(@csv_path, '''', '''''') + N'''';  -- single-quoted literal

-- (optional) visibility check
IF OBJECT_ID('tempdb..#filecheck') IS NOT NULL DROP TABLE #filecheck;
CREATE TABLE #filecheck (FileExists INT, IsDir INT, ParentDirExists INT);
INSERT #filecheck EXEC master.dbo.xp_fileexist @csv_path;
IF NOT EXISTS (SELECT 1 FROM #filecheck WHERE FileExists = 1)
BEGIN
    RAISERROR('CSV not found or not readable by SQL Server service account: %s',16,1,@csv_path);
    RETURN;
END

-- 2) Stage as text (column order MUST match the CSV header)
-- Expected headers: SessionID,CustomerID,EventType,EventTS,Amount,Channel,IsCardPresent
IF OBJECT_ID('tempdb..#ev_stg') IS NOT NULL DROP TABLE #ev_stg;
CREATE TABLE #ev_stg
(
    SessionID       NVARCHAR(4000) NULL,
    CustomerID      NVARCHAR(4000) NULL,
    EventType       NVARCHAR(4000) NULL,
    EventTS         NVARCHAR(4000) NULL,
    Amount          NVARCHAR(4000) NULL,
    Channel         NVARCHAR(4000) NULL,
    IsCardPresent   NVARCHAR(4000) NULL
);

DECLARE @t0 DATETIME2(3) = SYSUTCDATETIME();

-- BULK INSERT (CRLF, then LF fallback). No FORMAT='CSV'.
BEGIN TRY
    DECLARE @bulk1 NVARCHAR(MAX) =
        N'BULK INSERT #ev_stg FROM ' + @lit + N'
          WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''0x0d0a'', TABLOCK);';
    EXEC (@bulk1);
END TRY
BEGIN CATCH
    DECLARE @bulk2 NVARCHAR(MAX) =
        N'BULK INSERT #ev_stg FROM ' + @lit + N'
          WITH (FIRSTROW=2, FIELDTERMINATOR='','', ROWTERMINATOR=''0x0a'', TABLOCK);';
    EXEC (@bulk2);
END CATCH;

-- 3) Insert into bronze (typed + lineage)
INSERT INTO bronze.events_csv_raw
(
    SessionID, CustomerID, EventType, EventTS, Amount, Channel, IsCardPresent, _load_file
)
SELECT
    NULLIF(LTRIM(RTRIM(SessionID)), '')                                            AS SessionID,
    TRY_CAST(CustomerID AS BIGINT)                                                AS CustomerID,
    NULLIF(LTRIM(RTRIM(EventType)), '')                                           AS EventType,
    TRY_CONVERT(DATETIME2(3), EventTS, 126)                                       AS EventTS,
    TRY_CAST(Amount AS DECIMAL(18,2))                                             AS Amount,
    NULLIF(LTRIM(RTRIM(Channel)), '')                                             AS Channel,
    CASE
        WHEN IsCardPresent IN ('1','true','TRUE','y','Y') THEN 1
        WHEN IsCardPresent IN ('0','false','FALSE','n','N') THEN 0
        ELSE TRY_CAST(IsCardPresent AS BIT)
    END                                                                           AS IsCardPresent,
    @csv_path                                                                      AS _load_file
FROM #ev_stg;

DECLARE @rows BIGINT = @@ROWCOUNT;
DECLARE @t1   DATETIME2(3) = SYSUTCDATETIME();

PRINT CONCAT('EVENTS CSV rows inserted: ', @rows, '  Runtime(ms): ', DATEDIFF(ms, @t0, @t1));

-- 4) Quick verify
SELECT TOP 10
    SessionID, CustomerID, EventType, EventTS, Amount, Channel, IsCardPresent, _load_file, _load_ts
FROM bronze.events_csv_raw
WHERE _load_file = @csv_path
ORDER BY _load_ts DESC;
