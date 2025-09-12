/*===============================================================
 Data with Billy — Bronze JSON Ingest (ingest-only)
 File:   sql/01b_bronze_json_customers_ingestion.sql
 Purpose: Store one row per customer object from $.customers
================================================================*/
SET NOCOUNT ON;

IF DB_ID('DW_credit_card_fraud') IS NULL
    THROW 50000, 'DW_credit_card_fraud missing.', 1;
USE DW_credit_card_fraud;

IF OBJECT_ID('bronze.customers_json_raw') IS NULL
    THROW 50000, 'bronze.customers_json_raw missing. Run 01_bronze_tables_create.sql first.', 1;

-- EDIT THIS PATH
DECLARE @json_path NVARCHAR(4000)=N'C:\Users\User\OneDrive\Desktop\Project_SQL_Datawarehouse\customers_profile.json';
DECLARE @lit NVARCHAR(MAX)=N''''+REPLACE(@json_path,'''','''''')+N'''';

-- Insert each element from $.customers as one row (raw JSON + lineage)
DECLARE @sql NVARCHAR(MAX)=
N';WITH src AS (
    SELECT CAST(BulkColumn AS NVARCHAR(MAX)) AS payload
    FROM OPENROWSET(BULK '+@lit+', SINGLE_CLOB) AS s
)
INSERT INTO bronze.customers_json_raw (json_doc, _load_file)
SELECT j.value, '+@lit+N'
FROM src
CROSS APPLY OPENJSON(src.payload, ''$.customers'') AS j
WHERE ISJSON(j.value)=1;';
EXEC(@sql);

-- Verify
SELECT COUNT(*) AS rows_loaded_for_this_file
FROM bronze.customers_json_raw
WHERE _load_file=@json_path;

SELECT TOP 3 LEFT(json_doc,200) AS json_preview, _load_file, _load_ts
FROM bronze.customers_json_raw
WHERE _load_file=@json_path
ORDER BY _load_ts DESC;
