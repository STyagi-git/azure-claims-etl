-- Synapse Serverless SQL: Delta views
CREATE DATABASE IF NOT EXISTS claims_dw;
GO

CREATE EXTERNAL DATA SOURCE ClaimsGold
WITH (
    LOCATION = 'abfss://gold@styagidatalake.dfs.core.windows.net/'
);
GO

CREATE OR ALTER VIEW claims_dw.v_mart_claims_kpi_daily AS
SELECT *
FROM OPENROWSET(
    BULK 'marts/mart_claims_kpi_daily',
    DATA_SOURCE = 'ClaimsGold',
    FORMAT = 'DELTA'
) AS rows;
GO
