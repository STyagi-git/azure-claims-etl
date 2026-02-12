CREATE OR ALTER VIEW claims_dw.v_kpi_summary AS
SELECT
  claim_date,
  ProviderSpecialty,
  ClaimType,
  SUM(claim_count) AS claim_count,
  SUM(total_claim_amount) AS total_claim_amount,
  AVG(avg_claim_amount) AS avg_claim_amount
FROM claims_dw.v_mart_claims_kpi_daily
GROUP BY claim_date, ProviderSpecialty, ClaimType;
GO
