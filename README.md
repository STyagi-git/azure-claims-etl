# Azure Claims ETL (End-to-End) | ADF + Databricks + ADLS Gen2 + Synapse

This repo is an end-to-end ETL pipeline for an insurance/healthcare-like **claims dataset**.

## What you get
- Medallion (Bronze/Silver/Gold) on **ADLS Gen2**
- **ADF** pipeline JSON (copy + Databricks notebook runs)
- **Databricks** notebooks for Bronze ingest, Silver cleaning/dedupe, Gold marts
- **Synapse Serverless** SQL views over Delta for BI
- **Great Expectations** starter suite for data quality
- **Bicep** skeleton for infra
- **GitHub Actions** CI

## How to run (Azure)
1. Provision: Storage (HNS on), ADF, Databricks, Synapse (serverless), Key Vault (recommended).
2. Create containers: `raw`, `bronze`, `silver`, `gold`.
3. Upload your full CSV into: `raw/claims/claims.csv`
4. Import ADF assets from `adf/` and run pipeline `PL_Claims_EndToEnd`.
5. Create Synapse views using `sql/synapse/*.sql`.

## Notes
- Replace placeholders like `<storage>`, `<workspace>`, `<sub>` in configs.
- Don’t hardcode secrets. Use **Managed Identity + Key Vault**.
