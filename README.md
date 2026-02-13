# Azure Claims ETL (End-to-End) | ADF + Databricks + ADLS Gen2 + Synapse

End-to-end ETL pipeline for an insurance / healthcare-style claims dataset using:
    - **Azure Data Factory** for orchestration
    - **ADLS Gen2** as the lake (Raw → Bronze → Silver → Gold)
    - **Azure Databricks** (PySpark) for transformations
    - **Synapse Serverless SQL** for BI-friendly querying
    - **Great Expectations** starter checks for data quality
    - **GitHub Actions** for CI automation

> FileServer → ADLS Raw → Databricks Bronze → Silver → Gold → Synapse views.

---

## Repository structure

```text
azure-claims-etl/
├─ .github/
│  └─ workflows/                       # GitHub Actions workflows (CI)
├─ adf/                                # Azure Data Factory assets (pipelines, datasets, linked services)
├─ data/                               # Sample / local data files (e.g., claims.csv)
├─ databricks/
│  └─ notebooks/                       # Databricks notebooks (Bronze/Silver/Gold)
├─ docs/                               # Diagrams, screenshots, extra documentation
├─ infra/
│  └─ bicep/                           # Infrastructure-as-Code (Bicep templates/skeleton)
├─ quality/
│  └─ great_expectations/
│     └─ expectations/                 # Great Expectations suites/checks
├─ schemas/                            # Schema definitions / data contracts
├─ sql/
│  └─ synapse/                         # Synapse Serverless SQL views over Delta tables
├─ src/                                # Supporting code (helpers, configs, utilities)
└─ README.md
```
---

## What you get

### 1) ADF orchestration (adf/)

- Copy activity: File Server → ADLS Gen2 Raw
- Notebook activities: Runs Databricks notebooks in order:
    1. Bronze ingest
    2. Silver transform
    3. Gold marts

Example pipeline definition:
    - `PL_Claims_EndToEnd` copies the CSV into Raw and triggers the 3 Databricks stages.

### 2) Databricks Medallion notebooks (databricks/notebooks/)

- Bronze: ingest CSV → Delta, add metadata + record hash, partition by ingest date
- Silver: clean, filter invalid rows, dedupe by ClaimID, standardize text, add date partitions
- Gold: marts for KPI aggregates + simple fraud signals

### 3) Data Quality (quality/great_expectations/)

- Starter Great Expectations suites live under:
  - `quality/great_expectations/expectations/`

### 4) Synapse Serverless views (sql/synapse/)

- SQL scripts to query Delta outputs via Synapse Serverless and expose BI-ready views.

### 5) Infra skeleton (infra/bicep/)

- Bicep templates/skeleton to provision Azure resources (Storage/ADF/Databricks/Synapse etc.).

---

## How to run (Azure)

### Prerequisites

- Azure subscription
- ADLS Gen2 Storage Account (HNS enabled)
- Azure Data Factory
- Azure Databricks workspace (preferably Managed Identity auth)
- Synapse workspace (Serverless SQL is enough)
- (Recommended) Key Vault for secrets

#### 1) Create ADLS containers

Create these containers in your storage account:
    - `raw`
    - `bronze`
    - `silver`
    - `gold`

#### 2) Upload input data

Upload your CSV to:

`raw/claims/claims.csv`

(ADF can also copy it from File Server to that path depending on your setup.)

#### 3) Import ADF assets

- Import everything under adf/ into your Data Factory.
- Update any placeholders for:
    - storage account name / URL
    - databricks workspace details
    - subscription/resource group/workspace ids

The linked services + datasets in this project follow:
- `LS_ADLS_Gen2` → ADLS endpoint
- `LS_AzureDatabricks` → Databricks domain + workspace resource ID
- `DS_ADLS_Raw_Claims` → raw/claims/claims.csv

#### 4) Run the pipeline

Trigger:

`PL_Claims_EndToEnd`

This executes:

    1. CopyClaimsToRaw
    2. RunBronzeNotebook
    3. RunSilverNotebook
    4. RunGoldNotebook

---

## Output layout (lake)

- Bronze (Delta): `abfss://bronze@<storage>.dfs.core.windows.net/claims_delta/`
- Silver (Delta): `abfss://silver@<storage>.dfs.core.windows.net/claims_silver/`
- Gold (Delta marts): `abfss://gold@<storage>.dfs.core.windows.net/marts/`

`<storage>` = your Storage Account name

Gold marts produced:

`mart_claims_kpi_daily`
`mart_fraud_signals`

---

## Notes

- Prefer Managed Identity over secrets for ADF ↔ Databricks authentication.
- Keep configs parameterized (storage names, paths, dates) rather than hardcoding.
- Add/extend Great Expectations checks before publishing “production-ready” claims like “data quality ensured”.


