## Azure architecture (Medallion)

```mermaid
flowchart LR
  A[Source CSV/SFTP/SharePoint] -->|ADF Copy| B[ADLS Gen2 raw/claims]
  B -->|ADF Databricks activity| C[Notebook 01 Bronze]
  C --> D[ADLS Gen2 bronze Delta]
  D -->|Notebook 02 Silver| E[ADLS Gen2 silver Delta]
  E -->|Notebook 03 Gold| F[ADLS Gen2 gold marts Delta]
  F -->|Synapse Serverless OPENROWSET| G[Synapse Views]
  G --> H[Power BI]

  E -->|Great Expectations| Q[Quality Gate]
```
