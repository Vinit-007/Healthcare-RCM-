# Azure Healthcare RCM ETL Project

## End-to-End Azure Data Engineering Solution for Healthcare Revenue Cycle Management

---

##  Project Overview

This project implements a Azure Data Engineering solution for the Healthcare Revenue Cycle Management (RCM) domain. It focuses specifically on Accounts Receivable (AR) analytics—the process hospitals use to track and collect payments from patients and insurance providers.

The solution handles complex healthcare data integration challenges, including merging disparate hospital systems (Hospital A and Hospital B) with different schemas and overlapping patient IDs, to enable accurate calculation of critical financial KPIs.

---

##  Problem Statement

Hospitals struggle with:

- Disparate data sources across multiple hospital branches
- Schema inconsistencies between different EMR systems
- ID collisions where the same identifier refers to different patients
- Manual Excel-based reporting leading to delayed financial insights
- Inability to track unpaid claims, denied payments, and aging receivables

Business Questions We Answer:
- How much money is stuck in accounts receivable beyond 90 days?
- What is our average Days in AR (collection period)?
- How effective are we at collecting from insurance providers?
- Which departments/providers have the highest outstanding balances?

---

##  Architecture



### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│   Ingestion     │───▶│   Processing    │───▶│   Consumption   │
│                 │    │   (ADF)         │    │   (Databricks)  │    │   (Analytics)   │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Azure SQL DB  │    │ • Generic       │    │ • Bronze →      │    │ • Gold Tables   │
│   (Hospital A)  │    │   Pipelines     │    │   Parquet       │    │   (Star Schema) │
│ • Azure SQL DB  │    │ • Metadata-     │    │ • Silver →      │    │ • Power BI      │
│   (Hospital B)  │    │   Driven        │    │   Delta Tables  │    │   Dashboards    │
│ • Claims Files  │    │ • Incremental/  │    │ • SCD Type 2    │    │ • SQL Analytics │
│   (CSV)         │    │   Full Loads    │    │ • CDM           │    │                 │
│ • Public APIs   │    │ • Archiving     │    │ • Quarantine    │    │                 │
│   (NPI, ICD)    │    │                 │    │   Logic         │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                      │
                              ┌───────▼───────┐
                              │   Azure Key   │
                              │   Vault       │
                              │   (Secrets)   │
                              └───────────────┘
```

---

##  Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Orchestration | Azure Data Factory (ADF) | Pipeline orchestration, metadata-driven ingestion |
| Processing | Azure Databricks | Spark-based transformations, ETL logic |
| Storage | Azure Data Lake Storage Gen2 | Central data repository (Landing → Bronze → Silver → Gold) |
| Databases | Azure SQL Database | Source EMR systems (Hospital A & B) |
| Table Format | Delta Lake | ACID transactions, SCD Type 2,  |
| Security | Azure Key Vault | Secrets management, credentials storage |
| APIs | NPI Registry, WHO ICD-10 API | Reference data enrichment |

---

##  Medallion Architecture Implementation

### Landing Zone
- Temporary storage for raw CSV files from third-party insurance payors
- Files uploaded monthly by insurance companies
- Format: CSV

### Bronze Layer (Source of Truth)
- Raw data from all sources stored in Parquet format
- Automatic archiving of existing files before new ingestion
- Data organized by source (hosa/, hosb/) and table name
- Metadata columns added: `data_source`, `ingestion_timestamp`
- Key Features:
  - Full loads for small tables (Providers, Departments)
  - Incremental loads for large tables (Encounters, Transactions)
  - Extraction using audit logs

### Silver Layer (Cleansing & CDM)
- Data transformed into Delta Tables
- Common Data Model (CDM) implementation
  - Standardized column names across hospitals
  - `unionByName` for flexible schema merging
- Surrogate Keys to prevent ID collisions
  - `CONCAT(SRC_PatientID, '-', datasource) AS Patient_Key`
- SCD Type 2 for historical tracking
  - `is_current`, `inserted_date`, `modified_date` columns
- Quarantine Logic
  - `is_quarantined` flag for bad records
  - NULL checks, string validation ('null' values)
- Tables: patients, providers, departments, encounters, transactions, claims

### Gold Layer (Reporting)
- Star Schema (Fact and Dimension tables)
- Only current (`is_current = true`) and clean (`is_quarantined = false`) data
- Dimension Tables:
  - `dim_patient`, `dim_provider`, `dim_department`
  - `dim_cpt_code`, `dim_icd`, `dim_npi`
- Fact Tables:
  - `fact_transactions` (charge amounts, paid amounts)
- Business KPIs Enabled:
  - Accounts Receivable aging (0-30, 31-60, 61-90, 90+ days)
  - Days in AR = (Total AR / Average Daily Revenue)
  - Net Collection Rate = (Payments / Allowed Charges)
  - Provider/Department revenue analysis

---

##  Data Pipeline Workflow

### 1. Metadata-Driven Ingestion (ADF)

```
┌─────────────────────────────────────────────────────────────────┐
│                        ADF Pipeline                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                                │
│  │   Lookup    │── Reads load_config.csv                        │
│  │   Activity  │   (table_name, load_type, source, target_path) │
│  └─────────────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────┐                                                │
│  │   ForEach   │── Iterates through each table in config        │
│  │   Activity  │   (Batch count = 5 for parallelism)            │
│  └─────────────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────────────┐                                        │
│  │  Get Metadata       │── Checks if file exists in Bronze      │
│  │  Activity           │   (dynamically constructed path)       │
│  └─────────────────────┘                                        │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────────────┐                                        │
│  │  If Condition       │── If exists → Archive existing file    │
│  │  (Exists?)          │   to /archive/yyyy/mm/dd/              │
│  └─────────────────────┘                                        │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────────────┐                                        │
│  │  Load Type Check    │── Full vs Incremental                  │
│  └─────────────────────┘                                        │
│         ├─────────────────┬────────────────────┐                │
│         ▼                 ▼                    ▼                │
│  ┌─────────────┐   ┌─────────────┐      ┌─────────────┐        │
│  │   Full      │   │ Incremental │      │  Update     │        │
│  │   Load      │   │ Load        │      │  Audit Log  │        │
│  │   (SELECT *)│   │(WHERE date  │      │  (load_logs)│        │
│  └─────────────┘   │ >= watermark│      └─────────────┘        │
│                    └─────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Bronze → Silver Transformation (Databricks)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Silver Layer Processing                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step 1: Read from Bronze                                │   │
│  │  ┌─────────────────┐    ┌─────────────────┐             │   │
│  │  │ /mnt/bronze/    │    │ /mnt/bronze/    │             │   │
│  │  │ hosa/patients   │    │ hosb/patients   │             │   │
│  │  └────────┬────────┘    └────────┬────────┘             │   │
│  │           └──────────┬───────────┘                       │   │
│  │                      ▼                                   │   │
│  │  ┌─────────────────────────────────────┐                 │   │
│  │  │  cdm_patients (TEMP VIEW)           │                 │   │
│  │  │  • Standardize column names         │                 │   │
│  │  │  • CONCAT(id, '-', source) AS       │                 │   │
│  │  │    Patient_Key                       │                 │   │
│  │  │  • UNION ALL from both hospitals     │                 │   │
│  │  └────────────────┬────────────────────┘                 │   │
│  │                   ▼                                       │   │
│  │  ┌─────────────────────────────────────┐                 │   │
│  │  │  quality_checks (TEMP VIEW)         │                 │   │
│  │  │  • CASE WHEN ... THEN                │                 │   │
│  │  │    is_quarantined = TRUE/FALSE       │                 │   │
│  │  │  • Check NULLs, 'null' strings       │                 │   │
│  │  └────────────────┬────────────────────┘                 │   │
│  │                   ▼                                       │   │
│  │  ┌─────────────────────────────────────┐                 │   │
│  │  │  MERGE INTO silver.patients         │                 │   │
│  │  │  • SCD Type 2 logic                  │                 │   │
│  │  │  • Update is_current=FALSE on change │                 │   │
│  │  │  • Insert new version                │                 │   │
│  │  └─────────────────────────────────────┘                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3. Silver → Gold Transformation

```
┌─────────────────────────────────────────────────────────────────┐
│                      Gold Layer Processing                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step 1: Filter Data                                     │   │
│  │  • WHERE is_current = TRUE                               │   │
│  │  • AND is_quarantined = FALSE                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step 2: Create Dimension Tables                         │   │
│  │  ┌─────────────────┐    ┌─────────────────┐             │   │
│  │  │ dim_patient     │    │ dim_provider    │             │   │
│  │  │ dim_department  │    │ dim_cpt_code    │             │   │
│  │  │ dim_icd         │    │ dim_npi         │             │   │
│  │  └─────────────────┘    └─────────────────┘             │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step 3: Create Fact Table                               │   │
│  │  fact_transactions:                                      │   │
│  │  • FK_Patient_Key                                        │   │
│  │  • FK_Provider_Key                                       │   │
│  │  • FK_Department_Key                                     │   │
│  │  • Charge_Amt, Paid_Amt                                  │   │
│  │  • (Charge_Amt - Paid_Amt) AS Remaining_AR              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           ▼                                      │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Step 4: Business KPIs                                   │   │
│  │  • AR Aging buckets                                      │   │
│  │  • Days in AR                                            │   │
│  │  • Net Collection Rate                                   │   │
│  │  • Revenue by Provider/Department                        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

##  Key Features

### 1. Metadata-Driven Pipelines
- Single generic ADF pipeline reads `load_config.csv`
- Configuration controls: table names, load types (Full/Incremental), source paths
- `is_active` flag to enable/disable tables without code changes
- Dynamic path construction for source and destination

### 2. Incremental Loading with Watermarking
- Large tables (Encounters, Transactions) use incremental loads
- Watermark column: `modified_date`
- Audit table `load_logs` tracks last successful load timestamp
- Idempotent design with `>=` condition for safe reprocessing

### 3. Parallel Processing
- Initial sequential design (due to auto-increment ID in audit table)
- Enhanced to parallel processing (batch count = 5)
- Removed identity column to eliminate locking issues
- 5x performance improvement

### 4. Common Data Model (CDM)
- Standardizes disparate schemas from Hospital A and Hospital B
- Column name mapping using aliases
- `unionByName` for flexible schema merging
- Preserves original IDs as `SRC_*` columns for lineage

### 5. Surrogate Keys for ID Collision Resolution
```
Patient_Key = CONCAT(SRC_PatientID, '-', datasource)
Example: 101-hosa, 101-hosb
```
- Ensures global uniqueness across hospital network
- Maintains referential integrity in star schema

### 6. SCD Type 2 Implementation
- Historical tracking for: Patients, Transactions, Encounters, Claims
- Metadata columns: `is_current`, `inserted_date`, `modified_date`
- MERGE statement with field-by-field comparison
- Full history preservation for auditing

### 7. Data Quality & Quarantine Logic
```
CASE 
  WHEN SRC_PatientID IS NULL 
    OR dob IS NULL 
    OR firstname IS NULL 
    OR lower(firstname) = 'null' 
  THEN TRUE 
  ELSE FALSE 
END AS is_quarantined
```
- Bad records isolated (not deleted)
- Gold layer filters: `WHERE is_quarantined = FALSE`
- Preserves data for debugging and remediation

### 8. Multiple Data Source Integration
| Source Type | Method | Frequency |
|-------------|--------|-----------|
| Azure SQL DB (EMR) | ADF Copy Activity | Daily (Incremental) |
| Claims CSV Files | Landing Zone → Bronze | Monthly |
| NPI Registry API | Databricks Python requests | As needed |
| WHO ICD-10 API | Databricks with OAuth2 | As needed |

### 9. Automated Archiving
- Before new ingestion, existing files moved to `/archive/yyyy/mm/dd/`
- Prevents data collisions
- Maintains historical raw data for recovery

### 10. Star Schema for Analytics
- Fact: `fact_transactions` (quantitative metrics)
- Dimensions: patient, provider, department, codes
- Optimized for business user queries
- Enables complex RCM KPI calculations

---

##  Data Models

### Core Tables

#### Bronze Layer (Parquet)
```
/mnt/bronze/
├── hosa/
│   ├── patients/
│   ├── providers/
│   ├── departments/
│   ├── encounters/
│   └── transactions/
├── hosb/
│   ├── patients/
│   ├── providers/
│   ├── departments/
│   ├── encounters/
│   └── transactions/
└── claims/
    └── [insurance_files]/
```

#### Silver Layer (Delta Tables)
```sql
-- silver.patients
CREATE TABLE IF NOT EXISTS silver.patients (
    Patient_Key STRING,
    SRC_PatientID STRING,
    firstname STRING,
    lastname STRING,
    dob DATE,
    address STRING,
    phone STRING,
    datasource STRING,
    is_quarantined BOOLEAN,
    is_current BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP
) USING DELTA;

-- silver.transactions
CREATE TABLE IF NOT EXISTS silver.transactions (
    Transaction_Key STRING,
    SRC_TransactionID STRING,
    encounter_id STRING,
    charge_amt DECIMAL(10,2),
    paid_amt DECIMAL(10,2),
    transaction_date DATE,
    datasource STRING,
    is_quarantined BOOLEAN,
    is_current BOOLEAN,
    inserted_date TIMESTAMP,
    modified_date TIMESTAMP
) USING DELTA;
```

#### Gold Layer (Star Schema)
```sql
-- gold.dim_patient
CREATE TABLE IF NOT EXISTS gold.dim_patient (
    Patient_Key STRING,
    firstname STRING,
    lastname STRING,
    dob DATE,
    address STRING,
    phone STRING
) USING DELTA;

-- gold.fact_transactions
CREATE TABLE IF NOT EXISTS gold.fact_transactions (
    Transaction_Key STRING,
    FK_Patient_Key STRING,
    FK_Provider_Key STRING,
    FK_Department_Key STRING,
    charge_amt DECIMAL(10,2),
    paid_amt DECIMAL(10,2),
    remaining_ar DECIMAL(10,2),
    transaction_date DATE,
    aging_bucket STRING
) USING DELTA;
```

### Audit Table
```sql
-- audit.load_logs
CREATE TABLE IF NOT EXISTS audit.load_logs (
    data_source STRING,
    table_name STRING,
    number_of_rows_copied BIGINT,
    watermark_column_name STRING,
    load_date TIMESTAMP
) USING DELTA;
```

---

##  Security & Governance

### Azure Key Vault Integration
- Secrets stored: Storage account keys, SQL passwords, Databricks tokens
- No hardcoded credentials in notebooks or pipelines
- Databricks secret scope mapped to Key Vault
- ADF Linked Service for Key Vault access

### App Registrations
- Service principals created for ADF and Databricks
- Access policies in Key Vault (Get/List permissions only)
- Principle of least privilege

### Unity Catalog
- Centralized metadata management
- Data lineage tracking (Bronze → Silver → Gold)
- Access control at schema/table level
- Cross-workspace discoverability

### ADLS Mount Points
```python
# Secure mount with Key Vault integration
storage_account = "ttadlsdev"
container_names = ["gold", "silver", "bronze", "landing", "configs"]

for container in container_names:
    if not any(mount.mountPoint == f"/mnt/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
            mount_point = f"/mnt/{container}",
            extra_configs = {
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net": 
                dbutils.secrets.get(scope="tt-hc-kv", key="tt-adls-access-key-dev")
            }
        )
```

---

##  Getting Started

### Prerequisites
- Azure subscription
- Azure Data Factory
- Azure Databricks workspace
- Azure Data Lake Storage Gen2
- Azure SQL Database (or use provided Faker generator)
- Azure Key Vault

### Setup Steps

#### 1. Clone Repository
```bash
git clone https://github.com/yourusername/azure-healthcare-rcm-etl.git
cd azure-healthcare-rcm-etl
```

#### 2. Configure Azure Resources
```powershell
# Create Resource Group
az group create --name tt-healthcare-rg --location eastus

# Create Storage Account
az storage account create --name ttadlsdev --resource-group tt-healthcare-rg --location eastus --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true

# Create Containers
az storage container create --account-name ttadlsdev --name landing --auth-mode login
az storage container create --account-name ttadlsdev --name bronze --auth-mode login
az storage container create --account-name ttadlsdev --name silver --auth-mode login
az storage container create --account-name ttadlsdev --name gold --auth-mode login
az storage container create --account-name ttadlsdev --name configs --auth-mode login
```

#### 3. Set Up Key Vault
```powershell
# Create Key Vault
az keyvault create --name tt-healthcare-kv --resource-group tt-healthcare-rg --location eastus

# Add Secrets
az keyvault secret set --vault-name tt-healthcare-kv --name "tt-adls-access-key-dev" --value "YOUR_STORAGE_ACCESS_KEY"
az keyvault secret set --vault-name tt-healthcare-kv --name "sqldb-pwd" --value "YOUR_SQL_PASSWORD"
```

#### 4. Configure Databricks
- Create secret scope linked to Key Vault
- Import notebooks from `/notebooks/` directory
- Run mount notebook to connect to ADLS
- Create audit database and tables

#### 5. Configure ADF
- Create Linked Services for:
  - Azure SQL Database (Hospital A & B)
  - Azure Databricks
  - Azure Key Vault
  - ADLS Gen2
- Import pipelines from `/pipelines/` directory
- Upload `load_config.csv` to configs container
- Trigger pipeline



---

## Key Performance Indicators (KPIs)

### 1. Accounts Receivable Aging
```sql
SELECT 
    CASE 
        WHEN DATEDIFF(current_date, transaction_date) <= 30 THEN '0-30 days'
        WHEN DATEDIFF(current_date, transaction_date) <= 60 THEN '31-60 days'
        WHEN DATEDIFF(current_date, transaction_date) <= 90 THEN '61-90 days'
        ELSE '90+ days'
    END AS aging_bucket,
    SUM(charge_amt - paid_amt) AS total_ar
FROM gold.fact_transactions
WHERE is_current = TRUE
GROUP BY aging_bucket
ORDER BY aging_bucket;
```

### 2. Days in AR
```sql
-- Days in AR = (Total AR / Average Daily Revenue)
WITH daily_revenue AS (
    SELECT 
        transaction_date,
        SUM(paid_amt) AS daily_paid
    FROM gold.fact_transactions
    WHERE transaction_date >= DATEADD(month, -3, current_date)
    GROUP BY transaction_date
)
SELECT 
    (SELECT SUM(charge_amt - paid_amt) FROM gold.fact_transactions WHERE is_current = TRUE) / 
    AVG(daily_paid) AS days_in_ar
FROM daily_revenue;
```

### 3. Net Collection Rate
```sql
SELECT 
    d.provider_name,
    d.department_name,
    SUM(f.charge_amt) AS total_charges,
    SUM(f.paid_amt) AS total_paid,
    (SUM(f.paid_amt) / SUM(f.charge_amt)) * 100 AS net_collection_rate
FROM gold.fact_transactions f
JOIN gold.dim_provider d ON f.FK_Provider_Key = d.Provider_Key
WHERE f.transaction_date >= DATEADD(month, -1, current_date)
GROUP BY d.provider_name, d.department_name
ORDER BY net_collection_rate DESC;
```

### 4. Revenue by Department
```sql
SELECT 
    d.department_name,
    SUM(f.charge_amt) AS total_charges,
    SUM(f.paid_amt) AS total_paid,
    SUM(f.charge_amt - f.paid_amt) AS outstanding_ar
FROM gold.fact_transactions f
JOIN gold.dim_department d ON f.FK_Department_Key = d.Department_Key
WHERE f.transaction_date >= DATEADD(month, -1, current_date)
GROUP BY d.department_name
ORDER BY outstanding_ar DESC;
```

---

##  Conclusion

This Azure Healthcare RCM ETL project demonstrates data engineering solution that:

1. Solves real healthcare problems by making unpaid balances visible and measurable
2. Implements industry best practices (Medallion Architecture, SCD Type 2, CDM)
3. Handles complex data integration challenges (schema variance, ID collisions)
4. Enables critical financial KPIs for Revenue Cycle Management
5. Follows security best practices with Azure Key Vault and Unity Catalog

---
