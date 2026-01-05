# Dataset & Data Modeling

This section documents the **EPC dataset**, the **data contract assumptions**, and the **key modeling strategies** used to transform raw source data into analytics-ready tables.

---

## Dataset Overview

* **Source:** UK Energy Performance Certificates (EPC) – England & Wales
* **Provider:** Department for Levelling Up, Housing & Communities
* **Ingestion Mode:** Monthly batch ingestion via public EPC API
* **Delivery Format:** ZIP archives containing CSV files
* **Domains Covered:**

  * Domestic Certificates
  * Non-Domestic Certificates
  * Display Energy Certificates
  * Corresponding Recommendations datasets

The official dataset schema and column definitions are documented here: **[https://epc.opendatacommunities.org/docs/csvw](https://epc.opendatacommunities.org/docs/csvw)**

---

## Data Contract (Project-Level)

Given the nature of the source data, the project follows a pragmatic data contract rather than a strict schema contract.

#### Contract Assumptions

* Column names and semantic meaning are stable across monthly releases
* Natural identifiers (e.g., UPRN, Building Reference Number) are *present but not strictly enforced*
* Source data does **not** provide:

  * Primary keys
  * Explicit versioning or change indicators

#### Implications

* Raw data is treated as **append-only**
* Historical changes must be inferred downstream
* Integrity enforcement is shifted to the transformation layer

---

## Data Modeling and Challenges

The EPC dataset introduces several modeling challenges:

* **No enforced uniqueness** at the source
* **Natural keys are unstable** (same property can appear multiple times with changes)
* **Multiple datasets** represent the same real-world entity (property) with partial overlap
* **Snowflake constraints** (PK/FK/UNIQUE) are informational and not enforced at runtime

These constraints require defensive modeling and explicit deduplication strategies.

The following image shows the data modeling that was arrived at,
<p align="center">
  <img src="docs/PBI_Data_Model.png" alt="Power BI Data Model" width="1000"/>
</p>

---

## Combined Property Modeling Strategy

To create a unified `dim_property` model:

* Domestic, Non-Domestic, and Display certificates are first standardized in staging models
* Records are **unioned** into a single combined stream
* Incremental logic ensures only new or changed records are processed per run
* Deduplication is applied using window functions ordered by ingestion timestamp

This approach allows consistent property modeling across heterogeneous EPC domains.

---

## Surrogate Key & Business Key Strategy

Because no reliable primary key exists at the source, two keys are introduced:

#### Property Identity Key (Surrogate Key)

* Used as the **technical unique key**
* Derived using:

  * `UPRN` where available
  * Fallback to an **MD5 hash of normalized address fields** when UPRN is missing
* Guarantees deterministic, repeatable identity across runs

#### Property Business Key

* Represents the **business-level state** of a property
* Generated as an MD5 hash of descriptive attributes (location, property type, tenure, etc.)
* Used to detect meaningful attribute changes over time

The provided dbt model implements this logic by:

* Normalizing text fields (lowercasing, trimming, removing spaces)
* Hashing a stable combination of attributes
* Ranking records per property by latest ingestion timestamp

---

## Snapshotting Strategy (SCD Type 2)

* **dbt snapshots** are used to track historical changes in property attributes
* Snapshot configuration:

  * `strategy: check`
  * **Unique key:** `property_identity_key`
  * **Change detection:** based on `property_bk` (business key)
* A downstream view exposes only the **currently active record** for ease of consumption

This enables full change history while keeping analytics queries simple.

---

## Data Quality & Integrity Enforcement

Since Snowflake does not enforce relational constraints, integrity is maintained using **dbt tests**.

#### Tests Implemented

* `unique`
* `not_null`
* `relationships`
* `expect_column_distinct_values_to_be_in_set`
* `expect_column_values_to_be_between`

These tests act as the **runtime enforcement layer** for the data contract and prevent silent data drift.

---

## Open Assumptions & Trade-offs

#### Assumptions

* EPC schema remains backward-compatible
* Address-based hashing provides sufficient uniqueness when UPRN is missing
* MD5 collision risk is negligible for this domain

#### Trade-offs

* Increased storage due to snapshotting
* Additional compute cost for hashing and deduplication
* Stronger correctness and auditability guarantees

---

#### Next Step

Detailed schema mappings and dbt SQL are intentionally excluded from this page for readability, they are available in:

* [dbt models](dbt/dbt_epc/models) 
* [Source documentation](https://epc.opendatacommunities.org/docs/csvw)