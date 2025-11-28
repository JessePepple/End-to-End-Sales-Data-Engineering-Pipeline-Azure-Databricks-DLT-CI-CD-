# End-to-End-Sales-Data-Engineering-Pipeline-Azure-Databricks-DLT-CI-CD-

This project demonstrates a complete Azure Data Engineering solution that processes data from ingestion to transformation and delivery using modern cloud-native best practices. The architecture leverages Azure Data Factory, Azure Databricks, and Delta Live Tables (DLT) to build a scalable, reliable, and production-ready data pipeline.

The solution is designed to handle incremental ingestion, schema evolution, CDC, and dimensional modelling, making it suitable for real enterprise workloads.

<img width="1444" height="902" alt="Sales_Data" src="https://github.com/user-attachments/assets/027a07fb-08a5-40a3-990d-63559010e330" />


## Problem Statement

Many organisations struggle with data that sits across multiple systems, arrives in different formats, and updates frequently. Traditional pipelines are often brittle, manual, and difficult to scale.

The challenge was to build a pipeline that:

Handles continuous ingestion from source systems

Automatically adapts to schema drift

Supports Change Data Capture (CDC)

Delivers curated data models for analytics

Ensures reliability, auditability, and reproducibility

This project solves those challenges using a modern lakehouse approach.

## Architecture
Key Components

Azure Data Factory (ADF) – Orchestrates ingestion and schedules pipeline runs

Azure Databricks – Performs scalable compute & transformation

Autoloader – Efficient streaming ingestion with schema evolution

Delta Lake – Provides ACID transactions, time travel & reliability

Delta Live Tables (DLT) – Manages SCDs, data quality & lineage

Bronze → Silver → Gold layered architecture

High-Level Flow

Ingestion (ADF → Databricks)

ADF pulls raw data from external sources (API, Blob, SQL DB)

Data is landed as raw files in Bronze through Databricks Autoloader

Bronze Layer (Raw Storage)

Stores unprocessed raw data

Handles schema inference & automatic file discovery

## Silver Layer (Cleansed & Enriched)

Data Transformation

Deduplication

Incremental load logic

CDC handling using metadata columns

## Gold Layer (DLT Managed Tables)

Dimensions and fact tables

Slowly Changing Dimensions (SCD Type 2)

Surrogate key generation

Business-ready data models

## Consumption

Dashboards (Power BI)

Downstream applications

Finance/ops analytics

## Phase 1 Data Ingestion

The project began by setting up a Git repository to enable version control and streamlined deployment of the Azure Data Factory artifacts. A dedicated development branch was then created to manage feature enhancements and isolate ongoing updates from the main production branch.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 08 50" src="https://github.com/user-attachments/assets/ab0f7047-943e-4f3e-a35a-2652783f53dd" />

During the ingestion phase, data was sourced from a website and retrieved through a REST API. The pipeline was fully parameterized in Azure Data Factory and executed using a ForEach activity to handle multiple API calls dynamically. The ingested data was then written into Azure Data Lake Storage, along with a timestamp column to what data context this new source is for auditability and downstream processing.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 09 45" src="https://github.com/user-attachments/assets/fc0b4639-e960-4ce9-9478-c2ee951c7853" />

As illustrated in the diagram, the data source was first validated using a Web Activity. Once validation was successful, the ingestion process was initiated with parameterized ForEach activities to handle dynamic data retrieval. Upon completion of the ingestion, a pull request was created, and the changes were reviewed and merged into the main branch to maintain version control and ensure production readiness.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 11 12" src="https://github.com/user-attachments/assets/f42b142b-2ea3-4e06-9a59-24adff916f7a" />

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 13 19" src="https://github.com/user-attachments/assets/ac920ba2-2e54-44bc-a344-d7c5819f66ff" />


## Phase 2 Transformation

The transformation process for this project began with the initialization of our Databricks bundle project, setting up the environment and project structure required for scalable and maintainable data processing.

Next, I created external locations for the containers within our Data Lake, establishing secure and organized storage paths for raw, processed, and curated data.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 15 08" src="https://github.com/user-attachments/assets/9e50c5a9-fd0d-405f-bc97-657eda9aaf14" />
<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 17 14" src="https://github.com/user-attachments/assets/728de1bb-34fb-462b-8f41-ce27a961eea4" />
<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 18 07" src="https://github.com/user-attachments/assets/e8d07692-09f7-4d81-a4fb-312e468dde1d" />
<img width="1440" height="725" alt="Screenshot 2025-11-27 at 02 18 38" src="https://github.com/user-attachments/assets/27aa3ab6-b8a1-47da-ae7d-8b680e75ec8d" />

The transformation process leveraged Databricks Autoloaders to ensure idempotent processing and ingestion. The transformations primarily focused on casting each column to the correct data type to enforce the target schema. This required careful attention, as the datasets contained numerous columns with varying types, making precise schema casting essential for data integrity and downstream reliability. After transforming our data I loaded the enriched data to the datalake and began the creation of our slowly changing dimensional modelling using DLT in our curated layer
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 05 12 47" src="https://github.com/user-attachments/assets/55235277-f59e-4b0f-8278-03aa84f2032d" />

## Phase 3 Curated Gold Layer With DLT
For the curated (Gold) layer, I utilized Delta Live Tables (DLT) to implement a robust Lakehouse pipeline. Dimension tables followed a Slowly Changing Dimension (SCD) Type 2 pattern, while the fact table used SCD Type 1. Since the dimension tables were already well-prepared at the source, there was no need to generate new surrogate keys or perform additional modeling. The main focus was on creating an automated CDC flow to handle both SCD Type 1 and Type 2 changes. Using DLT, I also defined data quality expectations on key tables to ensure accuracy before finalizing the SCD Type 2 implementation. Once validated, the curated data was loaded into the SQL Data Warehouse for analytics and reporting. Finally, the curated tables were optimized with Z-ordering on their primary keys to improve query performance.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 43 45" src="https://github.com/user-attachments/assets/6619eea0-e0a8-410c-b4cf-4857a01ebf7f" />

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 43 52" src="https://github.com/user-attachments/assets/dbfc5304-5c07-4699-ab2b-953f27e06cf7" />


<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 44 04" src="https://github.com/user-attachments/assets/535635f3-ec16-437b-88e3-2bee7347181c" />
<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 44 12" src="https://github.com/user-attachments/assets/71095a02-bca0-4cbc-9c9e-142836fd3c53" />


<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 44 21" src="https://github.com/user-attachments/assets/8dd4716f-4188-4155-9407-ab7ba1bd297b" />

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 44 29" src="https://github.com/user-attachments/assets/ffe3396a-fc05-41a7-92f9-192f491c768d" />

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 44 37" src="https://github.com/user-attachments/assets/598fbc8a-fd5e-4284-9a4a-f51eb0a739a1" />

After performing a dry run and validating the data, I executed the DLT pipeline to apply the transformations and load the curated datasets into the Gold layer. Slowly Changing Dimension (SCD) Type 2 was implemented on the dimension tables to capture historical changes, while SCD Type 1 was applied to the fact tables to maintain only the latest state.

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 45 56" src="https://github.com/user-attachments/assets/c2df03f4-4d3d-4d55-8c97-1515b43eb678" />

<img width="1440" height="725" alt="Screenshot 2025-11-27 at 04 49 44" src="https://github.com/user-attachments/assets/35e7eb8e-b850-476f-af44-f3312a39f7f5" />

<img width="1440" height="695" alt="Screenshot 2025-11-27 at 04 50 10" src="https://github.com/user-attachments/assets/d7f385bc-a8b2-4b63-91ca-cd9522c9b7df" />


<img width="1396" height="673" alt="Screenshot 2025-11-28 at 21 31 45" src="https://github.com/user-attachments/assets/9bb9bdc9-614a-475c-84ba-6d0edc1f03ea" />



As shown in the diagram, our upsert logic executed successfully, and all defined data quality expectations were met.
<img width="1440" height="695" alt="Screenshot 2025-11-27 at 04 50 45" src="https://github.com/user-attachments/assets/68e5e45a-0852-4242-9142-0cd2b63fa4ae" />


## DataBricks SQL Warehouse
I validated the curated datasets in the Databricks SQL Warehouse and subsequently created sample dashboards to demonstrate the usability and analytical value of the curated data.

<img width="1440" height="644" alt="Screenshot 2025-11-27 at 04 52 26" src="https://github.com/user-attachments/assets/69286122-6657-47f8-88c6-48b59400fa01" />
<img width="1440" height="644" alt="Screenshot 2025-11-27 at 04 52 46" src="https://github.com/user-attachments/assets/490f6ef6-fa7f-4b12-a380-ef12885898a7" />
<img width="1440" height="644" alt="Screenshot 2025-11-27 at 04 53 34" src="https://github.com/user-attachments/assets/26e2847f-f3dd-4db0-b2b6-68de6a793ff9" />
<img width="1440" height="644" alt="Screenshot 2025-11-27 at 04 54 00" src="https://github.com/user-attachments/assets/5abdf1f7-9799-41ad-aa42-40e2a3e1941a" />
<img width="1440" height="644" alt="Screenshot 2025-11-27 at 04 53 03" src="https://github.com/user-attachments/assets/d963bece-9d7f-4414-9beb-2cd302041397" />

<img width="1440" height="722" alt="Screenshot 2025-11-27 at 04 55 29" src="https://github.com/user-attachments/assets/336a2e45-01da-41c2-ba81-d439410cbc24" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 04 56 05" src="https://github.com/user-attachments/assets/fca838f6-f47b-42c9-8640-3512f3e97564" />

<img width="1440" height="722" alt="Screenshot 2025-11-27 at 04 58 12" src="https://github.com/user-attachments/assets/ccc4ada8-6f03-4033-bdb2-a16e86d0756c" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 04 58 59" src="https://github.com/user-attachments/assets/7aac9bed-e173-45eb-9715-5431f5eff562" />


After completing the Silver and Gold layers, I successfully deployed the Databricks bundle to the designated GitHub repository for version control.
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 05 12 47" src="https://github.com/user-attachments/assets/55235277-f59e-4b0f-8278-03aa84f2032d" />

## BI Reporting
Leveraging Databricks Partner Connect, I provided a BI connector to data analysts, enabling them to directly query and visualize the cleaned data in Power BI without relying on the SQL Data Warehouse. Subsequently, I loaded the curated datasets into the Synapse Data Warehouse for additional analytics and reporting. Upon completing the project, I deployed all notebooks and pipelines to the PROD folder in Databricks using Databricks Asset Bundles, and version-controlled the project by pushing it to my GitHub repository.

<img width="1440" height="722" alt="Screenshot 2025-11-27 at 05 42 47" src="https://github.com/user-attachments/assets/e4388f20-07e3-41bd-904b-05a9a194692b" />


## Loading To Synapse

Once the data was successfully validated in the Databricks SQL Warehouse, I loaded the curated datasets into the Gold layer of our Data Lake, and subsequently pushed them to the Synapse Analytics Warehouse for enterprise-wide consumption and reporting.

<img width="1440" height="722" alt="Screenshot 2025-11-27 at 06 12 04" src="https://github.com/user-attachments/assets/28d3661c-3438-4ddc-b554-be23c56939d6" />

<img width="1440" height="722" alt="Screenshot 2025-11-27 at 06 55 48" src="https://github.com/user-attachments/assets/eafafe92-a801-43b8-a5b6-d833eafb5946" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 13 15" src="https://github.com/user-attachments/assets/8fdfc107-5599-49e9-8c97-92ae628df3c9" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 14 20" src="https://github.com/user-attachments/assets/a74694db-629b-4ec7-ac35-cfd016917b3e" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 14 39" src="https://github.com/user-attachments/assets/8d0243df-9c02-457e-8e42-a988b079c6a4" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 14 56" src="https://github.com/user-attachments/assets/cfeab348-3315-413c-8566-194c9c408cc7" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 15 14" src="https://github.com/user-attachments/assets/b0462093-366a-4de9-9ec0-3826b2d3c5ac" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 15 28" src="https://github.com/user-attachments/assets/35521079-333d-4a6a-a222-9df49aec2054" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 15 37" src="https://github.com/user-attachments/assets/5483a72b-016b-4fef-ae5f-2c9a8bc32fa2" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 16 08" src="https://github.com/user-attachments/assets/a34563d4-de52-40fa-897a-4d17452d04c2" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 16 29" src="https://github.com/user-attachments/assets/2d5b98b7-b60a-43f5-97d5-618477ced82f" />
<img width="1440" height="722" alt="Screenshot 2025-11-27 at 07 16 57" src="https://github.com/user-attachments/assets/152f87d1-90ae-43d6-aae6-02bbceb8545d" />

