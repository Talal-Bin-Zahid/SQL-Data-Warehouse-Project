# 🚀 Data Warehouse and Analytics Project: Sales Data Platform

This repository showcases a comprehensive, end-to-end **Data Warehouse and Analytics Project** built using the **Medallion Architecture (Bronze, Silver, Gold layers)**. It is designed to consolidate sales data from multiple source systems, cleanse and standardize it, and present it in an optimized **Star Schema** for advanced analytics and informed decision-making.

## 🎯 Project Overview
<img width="814" height="536" alt="Screenshot 2025-11-09 123347" src="https://github.com/user-attachments/assets/7759ed97-6abc-42cc-8e3e-cccf0e123f48" />


This project serves as a portfolio piece demonstrating expertise in modern data engineering, data modeling, and business intelligence (BI).

| Aspect | Description |
| :--- | :--- |
| **Objective** | Develop a reliable, scalable data warehouse to enable detailed reporting on **Sales Trends**, **Customer Behavior**, and **Product Performance**. |
| **Architecture** | **Medallion Architecture** (Bronze, Silver, Gold Layers). |
| **Data Model** | **Star Schema** in the Gold Layer. |
| **Data Scope** | Focus on the **latest dataset only**; historization is excluded from the scope. |
| **Technology** | **SQL Server** for Data Warehouse management and **SQL** for all transformations. |

## 🏗️ Data Warehouse Architecture (Medallion Pattern)

The project utilizes the Medallion Architecture, providing structure and rigor to the data ingestion and transformation process.

### 1. Bronze Layer (Raw Data)
* **Purpose:** Ingest raw, untransformed data directly from source systems.
* **Object Type:** Tables.
* **Data Model:** None (as-is).
* **Load Type:** Batch Processing, Full Load (Truncate & Insert).

### 2. Silver Layer (Clean & Standardized Data)
* **Purpose:** Cleanse, standardize, and normalize the raw data, resolving quality issues and unifying formats.
* **Object Type:** Tables.
* **Transformations:** Data Cleaning, Data Standardization, Data Normalization, Derived Columns (e.g., `cat_id` from `prd_key`), Data Enrichment.
* **Data Model:** None (as-is, but cleaned).

### 3. Gold Layer (Business-Ready Data)
* **Purpose:** Final layer modeled for consumption, optimized for reporting and analytical queries.
* **Object Type:** Views (to expose the final model).
* **Transformations:** Data Integrations (combining CRM & ERP sources), Aggregations, Business Logics (e.g., Sales Calculation: `Quantity * Price`).
* **Data Model:** **Star Schema** (Fact & Dimension Tables).


---

## 🌊 Data Flow Diagram
<img width="768" height="544" alt="Screenshot 2025-11-11 021652" src="https://github.com/user-attachments/assets/e83a0d9c-df8f-4afc-af28-7b7e54b6f8d1" />


The ETL pipeline extracts data from two primary source systems, moves it through the Bronze and Silver layers, and finally integrates it into the dimension and fact tables of the Gold Layer.

### 📂 Source Systems
1.  **CRM (Customer Relationship Management):** Contains sales transactions, customer demographics, and product details.
2.  **ERP (Enterprise Resource Planning):** Contains supplementary customer details (e.g., birthdate, location) and product category information.

### 🔄 Data Transformation Path
The project integrates tables from both sources into a unified dimensional model:

| Bronze Table | Silver Table (Cleaning/Standardization) | Gold Table (Integration/Modeling) |
| :--- | :--- | :--- |
| `crm_cust_info` | `silver.crm_cust_info` | `gold.dim_customers` |
| `erp_cust_az12` | `silver.erp_cust_az12` | `gold.dim_customers` |
| `erp_loc_a101` | `silver.erp_loc_a101` | `gold.dim_customers` |
| `crm_prd_info` | `silver.crm_prd_info` | `gold.dim_products` |
| `erp_px_cat_g1v2` | `silver.erp_px_cat_g1v2` | `gold.dim_products` |
| `crm_sales_details`| `silver.crm_sales_details`| `gold.fact_sales` |


---

## 🌟 Gold Layer Data Model (Star Schema)

The Gold layer uses a **Star Schema**, which is highly efficient for analytical queries, consisting of one central Fact table linked to multiple Dimension tables.

### 💰 `gold.fact_sales` (Fact Table)
* **Grain:** One row per product within a specific order.
* **Key Metrics:** `sales_amount`, `quantity`, `price`.
* **Foreign Keys:** `product_key`, `customer_key`.

### 🧍 `gold.dim_customers` (Dimension Table)
* **Grain:** One row per unique customer.
* **Attributes:** Consolidated attributes from both CRM (`cst_firstname`, `cst_marital_status`) and ERP (`birthdate`, `country`).

### 🛍️ `gold.dim_products` (Dimension Table)
* **Grain:** One row per unique product.
* **Attributes:** Combined product details from CRM (`product_name`, `product_line`) and ERP (`category`, `subcategory`, `maintenance`).

> **Sales Calculation Logic:**
> The core metric is calculated within the BI layer or the Fact View: `Sales = Quantity * Price`.


---<img width="831" height="518" alt="Screenshot 2025-11-11 012313" src="https://github.com/user-attachments/assets/b7e548e2-379e-4617-82e8-42d93582f13b" />


## 🛠️ Key SQL Code Components

The repository contains SQL scripts that implement the entire pipeline:

### 1. Database Setup
* `create_db_schemas.sql`: Scripts to create the `datawarehousedb` database and the three schemas: `bronze`, `silver`, and `gold`.

### 2. Bronze Layer Load (ETL: Extraction & Loading)
* **DDL:** Creates all **Bronze tables** (`bronze.crm_cust_info`, etc.) with their raw data types.
* **Stored Procedure (`bronze.load_bronze`):** Orchestrates the data ingestion process:
    * `TRUNCATE` tables for full load.
    * `BULK INSERT` data from external CSV files (simulating extraction from sources).

### 3. Silver Layer Transformation (ETL: Transformation & Cleaning)
* **DDL:** Creates all **Silver tables** with adjusted data types and an audit column (`dwh_create_date`).
* **Cleaning & Standardization:** Scripts perform critical data quality checks and transformations:
    * **Data Deduplication:** Using `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` to select the latest/primary record (e.g., in `crm_cust_info`).
    * **Data Trimming:** Removing unwanted spaces (`TRIM()`).
    * **Data Consistency:** Standardizing values (e.g., converting 'S'/'M' to 'Single'/'Married' for gender/marital status).
    * **Date Repair/Handling:** Using `LEAD()` window function to correctly calculate `prd_end_dt` for versioned products and fixing bad date entries.
    * **Data Derivation:** Extracting `cat_id` from `prd_key` using string manipulation functions.

### 4. Gold Layer Data Modeling (Integration & Business Logic)
* *Future Scripts (To be developed/included)*: DDL and DML scripts to build the final `gold.dim_customers`, `gold.dim_products`, and `gold.fact_sales` tables/views by joining and aggregating data from the Silver Layer.

---<img width="844" height="530" alt="Screenshot 2025-11-11 010155" src="https://github.com/user-attachments/assets/144eed9a-e010-4bd8-af98-8bb73cb681c0" />


## 📈 Analytics & Reporting Objectives

The final Gold Layer model enables analysts to answer key business questions and generate actionable insights:

| Focus Area | Key Metrics / Reporting Examples |
| :--- | :--- |
| **Customer Behavior** | Segmentation by `marital_status` or `country`, analysis of sales by `birthdate` (age group), and identifying top-spending customers. |
| **Product Performance**| Reporting sales volume and revenue by `category`, `subcategory`, and `product_line`; analyzing product `cost` vs. `price` for profitability. |
| **Sales Trends** | Analyzing sales by `order_date`, identifying peak shipping/due date periods, and tracking trends over time. |

## 💡 How to Use This Repository

This project is structured for review by potential employers or collaborators interested in:

* **Reviewing Data Architecture:** Inspecting the implementation of the Medallion Pattern.
* **Evaluating ETL/ELT Logic:** Examining the SQL code used for data cleaning and transformations in the Silver layer.
* **Understanding Data Modeling:** Analyzing the Star Schema design and its suitability for analytics.

---
