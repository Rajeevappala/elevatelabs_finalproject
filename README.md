# ETL Project – Online Retail Data Warehouse  

## 📌 Project Overview  
This project demonstrates the **ETL (Extract, Transform, Load)** process using SQL Server with the **Online Retail Dataset**.  
The goal is to build a **data warehouse** that supports reporting and analytics by transforming raw retail data into a **star schema** with dimensions and fact tables.  

The repository includes:  
- SQL scripts for ETL pipeline  
- A report explaining the project  
- Source CSV dataset  

---

## 🛠 Tools & Technologies  
- **SQL Server**  
- **T-SQL (DDL, DML, CTEs, Triggers, Stored Procedures)**  
- **CSV dataset**  

---

## 🔄 ETL Process  

### 1. Extract  
- Data is imported from the CSV file (`Online Retail.csv`) into the **Raw_OnlineRetail** table using `BULK INSERT`.  

### 2. Transform  
- Data type conversions (`TRY_CAST` for `Quantity`, `UnitPrice`, `InvoiceDate`).  
- Cleaning operations:  
  - Removal of duplicates  
  - Removal of rows with null critical fields (`InvoiceNo`, `StockCode`, `Quantity`, `UnitPrice`).  
- Creation of **Staging_OnlineRetail** for clean, structured data.  

### 3. Load  
- **DimCustomer** – Contains customer information with latest country mapping.  
- **DimProduct** – Contains unique products with descriptions.  
- **DimDate** – Date dimension generated from min to max invoice dates.  
- **FactSales** – Sales fact table referencing dimension keys, storing quantity, unit price, and computed total amount.  
- **AuditLog** – Captures number of rows inserted into fact tables with automated logging via triggers.  

---

## 📊 Star Schema Design  

      DimCustomer
           |
      FactSales --- DimProduct
           |
       DimDate


- **FactSales** (central fact table) references **DimCustomer**, **DimProduct**, and **DimDate**.  
- Enables efficient querying for reporting (e.g., sales by customer, country, product, or time).  

---

## ✅ Features  
- Data Cleaning (duplicates, nulls, type casting)  
- Slowly Changing Dimensions (latest country for customers)  
- Automated Date Dimension generation  
- Audit Logging for monitoring ETL loads  
- Star Schema Design for analytics  

---

## 📂 Repository Structure  

- 📜 final project.sql # SQL script with ETL pipeline
- 📜 Online Retail.csv # Dataset
- 📜 SQL ETL Pipeline Simulation Report.pdf # Report describing the project
- 📜 README.md # Documentation

## 📝 Author

👤 Sai Rajeev Appala


