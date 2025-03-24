# 📌 Data Migration: On-Prem SQL DB to Azure SQL DB

## 🚀 Project Overview
This project demonstrates **Incremental Data Migration** from an **On-Prem SQL Database** to **Azure SQL Database** using **Azure Synapse Analytics**. The data is first transferred to **Azure Data Lake Storage Gen2 (ADLS Gen2)** and then processed using different loading techniques before being stored in Azure SQL DB.

## 📂 Source Tables Used
The following five tables were used for this migration:
- 📚 **Books**  
- 👥 **Members**  
- 📖 **BorrowRecords**  
- 📦 **Inventory**  
- 🏢 **Librarians**  

## 🔄 Data Movement Process
1️⃣ **On-Prem SQL DB ➡ ADLS Gen2 Storage** (Incremental Load)  
2️⃣ **ADLS Gen2 Storage ➡ Azure SQL DB** (Using Azure Synapse Pipelines)  

## ⚙️ Data Loading Methodologies
Three different methods were used to process data from **ADLS Gen2** to **Azure SQL DB**:
- 🔹 **Normal Data Load** – For *BorrowRecords, Inventory, and Librarians*.
- 🔹 **SCD Type 1 (Slowly Changing Dimension – Type 1)** – For *Books* (Overwrites existing records).
- 🔹 **SCD Type 2 (Slowly Changing Dimension – Type 2)** – For *Members* (Keeps historical records).

## 🏗️ Pipeline Architecture
The **Azure Synapse Analytics Pipeline** consists of the following activities:

### 🔄 **Incremental Data Load: On-Prem SQL DB ➡ ADLS Gen2**
- **Set Variable Activity** – Stores file name with a timestamp 📌
- **Lookup Activities** (2x) –
  - One to fetch watermark table 📊
  - Another to find max value in source tables 🔍
- **Foreach Activity** – Iterates through tables fetched from the watermark lookup 🔄
- **Copy Activity** – Moves data from On-Prem SQL DB to ADLS Gen2 Storage 📂

### 🚀 **Data Processing & Load: ADLS Gen2 ➡ Azure SQL DB**
- **IF Condition Activity** – Ensures only non-SCD tables are loaded normally ⚡
- **Copy Activity** – Loads data from ADLS Gen2 to Azure SQL DB (for normal tables) 🔄
- **Stored Procedure Activity** – Updates the watermark table with the latest max value 🛠️
- **Data Flow Activity (2x)** – Implements **SCD Type 1 & Type 2** transformations 🚀

## 🏆 Key Takeaways
✅ **Incremental Data Loading** ensures efficient data migration 📊  
✅ **Optimized ETL process** using different data loading strategies ⚙️  
✅ **Robust Pipeline Architecture** with Azure Synapse Analytics 🔄  

## 🚀 How to Run the Project
### 1️⃣ Prerequisites
- Azure Subscription
- On-Prem SQL Server with required tables
- Azure Data Lake Storage Gen2
- Azure Synapse Analytics
- Azure SQL Database

### 2️⃣ Steps to Execute
1. Set up the **On-Prem SQL DB** and load the sample data.
2. Configure **Azure Data Factory / Synapse Pipeline** for incremental load.
3. Execute the pipeline to load data into **ADLS Gen2 Storage**.
4. Process the data into **Azure SQL DB** using different ETL strategies.
5. Verify the data using **SQL Queries**.

## 📧 Contact & Feedback
If you have any questions or feedback, feel free to connect with me on LinkedIn or open an issue in this repository. Let’s collaborate! 💬

#Azure #DataEngineering #AzureSynapse #SQL #ETL #CloudMigration #AzureDataFactory #ADLSGen2 #DataTransformation
