# End-To-End Azure Data Engineering Project

A comprehensive Data Engineering solution built on Microsoft Azure, demonstrating modern data pipeline architecture with incremental processing and dimensional data modeling.



##  Table of Contents

- Project Overview
- Architecture
- Technologies Used
- What You'll Learn
- Key Components
- Data Flow



## Project Overview

This end-to-end data engineering project demonstrates building a complete data platform on Azure. The project extracts data from SQL Server, processes it through multiple layers (Bronze → Silver → Gold), and creates a Star Schema dimensional model for analytics.



**Key Highlights:**
- Incremental data ingestion with watermark tracking
- Multi-layer medallion architecture
- Databricks Autoloader
- Star Schema dimensional modeling for analytics
- Delta Live Tables for real-time processing
- Azure Data Factory pipeline orchestration
- Git integration for version control and CI/CD

 

## 🏛️ Architecture

The project implements a three-layer architecture:

**Bronze Layer (Raw Data)**
- Raw data ingestion from Azure SQL Server
- Minimal transformation applied
- Current timestamp added to track load time
- Data stored as-is from source system

**Silver Layer (Cleaned Data)**
- Data deduplication and validation
- Null value handling and type conversions
- Business rule application
- Data quality checks implemented

**Gold Layer (Analytics Ready)**
- Star Schema dimensional model created
- Fact tables for business events
- Dimension tables for descriptive attributes
- Optimized for analytical queries and BI tools


<img width="1392" height="768" alt="download" src="https://github.com/user-attachments/assets/e6f55e44-e039-4f72-bfa1-4642ecb19599" />



## 🛠️ Technologies Used

- Data Source:     Azure SQL Server
- Orchestration:   Azure Data Factory (ADF)
- Processing:      Azure Databricks
- Language:        PySpark, Python
- Templating:      Jinja2
- Storage:         Azure Data Lake (ADLS Gen2), Web Storage
- Streaming:       Databricks Autoloader, Delta Live Tables
- Data Format:     Parquet, Delta
- Version Control: Git/GitHub
- Cloud Platform:  Microsoft Azure


## 🎓 What You'll Learn

- Building incremental data pipelines with watermark patterns
- Multi-layer data architecture (Bronze, Silver, Gold)
- Azure Data Factory pipeline creation and scheduling
- PySpark data transformations and processing
- Star Schema dimensional modeling
- Databricks Autoloader
- Delta Lake and Delta Live Tables
- Azure Data Lake storage and management
- Databricks workspace setup and notebook development
- Git version control in data engineering projects
- Real-time and batch data processing patterns



##  Key Components

### Azure SQL Server Source

Azure SQL Server acts as the source system containing transactional data. Connection strings managed securely in Azure Key Vault. Firewall rules control access to the database.


<img width="1399" height="723" alt="Screenshot 2026-01-15 201851" src="https://github.com/user-attachments/assets/b406b31c-8609-4275-912d-90d14b48b5a0" />


### Azure Data Factory Pipeline

Azure Data Factory orchestrates the entire data workflow. Pipelines extract data from Azure SQL Server, validate the data, and load it into Azure Data Lake. ForEach activities handle multiple tables dynamically. The pipeline tracks execution metrics and handles errors gracefully.


 <img width="1919" height="901" alt="Screenshot 2026-01-16 013706" src="https://github.com/user-attachments/assets/84807f70-527a-4a04-8da1-4a09782f0bfb" />
 <img width="1919" height="920" alt="Screenshot 2026-01-15 024844" src="https://github.com/user-attachments/assets/e5835f0d-087b-4ca6-9956-31e1ccffec37" />
 <img width="1919" height="901" alt="Screenshot 2026-01-16 013706" src="https://github.com/user-attachments/assets/b4ff2006-337c-4f03-9516-507168c10b8b" />

### Logic Apps Integration

Azure Logic Apps sends notifications when pipelines complete. Email alerts notify teams of successful executions or failures. Integration with Azure Data Factory webhooks enables automated workflow notifications.


<img width="1685" height="647" alt="Screenshot 2026-01-15 214238" src="https://github.com/user-attachments/assets/8d6f1d56-604d-4853-b45a-32f4a2a701ce" />


## Databricks Notebooks
### Databricks Autoloader

Notebooks contain PySpark code for data transformations. Bronze layer notebooks read raw SQL data and persist it with timestamps. Silver layer notebooks apply cleaning and validation logic. Gold layer notebooks create the Star Schema fact and dimension tables.


<img width="1919" height="912" alt="Screenshot 2026-01-19 065644" src="https://github.com/user-attachments/assets/e7d1e734-0bb8-480c-bf5e-c0e1b24aa7e8" />



### Delta Live Tables

Delta Live Tables provide a declarative approach to building data pipelines. They automatically handle dependencies between tables and enable data quality monitoring. The gold pipeline creates streaming tables that feed dashboards and reports.

<img width="1907" height="906" alt="Screenshot 2026-01-21 014708" src="https://github.com/user-attachments/assets/d8c754fc-49c3-473b-ba0f-565fa36a939f" />
<img width="332" height="246" alt="Screenshot 2026-01-21 002111" src="https://github.com/user-attachments/assets/98a5e219-2540-4f00-8bb8-f7d3587d48f4" />



### Jinja2 Templates

Dynamic SQL query generation using Jinja2 templating. Configuration files define table mappings and transformation rules. Templates reduce code duplication and improve maintainability across multiple data sources.


<img width="1902" height="922" alt="Screenshot 2026-01-20 150745" src="https://github.com/user-attachments/assets/94b617a6-db4a-4063-b544-970108a5dcad" />

<img width="464" height="334" alt="Screenshot 2026-01-20 141921" src="https://github.com/user-attachments/assets/079e62b6-ebd2-49ca-a02e-8828ee955b60" />


##  Data Flow

**Source Extraction**
Data is extracted from Azure SQL Server using Azure Data Factory. Incremental loads use watermark columns to track changed data. Only new and modified records are processed in subsequent runs.

**Bronze Layer Landing**
Raw data lands in Azure Data Lake in the bronze folder. Current timestamp is added to identify when data was loaded. Original source structure is preserved without transformation.

**Silver Layer Transformation**
PySpark jobs read bronze layer data and apply transformations. Duplicate records are removed based on business keys. Data types are validated and null values are handled. Business rules are applied and data is enriched.

**Gold Layer Star Schema**
Fact and dimension tables are created from silver layer data. The Star Schema structure consists of central fact tables connected to dimension tables. Slowly changing dimensions track historical attribute changes. Data is optimized for analytical queries.



##  Data Models

### Star Schema Implementation

**Fact Tables**
Contain measures and metrics from business events. Store transactional data at the lowest level of granularity. Use foreign keys to connect to dimension tables.

**Dimension Tables**
Provide context and descriptive information for facts. Enable filtering, grouping, and sorting in analytical queries. Support slowly changing dimension logic for historical tracking.

**Slowly Changing Dimensions (SCD Type 2)**
Track changes in dimension attributes over time. Maintain effective dates and end dates for historical records. Enable period-over-period analysis and trend identification.



##  Project Outcomes

**Data Pipeline Automation**
Automated end-to-end data pipelines reduce manual effort and improve reliability. Scheduled execution ensures data freshness. Error handling and retry logic ensure robust operations.

**Incremental Processing**
Watermark-based incremental loads optimize performance and reduce costs. Only changed data is processed in each pipeline run. Historical tracking maintains data lineage.

**Dimensional Modeling**
Star Schema provides an intuitive structure for business analysis. Dimension tables enable rich context for fact analysis. Query performance is optimized through denormalization.

**Version Control Integration**
Git integration enables collaborative development and code review. CI/CD pipelines automate testing and deployment. Code changes are tracked and auditable.



##  Resources

- [Azure Data Factory Documentation](https://docs.microsoft.com/azure/data-factory/)
- [Databricks Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Delta Lake Guide](https://delta.io/)
- [Azure SQL Database](https://docs.microsoft.com/azure/azure-sql/)
- [Azure Data Lake Storage](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)



## 🤝 Contributing

This project welcomes contributions and improvements from the community.


## 📝 License

MIT License - See LICENSE file for details


## 👨‍💼 Author

**Mohd Hanzala**

Created: January 2026  
Status:  Complete and Functional



**Last Updated**: January 2026
