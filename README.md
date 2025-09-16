🚀 **SQL Data Warehouse with Medallion Architecture** 🚀

This repository demonstrates a modern Data Warehouse solution built entirely with SQL, following the Medallion Architecture (Bronze → Silver → Gold). 
The project showcases my ability to design, build, and optimize end-to-end ETL pipelines and analytical data models.

📊 **Project Overview**

**Architecture:** Medallion (Bronze, Silver, Gold layers)

<p align="center">
  <img width="1000" alt="SQL Data Warehouse Architecture" src="https://github.com/user-attachments/assets/4909e196-3b49-4fca-b061-e7e48b812c0f" />
</p>

**Source Data** (AI-Generated):

  transactions_2mo.csv → transaction-level data
  
  events_2mo_fallback.csv → customer event logs
  
  customers_profile.json → customer demographic & profile info

**Database:** SQL Server (procedural ETL using Stored Procedures)

**Deliverables**: Cleaned, business-ready star schema for analytics and BI dashboards


🏗️ **Data Architecture**

Here’s the high-level design of the warehouse:

  **Bronze**: Raw ingestion of CSV/JSON into staging tables

  **Silver**: Cleaned, typed, standardized tables with deduplication & JSON flattening

  **Gold**: Star schema with Fact & Dimension tables (transactions, events, customers, dates)

  **Outputs**: Power BI / Tableau dashboards, ML-ready datasets, and ad-hoc SQL queries
  

  📈 **Business Use Cases**

  **Customer 360° View**: Track customer activity, events, and transactions

  **Churn Analysis**: Identify drop-off patterns from event logs and transaction gaps

  **Revenue Insights**: Aggregate sales by customer segments, merchants, and time periods

  **Event Funnels**: Analyze customer behavior across event types
