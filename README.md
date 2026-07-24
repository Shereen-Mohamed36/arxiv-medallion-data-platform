# 📚 arXiv Medallion Data Platform: End-to-End Research Analytics

An end-to-end Data Engineering pipeline designed to automate, process, and model academic research papers from the arXiv API using the **Medallion Architecture** (Bronze -> Silver -> Gold). This platform orchestrates the complete lifecycle of data—from automated programmatic ingestion to scalable transformation and business intelligence.

---

## Architecture & Data Flow

The platform is designed following industry-standard modular practices, ensuring decoupling between orchestration, transformation scripts, and local storage layers:

1. **Ingestion (Bronze Layer):** A custom Python script polls the arXiv API using chunked HTTP requests, downloading metadata for specific computer science categories (e.g., `cs.AI`, `cs.LG`, `cs.CV`) into raw CSV format.
2. **Transformation & Cleaning (Silver Layer):** PySpark processes the multi-line raw data, cleanses text configurations (removing white spaces, tabs, and newlines), enforces precise schemas, and computes analytical text metrics.
3. **Dimensional Modeling (Gold Layer):** The cleaned data is mapped into a relational **Star Schema** optimized for analytical queries. Data is incrementally loaded into PostgreSQL while applying deduplication checks.
4. **Orchestration:** Apache Airflow manages and monitors the complete execution graph via containerized tasks.
5. **Visualization:** Power BI connects directly to the final Gold PostgreSQL tables to deliver interactive dashboards showcasing trends and collaboration matrices.
<img width="1721" height="993" alt="data flow" src="https://github.com/user-attachments/assets/c0968c6b-81ce-4569-a8e8-cb3fa8adb265" />

---

## 🛠️ Tech Stack & Component Breakdown

| Technology | Role & Function in the Platform |
| :--- | :--- |
| **Python (urllib & XML)** | **Programmatic Web Ingestion:** Communicates with the arXiv API to handle paginated XML responses, parses nested nodes (authors, categories, links), and flattens them securely. Includes built-in rate-limiting (`time.sleep`) to respect server policies. |
| **Apache Airflow** | **Workflow Orchestration:** Schedules the execution, monitors task status, tracks dependencies, and provides failure alerting/retries for production reliability. |
| **Apache Spark (PySpark)** | **Distributed Computation:** Processes heavy textual metadata at scale, executes data parsing (e.g., splitting comma-separated author columns into arrays), and models the relational structures. |
| **PostgreSQL** | **Relational Data Warehouse:** Serves as the central analytical storage repository, hosting the final Star Schema (Fact and Dimension tables) with optimized integrity. |
| **Docker & Docker Compose** | **Containerized Infrastructure:** Packages Airflow and PostgreSQL into isolated environments, eliminating local configuration mismatch issues and streamlining deployment. |
| **Power BI** | **Business Intelligence:** Consumes the Gold database layer to generate network visualizations of active authors, publication timelines, and domain category growth. |

---

## 🗂️ Project Directory Structure

```text
researchPipline/
│
├── dags/                           # Apache Airflow DAG definitions
│   └── arxiv_pipeline.py           # Core pipeline orchestration script
│
├── scripts/                        # Modular transformation and ingestion logic
│   ├── fetch_data.py               # Ingestion script (arXiv API parsing to Bronze)
│   └── spark.py                    # PySpark extraction, transformation, and Gold loading
│
├── power_bi/                       # Power BI dashboard storage
│   └── arxiv_analysis.pbix         # Interactive reporting file
│
├── diagrams/                       # Pipeline design blueprints
│   ├── data_flow.png               # Step-by-step pipeline component diagram
│   └── dashboard_screenshot.png    # Dashboard preview image
│
├── docker-compose.yaml             # Multi-container multi-service runtime configuration
│
└── README.md                       # platform documentation
```
## 📊 Relational Data Modeling (Star Schema)

To maximize query performance and optimize the data layer for Power BI, the Gold layer inside PostgreSQL is architected using a traditional **Star Schema** with explicit relational integrity:

### 1. Dimension Tables

| Table Name | Column | Data Type | Key / Constraint | Description |
| :--- | :--- | :--- | :--- | :--- |
| **`dim_categories`** | `category_id` <br> `category_name` | SERIAL <br> VARCHAR | **PK** <br> Unique, Not Null | Unique identifier for arXiv computer science domains. |
| **`dim_authors`** | `author_id` <br> `author_name` | SERIAL <br> VARCHAR | **PK** <br> Not Null | Cleaned and deduplicated academic author profiles. |
| **`dim_time`** | `date_id` <br> `full_date` <br> `day` <br> `month` <br> `year` <br> `quarter` | INT <br> DATE <br> INT <br> INT <br> INT <br> INT | **PK** <br> Not Null <br> - <br> - <br> - <br> - | Time dimension derived from `Publish_Date_Parsed` (Format: YYYYMMDD). |

### 2. Fact Table

| Table Name | Column | Data Type | Key / Constraint | Description |
| :--- | :--- | :--- | :--- | :--- |
| **`fact_papers`** | `paper_id` <br> `title` <br> `abstract` <br> `publish_date_id` <br> `category_id` <br> `title_word_count` <br> `abstract_word_count` <br> `author_count` | VARCHAR <br> TEXT <br> TEXT <br> INT <br> INT <br> INT <br> INT <br> INT | **PK** <br> - <br> - <br> **FK** (`dim_time.date_id`) <br> **FK** (`dim_categories.category_id`) <br> Metric <br> Metric <br> Metric | Core fact table storing SHA-256 unique paper hashes, parsed text content, relational keys, and computed text metrics. |

## 📉 Business Intelligence & Analytics Dashboard

The Gold relational layer in PostgreSQL is natively connected to Power BI to monitor domain growth, publishing velocities, and co-authorship collaboration metrics.

### Dashboard Preview
<img width="598" height="338" alt="data analytics" src="https://github.com/user-attachments/assets/07430ab7-cc32-48ec-9524-d0a24323acca" />


### Accessing the Report
* The production Power BI file can be found here: [`power_bi/arxiv_analytics_dashboard.pbix`](power_bi/arxiv_analytics_dashboard.pbix)
* Open the file using Power BI Desktop and update the PostgreSQL Database credentials under *Data Source Settings* to refresh with your local data.
