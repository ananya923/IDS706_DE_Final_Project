# Unusual Suspects: Ethereum Transaction Fraud Detection
Data Engineering Pipeline for Course IDS706, MS Data Science, Duke University

## Project Overview

This project implements an end-to-end data engineering pipeline for detecting fraudulent Ethereum transactions using real blockchain data from Etherscan.io. The system ingests transaction data via Etherscan APIs, stores it in AWS S3, performs ETL transformations and feature engineering using AWS Glue and PySpark, builds fraud detection models, stores results in Amazon S3, and visualizes insights through an interactive Streamlit dashboard.

The pipeline demonstrates practical cloud-based data engineering for blockchain analytics, covering data ingestion, distributed processing, machine learning, and real-time visualization.

## Team and Contributions

This project was developed collaboratively by five data engineers from November 12 to December 5, 2025. They performed the following roles:

- Project Manager/Team Lead: Overall project coordination, system architecture design, timeline management, and communication.
- ML Engineer: Isolation Forest model development, performance evaluation, feature engineering, and threshold optimization for anomaly detection.
- Backend Developer: AWS infrastructure management,Etherscan API integration, MySQL database design, batch processing pipeline, and automated scheduling implementation.
- Frontend Developer: Streamlit dashboard development, data visualization, user interface design, and real-time alert display systems.
- DevOps Engineer: README documentation, Docker containerization, CI/CD pipeline setup, and system monitoring.  

All team members contributed through regular commits, code reviews, and collaborative problem-solving throughout the project lifecycle.


## Architecture

The fraud detection system follows a five-stage architecture:

### 1. Data Ingestion (Etherscan API → AWS S3)

The ingestion layer retrieves real Ethereum transaction data from Etherscan.io using API keys. The script queries specific wallet addresses and their transaction histories, then writes raw JSON responses to S3 buckets organized by date. This creates a data lake foundation where all incoming blockchain data lands before processing.

### 2. Feature Engineering (AWS Glue / PySpark)

The feature engineering stage transforms raw transaction data into model-ready features using PySpark on AWS Glue. This distributed processing handles large transaction volumes efficiently.

The pipeline creates features at both transaction and address levels:

- Transaction-level features: gas prices, transaction values in Wei and Ether, timestamp conversions, transaction types  
- Address-level aggregations: total transaction counts, cumulative sent/received amounts, average transaction values, transaction frequency patterns, unique counterparty addresses  
- Behavioral metrics: time between transactions, transaction velocity, value distribution statistics, sender–receiver ratios  

These features capture behavioral patterns that distinguish normal activity from potentially fraudulent transactions.

### 3. Fraud Detection Modeling (PySpark ML)

The modeling layer applies machine learning algorithms to engineered features to calculate fraud risk scores. Using PySpark MLlib, the system trains classification models that identify suspicious transaction patterns.

The model pipeline includes:

- Feature selection and preprocessing (normalization, missing value handling)  
- Training fraud detection classifiers  
- Model evaluation with metrics like precision, recall, and F1-score  
- Risk score generation for each transaction and address  
- Model persistence for production deployment  

Predictions are written back to S3 and loaded into RDS for dashboard consumption.

### 4. Data Storage and Querying (S3 / Athena / RDS)

Storage Architecture:

- S3 data lake: raw ingestion data, intermediate transformation outputs, and model results stored in Parquet format  
- AWS Athena: SQL queries against S3 data for ad-hoc analysis and exploration  
- Amazon RDS (PostgreSQL): structured storage for processed transactions, engineered features, and fraud scores, optimized for dashboard queries

The dual storage approach provides flexibility for both analytical queries (Athena) and application-level access patterns (RDS).

### 5. Visualization (Streamlit Dashboard)

The Streamlit web application connects to RDS and presents fraud detection results through a visual interface. Users can explore:

- Real-time fraud risk scores for monitored Ethereum addresses  
- Transaction history with detailed feature breakdowns  
- Temporal patterns showing transaction volume and risk trends over time  
- Filtering by address, date range, transaction value, and risk level  
- Downloadable reports for compliance and investigation teams  

The dashboard updates automatically as new batches are processed.

## Data Flow

The complete pipeline executes in this sequence:

1. API key configuration: Etherscan API keys are configured as environment variables.  
2. Data ingestion: `etherscan_to_s3_glue.py` calls Etherscan APIs for configured addresses and writes transaction data to S3 buckets.  
3. Feature engineering: `glue_feature_engineering.py` reads raw S3 data, applies transformations using PySpark, and generates engineered feature tables stored back in S3.  
4. Model training and scoring: `glue_modeling.py` loads feature tables, trains or applies fraud detection models, and outputs predictions to S3 and RDS.  
5. Database loading: processed data and predictions are loaded into RDS tables with proper schema and indexes.  
6. Athena table creation: external tables in Athena are configured to point at S3 data for SQL-based analysis.  
7. Dashboard deployment: `app.py` Streamlit application connects to RDS and serves the fraud detection interface.  

## Repository Structure

```markdown
ethereum-fraud-detection/
│
├── .github/
│   └── workflows/              # CI/CD pipeline 
│       ├── ci-pipeline.yml
│       └── deploy-dashboard.yml
│
├── .vscode/                    # VS Code editor settings
│   ├── run_ingestor.sh
│   └── settings.json
│
├── dashboard/                  # Streamlit dashboard application
│   ├── Dockerfile
│   ├── appy.py
│   └── docker-compose.yml
│
├── glue_jobs/                  # AWS Glue ETL scripts
│   ├── etherscan_to_s3_glue.py
│   ├── glue_feature_engineering.py
│   └── ethereum_modeling.py
│
├── scripts/                    # Utility and automation scripts
│   ├── __init__.py
│   ├── feature_engineering.py
│   └── modeling.py
│
├── tests/                      # Test suite
│   ├── __pycache__/
│   ├── test_dashboard.py
│   ├── test_docker_integration.py
│   ├── test_etherscan_ingestion.py
│   ├── test_feature_engineering.py
│   ├── test_integration.py
│   └── test_modeling.py
│
├── .coverage                  # Code coverage report
├── .dockerignore              # Docker build exclusions
├── .gitignore                 # Git exclusions
├── Dockerfile                 # Docker container configuration
├── Makefile                   # Build automation commands
├── README.md                  # Project documentation
├── docker-compose.yml         # Multi-container Docker configuration
├── ethereum-feature-engineering.py
├── ethereum_fraud_modeling.py
├── file.env.example           # Environment variables template
├── requirements-dev.txt       # Development dependencies
└── requirements.txt           # Production dependencies

```


## Setup Instructions

### Prerequisites

- Python 3.10 or higher  
- AWS account with access to S3, Glue, Athena, and RDS  
- Etherscan API key  
- Docker and Docker Compose (optional)  

### Local Development Setup

1. Clone this Github repository.

2. Install Python dependencies:
pip install -r requirements.txt


3. Configure environment variables and edit `.env` with your credentials:
- cp .env.example .env
- ETHERSCAN_API_KEY=your_etherscan_api_key
- AWS_ACCESS_KEY_ID=your_aws_access_key
- AWS_SECRET_ACCESS_KEY=your_aws_secret_key
- AWS_REGION=us-east-1
- S3_BUCKET=your-ethereum-data-bucket
- RDS_HOST=your-rds-endpoint
- RDS_DATABASE=fraud_detection
- RDS_USER=your_db_user
- RDS_PASSWORD=your_db_password


4. Run tests:
pytest tests/ -v

### Running the Pipeline
**Step 1: Ingest Ethereum Data**
Run the ingestion script to fetch transactions from Etherscan:
python data_ingestion/etherscan_to_s3_glue-1.py

**Step 2: Feature Engineering**
Execute the Glue job or run locally:

```bash
# For local PySpark execution:
spark-submit glue_jobs/glue_feature_engineering.py

# Or trigger AWS Glue job through console/CLI
```

**Step 3: Train and Apply Fraud Detection Model**
Run the modeling script:

```bash
spark-submit glue_jobs/ethereum_modeling.py
```

**Step 4: Launch Dashboard**
Start the Streamlit application:

```bash
cd dashboard
streamlit run app.py
```
Access the dashboard at http://localhost:8501 to explore fraud detection results.

### Querying Data with Athena
Create Athena tables pointing to your S3 data:

```sql
CREATE EXTERNAL TABLE ethereum_transactions (
    hash STRING,
    from_address STRING,
    to_address STRING,
    value BIGINT,
    gas BIGINT,
    timestamp BIGINT
)
STORED AS PARQUET
LOCATION 's3://your-bucket/ethereum-transactions/';
```

## Testing Strategy
The CI/CD pipeline runs comprehensive tests on every pull request:

We implement testing coverage across six files: `test_feature_engineering.py` tests data transformation logic (column cleaning, temporal features, log transformations, z-scores), `test_modeling.py` validates anomaly detection algorithms (scoring, thresholds, labeling), `test_integration.py` performs system-level validation (pipeline structure, CI/CD setup, Docker configuration, environment reproducibility), `test_dashboard.py` validates the Streamlit dashboard (data loading, filtering, fraud metrics, suspicious address aggregation, daily trends), `test_docker_integration.py` tests containerization and deployment (Dockerfile structure, docker-compose services, .dockerignore, Makefile targets, Docker runtime), and `test_etherscan_ingestion.py` validates API data ingestion (API key rotation, S3 key formatting, transaction fetching, configuration parsing). Tests are organized in three tiers: unit tests for individual functions, integration tests for DataFrame operations, and system tests for project structure validation. Run with `pytest` or `pytest <filename>` for specific test files.

All tests must pass before merging to main, maintaining code quality and preventing regressions.

## Project Components
The team implemented the following components for this data engineering project.

1. Project Repository and Collaboration
- All team members actively contributed to a shared GitHub repository. Roughly, we followed the following team roles, and switched roles for a few tasks so that each member had exposure to all aspects of the project process:
    - Project Manager/Team Lead (Everybody) – Overall project coordination, system architecture design, timeline management, and communication.
    - ML Engineer (Seohyun Oh) – Isolation Forest model development, performance evaluation, feature engineering, and threshold optimization for anomaly detection.
    - Backend Developers (Ananya Jogalekar and Farnoosh Memari) – AWS infrastructure management, Etherscan API integration, MySQL database design, batch processing pipeline, and automated scheduling implementation.
    - Frontend Developer (Shelly Cao and Farnoosh Memari) – Streamlit dashboard development, data visualization, user interface design, and real-time alert display systems.
    - DevOps Engineer (Sebine Scaria and Ananya Jogalekar) – README documentation, Docker and Devcontainer containerization, CI/CD pipeline setup, and system monitoring.

2. Data Ingestion 
- We use daily batch ingestion of real transaction data from the [Etherscan API](https://etherscan.io/apis), which contains real-world Ethereum transactions.

3. Data Storage
- We store data in two systems: Amazon S3 (data lake) and Amazon RDS (relational database system).
- We use these two systems for appropriate use cases: large-scale data manipulation from S3 and querying and dashboarding from RDS.
- While deploying on the cloud platform AWS, we ensure proper schema design for aligning it across the deployment process which involves crawlers, ETL jobs, and triggers in Amazon Glue.

4. Data Querying
- We implement data querying in Athena:
    - Our first query generates daily transaction statistics by calculating the total number of transactions, the number of failed transactions, and the total on-chain transaction value for each date.
    - Our second query extracts high gas-price anomaly transactions by selecting all records flagged as gasprice_spike = 1. This helps identify potential outliers, bot activity, or suspicious high-fee transactions.

5. Data Transformation and Analysis
- We use PySpark to analyze and transform our data (run feature engineering) before processing it with an ML model.
    - This code does data cleaning of the raw API data, casts numeric columns, and prepares it for modeling.
- We generate various statistical insights in this process, including:
    - means, stddevs, z-scores, percentiles, anomaly scoring

6. Architecture and Orchestration
- We use Triggers in AWS to schedule daily batch ingestion and Glue ETL jobs to run the entire pipeline on these batches.
- Architecture diagram:
```markdown
┌──────────────────────────────┐
│   Raw Ethereum JSON Files     │
│  S3: de-27-team11-new/raw/... │
└───────────────┬───────────────┘
                │
                ▼
┌──────────────────────────────────────┐
│ Glue Job: ethereum-feature-engineering │
│ - Flatten JSON result[]                │
│ - Temporal features                    │
└──────────────────────────────────────┘
                              │  Creates Table:
                              │  ethereum_db.raw_txlist
                              ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ Glue Job: ethereum-feature-engineering                          │
  │ - Flatten JSON result[]                                         │
  │ - Temporal features                                             │
  │ - Address-level statistics                                      │
  │ - Rolling windows, z-scores, spikes                             │
  │ - Writes Parquet features to S3                                 │
  └───────────────┬─────────────────────────────────────────────────┘
                  │
                  ▼
 ┌────────────────────────────────────────────────────────────┐
 │ Glue Crawler: ethereum-processed-features-crawler          │
 │ → Creates ethereum_db.processed_txlist_features            │
 └───────────────┬────────────────────────────────────────────┘
                 │
                 ▼
  ┌────────────────────────────────────────────────────────────┐
  │ Glue Job: ethereum-fraud-modeling                          │
  │ - Reads processed features                                 │
  │ - Computes anomaly scores                                  │
  │ - Labels suspicious transactions                           │
  │ - Writes results to S3                                     │
  └───────────────┬────────────────────────────────────────────┘
                  │
                  ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ Glue Crawler: ethereum-fraud-predictions-crawler            │
 │ → Creates ethereum_db.fraud_predictions                     │
 └───────────────┬─────────────────────────────────────────────┘
                  │
                  ▼
                ┌─────────────────────────────────┐
                │            Athena               │
                │ Query raw, features, predictions│
                └──────────────┬──────────────────┘
                               │
                               ▼
                ┌─────────────────────────────────┐
                │      Streamlit Dashboard        │
                │ - Real-time fraud monitoring    │
                │ - Anomaly score visualization   │
                │ - Suspicious address tracking   │
                │ - Daily trend analysis          │
                └─────────────────────────────────┘
```

7. Containerization, CI/CD and Testing
- We implement a reporoducible environment, CI pipeline, and testing setup in this GitHub repository, as explained in the Testing Strategy section of this document.

8. Undercurrents of Data Engineering

Our system is built for scalability and efficiency, using S3 for unlimited storage and distributed PySpark processing on Glue to handle growing blockchain data volumes. A modular, reusable pipeline design separates ingestion, feature engineering, modeling, and monitoring, while strong observability, governance, and security practices (CloudWatch logging, IAM, TLS, Secrets Manager, encryption) ensure reliability and compliance. Fault-tolerance mechanisms, cost-optimized storage/compute choices, and database optimizations further support consistent, performant end-to-end data processing. Here is how we address specific aspects of the principles of data engineering:
- Scalability: data lake storage, distributed PySpark processing allows for large-scale data handling
- Modularity: Ingestion, feature engineering, and modeling scripts are decoupled, allowing parallel development and testing
- Reusability: Feature engineering functions are packaged as reusable PySpark transformations that can be applied to other blockchain networks (Bitcoin, Polygon) or extended for new fraud patterns
- Efficiency: S3 data is partitioned by date to minimize scanning for recent queries. Glue jobs use dynamic resource allocation, scaling clusters based on workload. Dashboard queries use database indexes to avoid full table scans
- Security: confidential information like API keys and connection authorization information are stored as environment variables, like GitHub secrets.

## Collaboration Workflow
The team followed a Git branch workflow:

1. Create feature branches from main for new work

2. Commit changes regularly with descriptive messages

3. Push branches and open pull requests for code review

4. GitHub Actions automatically runs tests on each PR

5. After approval and passing tests, merge to main

6. Main branch always remains deployable

7. Multiple team members commit weekly, ensuring active collaboration and knowledge sharing.

## Project Outcomes
The fraud detection system successfully processes real Ethereum transaction data and identifies suspicious patterns. The model achieves strong performance metrics on validation sets, demonstrating ability to surface actionable intelligence from raw blockchain data.

The complete pipeline executes daily ingestion and analysis of thousands of transactions, producing updated fraud scores within 30 minutes of data availability. This performance meets stakeholder requirements for near real-time fraud monitoring.

## Future Enhancements
- Automated Orchestration: Implement Apache Airflow DAGs to schedule and monitor the entire pipeline end-to-end
- Advanced Features: Incorporate graph analytics to trace fund flows across multiple hops and identify laundering patterns
- Real-time Processing: Add Kafka streaming for immediate fraud detection on high-value transactions
- Model Improvements: Experiment with deep learning (LSTM, GNN) for capturing complex temporal and network patterns
- Alerting System: Integrate SNS or PagerDuty to notify security teams when high-risk activity is detected
- Feedback Loop: Build analyst interface to label confirmed fraud cases, enabling continuous model retraining

## Contact and Contribution
For questions about setup, development, or contributions, contact the project team:
- Shelly Cao
- Ananya Jogalekar
- Farnoosh Memari
- Seohyun (Claire) Oh
- Sebine Scaria