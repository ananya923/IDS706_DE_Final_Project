# Unusual Suspects - Crypto Market Fraud Detection Model

# Description

 Unusual Suspects is a machine learning–driven fraud detection system designed to identify anomalous and suspicious patterns in Ethereum transaction data. The system runs on a daily batch schedule to extract, process, and analyze blockchain transactions using both labeled and unlabeled datasets. It delivers actionable insights to risk management teams through an interactive fraud monitoring dashboard, aiming to achieve high precision while maintaining low false-positive rates.

# Data Sources
Model development dataset: 
- [[Kaggle](https://www.kaggle.com/datasets/vagifa/ethereum-frauddetection-dataset)] Ethereum Fraud Detection DatasetLinks to an external site.
- Contains labeled fraudulent and legitimate Ethereum transactions
- Used for initial training, validation, and benchmark comparison.
Operational Dataset: 
- [[Etherscan](https://etherscan.io/)] Ethereum Transactions APILinks to an external site.
- Daily transactions and metadata (sender, receiver, gas fee, timestamps)
- Stored in structured form for batch feature extraction.

# Architecture

1. Data Collection & Storage

Etherscan API (Data) through automating scheduled ingestion in Amazon ECR -> Designing the data schema in Amazon S3 -> Store the data as a database in Amazon RDS -> 
Daily batch collection of Ethereum transactions via Etherscan API and automating scheduled ingestion in Amazon ECR. Store raw transaction data in Amazon S3 and process it into a database in Amazon RDS. 

2. Feature Engineering & Preprocessing

Extract transaction features including transaction patterns (amount, frequency), temporal behaviors, and address relationship mappings using pandas for data manipulation

3. Unsupervised Model Training

Train Isolation Forest model (scikit-learn) to train on unlabeled data from historical Ethereum transactions. Validate and tune hyperparameters using known fraud cases from the Kaggle dataset.

4. Anomaly Detection & Scoring

Apply trained model to daily transaction batches, compute anomaly scores, and apply threshold-based filtering to identify suspicious transactions with minimal false positives. 

5. Dashboard & Monitoring
Build Streamlit-based dashboard displaying real-time fraud alerts, transaction pattern visualizations, daily statistics, and manual review interface for risk team validation.

# Project Setup

# Devcontainer Setup

A devcontainer is a Docker-based container specifically configured to serve as a complete development environment, ensuring consistency and reproducibility across different machines and setups. It allows developers to package all the necessary tools, libraries, frameworks, and configuration files for a project into a single environment, making onboarding and environment setup much faster and more reliable.

The devcontainer folder for this assignment is named as .devconatiner which has a bunch of files in it that makes this entire project work seamlessly.

We do use some Amazon tools which need not require a devcontainer setup, since those tools are accessed in the cloud (AWS)

# Data Ingestion

# Description of the tools used for this step

- Amazon S3
Amazon S3 (Simple Storage Service) is an object storage service from AWS that provides industry-leading scalability, data availability, security, and performance for storing and protecting any amount of data. Organizations of all sizes use S3 for diverse use cases including data lakes, websites, mobile applications, backup and restore, archiving, enterprise applications, IoT devices, and big data analytics. S3 integrates seamlessly with other AWS services like ECR for big data processing, making it a foundational component of cloud-based data architectures.

- Amazon RDS
Amazon RDS (Relational Database Service) is a fully managed database service from AWS that simplifies setting up, operating, and scaling relational databases in the cloud. It supports multiple popular database engines including PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, and IBM Db2, allowing you to use familiar database software without managing the underlying infrastructure. For the purposes of this project, we are using MySQL. 

- Amazon ECR
Amazon ECR (Elastic Container Registry) is a fully managed Docker container registry service from AWS that allows you to store, manage, and deploy container images securely and reliably. For this project, we are using it to store the ETL jobs. 

We are using two data sources, the Kaggle data of fraud ethereum transactions as our labelled data for training the Isolation Forest model and then use this model on the ethereum transactions data which we collect daily using the Etherscan API. We use Amazon S3 and RDS to collect the API data, and store them. We also use Amazon ECR to preprocess the big API data to automate and schedule data ingestion and perform ETL jobs on it. 

# Data Transformation and Feature Engineering of the labelled (Kaggle) dataset.

The Kaggle Etheurem data (labelled data) gets transformed by doing some feature engineering to improve the performance of the Isolation Forest Model. We chose this dataset, because it is reliable (has past and original Ethereum transactions), labeled fraudulent and legitimate Ethereum transactions, and used for initial training, validation, and benchmark comparison.

The `Feature_Engineering.ipynb` notebook has all the codes for the data transformation.

The notebook starts with creating a Spark DataFrame from raw transaction data, then performs data cleaning and type conversions. It identifies contract interactions by checking if contractAddress is present, cleans function names by removing parameter signatures, and casts columns to appropriate types (numeric or categorical).

The code extracts time-based features from timestamps, including date, week start, hour, and time-of-day slots (midnight, morning, afternoon, night). It then aggregates transaction counts and failure rates at multiple time granularities—total, daily, and weekly—for both sender (from) and receiver (to) addresses. To measure transaction frequency patterns, it calculates average intervals between consecutive transactions per address using window functions with lag().

The code applies log transformations to value and gasPrice to handle their wide ranges, then implements anomaly detection using rolling 7-day windows. For each address, it calculates rolling means and standard deviations, computes z-scores, and flags "spike" transactions that exceed 3 standard deviations above the rolling mean—indicating potentially unusual transaction behavior. The spike detection includes safeguards to avoid false positives when insufficient historical data exists.

Categorical features like methodId, functionName, and time_slot are encoded using a PySpark ML Pipeline. The pipeline applies StringIndexer to convert strings to numeric indices, then OneHotEncoder to create binary vector representations suitable for machine learning models.

Finally, all engineered features are joined back to the main DataFrame using left joins on appropriate keys (from, to, date, week_start). The result is a comprehensive dataset combining raw transaction data with temporal patterns, behavioral metrics, anomaly indicators, and encoded categorical variables—ready for downstream analytics or fraud detection models.

# Application of Isolation Forest Tree Model on the transformed labelled data.

The notebook that has the codes for this step is `Modeling.ipynb`.

1. Package Installation
The notebook imports essential libraries including NumPy, pandas, and scikit-learn components (IsolationForest, StandardScaler, train_test_split, and evaluation metrics).​

2. Feature Selection
The code defines a comprehensive feature set that includes log-transformed values (log_value, log_gasPrice), behavioral statistics for both sender and receiver addresses (transaction counts, failure rates, intervals at daily/weekly/total levels), anomaly indicators (value_zscore, gasPrice_zscore, value_spike, gasPrice_spike), rolling transaction counts, and contract call flags. The PySpark DataFrame is converted to pandas, and categorical features (methodId, functionName, time_slot) are one-hot encoded using pd.get_dummies().​

3. Model Configuration
An Isolation Forest classifier is configured with 100 trees (n_estimators=100), assuming a 1% fraud contamination rate (contamination=0.01), and parallel processing enabled (n_jobs=-1). This unsupervised approach identifies outliers without requiring labeled training data.​

4. Training and Prediction
The model is trained on the feature matrix, then generates predictions where normal transactions are labeled as 1 and anomalies as -1. Each transaction receives an anomaly score (lower scores indicate higher suspicion), and the notebook outputs the total anomaly count with percentage, plus the top 10 most suspicious transactions sorted by anomaly score. 

# Application the Isolation Forest Tree Model on the processed and transformed API data.


# Running the test cases


# Key Takeaways:


# Dashboard


# Team Members and Roles

- Project Manager/Team Lead (Everybody) – Overall project coordination, system architecture design, timeline management, and communication. 
- ML Engineer (Seohyun Oh) – Isolation Forest model development, performance evaluation, feature engineering, and threshold optimization for anomaly detection.
- Backend Developers (Ananya Jogalekar and Farnoosh Memari)– AWS infrastructure management,Etherscan API integration, MySQL database design, batch processing pipeline, and automated scheduling implementation.
- Frontend Developer (Shelly Cao) – Streamlit dashboard development, data visualization, user interface design, and real-time alert display systems.

- DevOps Engineer (Sebine Scaria) – README documentation, Docker and Devcontainer containerization, CI/CD pipeline setup, and system monitoring.
