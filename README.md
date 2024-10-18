# scalable-etl-pipeline
## Overview
- This project demonstrates a scalable ETL (Extract, Transform, Load) pipeline using Python and PostgreSQL. It processes NYC taxi trip duration data from Kaggle, handling data extraction, transformation, and loading into a PostgreSQL database.

## Project Structure
- The project structure:
   ```bash
   scalable-etl-pipeline/
   ├── dags/                     # Airflow DAGs (Optional for future use)
   ├── data/                     # Raw data files (CSV)
   ├── data_clean/               # Processed data (CSV)
   ├── etl/                      # ETL scripts (extract, transform, load)
   ├── logs/                     # Log files for the ETL pipeline
   ├── scripts/                  # SQL scripts for DB setup
   ├── tests/                    # Unit and integration tests
   ├── .env                      # Environment variables for DB config
   ├── requirements.txt          # Python dependencies
   ├── docker-compose.yaml       # Docker setup (optional)
   ├── Dockerfile.spark          # Spark setup (optional)
   └── run_all_etl.py            # Orchestrates the ETL pipeline

## Setup Instructions
1. Clone the Repository
   ```bash
   git clone https://github.com/yourusername/scalable-etl-pipeline.git
   cd scalable-etl-pipeline
2. Install Dependencies
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
3. Set Up Environment Variables
   ```bash
   SUPERUSER_USER=your_username
   SUPERUSER_PASSWORD=your_password
   DB_USER=airflow
   DB_PASSWORD=airflow
   DB_NAME=airflow
   DB_HOST=localhost
   DB_PORT=5432
4. Set Up PostgreSQL
- Run the database setup scripts:
   ```bash
   psql -U your_username -f scripts/setup_db.sql
   psql -U your_username -d airflow -f scripts/setup_tables.sql
5. Run the ETL Pipeline
   ```bash
   python run_all_etl.py
6. Reset the Pipeline (Optional)
- To reset the database and start fresh:
   ```bash
   psql -U your_username -f scripts/reset_db.sql
## Docker Setup (Optional)
- To run using Docker:
   ```bash
   docker-compose up --build
## Logs
- Logs are stored in the logs/ directory for each ETL run.
## Tests
- Run tests with:
   ```bash
   pytest tests/
## License
- This project is licensed under the MIT License.

