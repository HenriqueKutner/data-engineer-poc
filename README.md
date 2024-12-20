# INDICIUM Challenge

This project uses Apache Airflow to orchestrate an ETL process that extracts data from a PostgreSQL database, performs transformations, and loads the data into a new database. The process also includes loading data from a CSV file into the target database.

## Requirements

- Docker
- Docker Compose
- Python 3.x (for Airflow and required libraries)

## Project Structure

The project consists of the following files and directories:

- **DAGs**: Definition of Airflow tasks and workflow.
- **Docker Compose**: Configuration of the necessary containers (Airflow, PostgreSQL, etc.).
- **Python Scripts**: ETL functions to extract, transform, and load data.

## How to Run:

### 1. Clone the Repository
### 2. Update Environment Variables
Modify the .env file with appropriate environment variables.

### 3. Start Docker
docker-compose up -d

### 4. Airflow UI
localhost:8080


### 4. Create Connections in Airflow

After accessing the Airflow interface, you need to configure the connections to the databases used in the project. Follow the steps below:

1. Go to the Admin menu in the Airflow interface.
2. Select Connections.
3. Click + to add a new connection.
4. Create the following connections:

   - **northwind_db**: 
     - **Conn Id**: `northwind_db`
     - **Conn Type**: `Postgres`
     - **Host**: `northwind_db` 
     - **Schema**: `northwind`
     - **Login**: `northwind_user`
     - **Password**: `thewindisblowing`
     - **Port**: `5432`
   
   - **new_postgres_db**: 
     - **Conn Id**: `new_postgres_db`
     - **Conn Type**: `Postgres`
     - **Host**: `new_postgres_db`
     - **Schema**: ``
     - **Login**: ``
     - **Password**: ``
     - **Port**: `5432`


### 5. Init DAG