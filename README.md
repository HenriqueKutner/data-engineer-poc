# INDICIUM Challenge

Este projeto utiliza o Apache Airflow para orquestrar um processo ETL que extrai dados de um banco de dados PostgreSQL, realiza transformações e carrega os dados em um novo banco de dados. O processo também inclui a carga de dados de um arquivo CSV para o banco de dados de destino.

## Requisitos

- Docker
- Docker Compose
- Python 3.x (para Airflow e bibliotecas necessárias)

## Estrutura do Projeto

O projeto é composto pelos seguintes arquivos e diretórios:

- **DAGs**: Definição das tarefas do Airflow e fluxo de trabalho.
- **Docker Compose**: Configuração dos containers necessários (Airflow, PostgreSQL, etc).
- **Scripts Python**: Funções de ETL para extrair, transformar e carregar os dados.

## Como Rodar

### 1. Clonar o Repositório

Clone o repositório do projeto para sua máquina local.


git clone <url-do-repositorio>
cd <diretorio-do-projeto>


### 2. Docker
docker-compose up -d

### 3. Airflow UI
localhost:8080


### 4. Criar Conexões no Airflow

Após acessar a interface do Airflow, é necessário configurar as conexões com os bancos de dados utilizados no projeto. Para isso, siga os passos abaixo:

1. Vá até o menu **Admin** na interface do Airflow.
2. Selecione **Connections**.
3. Clique em **+** para adicionar uma nova conexão.
4. Crie as seguintes conexões:

   - **northwind_db**: Conexão para o banco de dados de origem (Northwind).
     - **Conn Id**: `northwind_db`
     - **Conn Type**: `Postgres`
     - **Host**: `northwind_db` (nome do serviço no `docker-compose.yml`)
     - **Schema**: `northwind`
     - **Login**: `northwind_user`
     - **Password**: `thewindisblowing`
     - **Port**: `5433`
   
   - **new_postgres_db**: Conexão para o banco de dados de destino.
     - **Conn Id**: `new_postgres_db`
     - **Conn Type**: `Postgres`
     - **Host**: `new_postgres_db` (nome do serviço no `docker-compose.yml`)
     - **Schema**: `new_database`
     - **Login**: ``
     - **Password**: ``
     - **Port**: `5434`


### 5. Init DAG