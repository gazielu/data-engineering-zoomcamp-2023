>[Back to Index](README.md)

>Next: [Data Ingestion](2_data_ingestion.md)

### Table of contents

- [Introduction to Data Engineering](#introduction-to-data-engineering)
  - [Architecture](#architecture)
  - [Data pipelines](#data-pipelines)
- [Docker and Postgres](#docker-and-postgres)
  - [Docker basic concepts](#docker-basic-concepts)
  - [Creating a custom pipeline with Docker](#creating-a-custom-pipeline-with-docker)
  - [Running Postgres in a container](#running-postgres-in-a-container)
  - [Ingesting data to Postgres with Python](#ingesting-data-to-postgres-with-python)
  - [Connecting pgAdmin and Postgres with Docker networking](#connecting-pgadmin-and-postgres-with-docker-networking)
  - [Using the ingestion script with Docker](#using-the-ingestion-script-with-docker)
    - [Exporting and testing the script](#exporting-and-testing-the-script)
    - [Dockerizing the script](#dockerizing-the-script)
  - [Running Postgres and pgAdmin with Docker-compose](#running-postgres-and-pgadmin-with-docker-compose)
  - [SQL refresher](#sql-refresher)
- [Terraform and Google Cloud Platform](#terraform-and-google-cloud-platform)
  - [GCP initial setup](#gcp-initial-setup)
  - [GCP setup for access](#gcp-setup-for-access)
  - [Terraform basics](#terraform-basics)
  - [Creating GCP infrastructure with Terraform](#creating-gcp-infrastructure-with-terraform)
- [Extra content](#extra-content)
  - [Setting up a development environment in a Google Cloud VM](#setting-up-a-development-environment-in-a-google-cloud-vm)
  - [Port mapping and networks in Docker](#port-mapping-and-networks-in-docker)

# Introduction to Data Engineering
***Data Engineering*** is the design and development of systems for collecting, storing and analyzing data at scale.

## Architecture

During the course we will replicate the following architecture:

![architecture diagram](https://github.com/DataTalksClub/data-engineering-zoomcamp/raw/main/images/architecture/arch_1.jpg)

# project formula  want to ingest races and load it into postgres
# we have to dcokers and we need to set them in a same network


 this is how we connect from local host / dbeaver  
 ```python
import pandas as pd 
from sqlalchemy import create_engine 
engine = create_engine('postgresql://root:root@localhost:5432/ny_texi') 
engine.connect()
```

if we run it without docker build we need to install each time the pip and then call python, we must give the full path in the docker volume path
```bash
pip install pandas sqlalchemy psycopg2

python
import pandas as pd 
from sqlalchemy import create_engine 
engine = create_engine('postgresql://root:root@thirsty_darwin/ny_texi') 
engine.connect()
query = """select 1 as col;"""
pd.read_sql(query, con=engine)
df = pd.read_csv('/var/lib/formula1/data/races.csv')
query =  """select * from races
```

this is how we run by command the dicker with all the parameters, actually the postgres its only the database so we have no nned to map the vlume
```bash
docker run -it \
  --network "formula" \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_texi" \
  -v "/c/dataai/github/data-engineering-zoomcamp-2023/week_1_basics_n_setup/2_docker_sql/data:/var/lib/formula1/data" \
  -p 5432:5432 \
  postgres:13
  ```

  in the python app we need to set the volume in the docker run commend
  we use bash in the end becouse we want to start with bash as entrypoint
  ```bash
  docker run -it \
  --network "formula" \
  -v "//c/dataai/github/data-engineering-zoomcamp-2023/week_1_basics_n_setup/2_docker_sql/data:/var/lib/formula1/data" \
  python:3.9 \
  bash
  ```

incase that we need to understand the path in python
```python
Then, you can use os.getcwd() to print the current working directory:
python
Copy code
print(os.getcwd())
```


we create a network as well

Step 1: Create or Identify Your Network
If you haven't already created a Docker network, you can create one with the following command:

```bash
docker network create my-network
docker network connect my-network container_name_or_id
'''
if we want to inspect
```bash
docker network inspect my-network
```


FROM python:3.9
```dockerfile
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data_races.py ingest_data_races.py 

ENTRYPOINT [ "python", "ingest_data_races.py" ]
```

running this query
```bash
docker build -t formula:pandas .
```

when use docker build
```
docker run -it \
  --network "formula" \
  -v "//c/dataai/github/data-engineering-zoomcamp-2023/week_1_basics_n_setup/2_docker_sql_formula/data:/var/lib/formula1/data" \
  formula:pandas
  
```
