## Table of contents
- [Table of contents](#table-of-contents)
- [Project Requirements](#project-requirements)
- [Project Structure](#project-structure)
- [Miniconda for IDEA](#Miniconda-for-idea)
- [High Level Architecture](#high-level-architecture)
- [:ship: Containers](#ship-containers)
- [Step-by-Step](#step-by-step)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Setup environment](#2-setup-environment)
  - [3. Airflow: Create user for UI](#3-airflow-create-user-for-ui)
  - [3.1 Airflow: Postgres & Spark connections configuration](#31-airflow-postgres-minio--spark-connections-configuration)
    - [Postgres](#postgres)
    - [Spark](#spark)
- [Stack](#stack)
- [References](#references)

## Project Requirements

This project was develop and tested using the following environment.


| Item           | Version       |
|----------------|---------------|
| Ubuntu         | `20.04.6 LTS` |
| Docker         | `23.0.2`      |
| Docker Compose | `1.25.0`      |
| Python         | `3.8.16`      |
| OpenJDK        | `11.0.19`     |
| Git            | `2.25.1`      |
| Conda          | `4.12.0`      |


## Project Structure
 

This challenge is currently structured with the following specifications.

|   Path             |        Description        |
|----------------|-------------------------------|
|src|`Dockefile, dags and Spark applications`            |

## Miniconda for IDEA
    # create env
    conda create -n airlfow-spark-postgres python=3.8

    # activate env
    conda activate airlfow-spark-postgres
    
    # add packages
    pip install apache-airflow==2.2.4
    pip install pyspark==3.2.1
    pip install apache-airflow-providers-apache-spark==3.0.0
    pip install wget

    # check installed version
    conda list pyspark -f
    
    # add `airlfow-spark-postgres` conda env as a Python Interpreter in your IDEA project.

##  High Level Architecture

    Database: PostgreSQL

    ETL Orchestration: Airflow

    Data Processing: Spark


## :ship: Containers

* **airflow-webserver**: Airflow v2.2.4 (Webserver & Scheduler)
    * image: andrejunior/airflow-spark:latest | Based on python:3.8-buster
    * port: 8085 
  
* **postgres**: Postgres database (Airflow metadata and our pipeline)
    * image: postgres:14-bullseye
    * port: 5432

* **spark-master**: Spark Master
    * image: bitnami/spark:3.2.1
    * port: 8081

* **spark-worker**: Spark workers
    * image: bitnami/spark:3.2.1

## Step-by-Step

### 1. Clone the Repository

`git clone https://github.com/MaorAharon/airflow-spark-postgres.git`

### 2. Setup environment

```
# launch 
$ docker-compose -f docker-compose.yml up -d

# stop
# $ docker compose stop

# pyspark / spark-submit examples 
$ docker exec -i -t airflow /bin/bash

$ pyspark --conf spark.sql.caseSensitive=True --conf spark.executor.memory=1024M --conf spark.driver.cores=1 --conf spark.driver.memory=1024M --jars /shared-data/accessories/postgresql-42.2.6.jar --total-executor-cores 1 
$ spark-submit --master spark://spark:7077 --conf spark.master=spark://spark:7077 --conf spark.sql.caseSensitive=True --conf spark.executor.instances=1 --conf spark.executor.cores=1 --conf spark.executor.memory=1024M --conf spark.driver.cores=1 --conf spark.driver.memory=1024M --py-files /shared-data/accessories/wget.py --driver-class-path /shared-data/accessories/postgresql-42.2.6.jar --jars /shared-data/accessories/postgresql-42.2.6.jar --num-executors 1 --executor-cores 1 --queue root.default /usr/local/spark/applications/ny_yellow.py


# postgres db examples: 
$ docker exec -i -t postgres /bin/bash
$ psql -U 'airflow' -d 'airflow'

>> SELECT * FROM information_schema.schemata WHERE schema_name = 'stage';
>> select date, count(*) as cnt from taxi.ny_yellow group by 1 order by 1;
>> select * as cnt from taxi.ny_yellow_cnt order by 1;
```
 

### 3. Airflow: Create user for UI
To access Airflow UI is required to create a new user account, so in our case, we are going to create an fictional user with an Admin role attached.

> **NOTE**: Before **RUN** the command below please confirm that Airflow is up and running, it can be checked by accessing the URL [http://localhost:8085](http://localhost:8085). Have in mind that in the first execution it may take 2 to 3 minutes :stuck_out_tongue_winking_eye:


docker-compose run airflow-webserver airflow users create --role Admin --username airflow --email airflow@example.com --firstname airflow --lastname airflow --password airflow

### 3.1 Airflow: Postgres & Spark connections configuration

1. Open the service in your browser at http://localhost:8085
   Use the credentials 
   ```
   User: airflow
   Password: airflow
   ```

2. Click on Admin -> Connections in the top bar.
    ![](./imgs/connections.png "connections")

3. Click on + sign and fill in the necessary details for each source below:
    ![](./imgs/add_conn.png "add_conn")

4. Trigger the dag `initialize-pgsql-schema` manually only once 
  
#### Postgres

    Conn Id: postgres_conn
    Conn Type: Postgres
    Host: postgres
    Schema: airflow
    Login: airflow
    Password: airflow
    Port: 5432

####   Spark

    Conn ID: spark_conn
    Host: spark://spark
    Port: 7077
    Extra: consists of the JSON below:
```
{"queue": "root.default"}
```


## Stack

|        Application        |URL                          |Credentials                         |
|----------------|-------------------------------|-----------------------------|
|Airflow| [http://localhost:8085](http://localhost:8085) | ``` User: airflow``` <br> ``` Pass: airflow``` |         |
|Postgres| **Server/Database:** localhost:5432/airflow | ``` User: airflow``` <br> ``` Pass: airflow``` |           |
|Spark (Master) | [http://localhost:8081](http://localhost:8081)|  |         |
  

## References

[airflow.apache.org](https://airflow.apache.org/docs/apache-airflow/stable/)

[puckel/docker-airflow](https://github.com/puckel/docker-airflow)

 [cordon-thiago/airflow-spark](https://github.com/cordon-thiago/airflow-spark/)

 [pyjaime/docker-airflow-spark](https://github.com/pyjaime/docker-airflow-spark/)
