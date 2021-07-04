# Sparkfy DW

The Sparkfy process to allow the analysis of song listening data.

With the growing user base, it's became mandatory to build a escalable infrastructure in the cloud to process huge amounts of data. And, at the same time, to have this info digested into an easy reading set of tables.

## Raw data

The raw data is composed by:

- Song data
- Event logs

The song data reflects data about the songs, such as the artist name, the song name, etc. While the event logs contains info about the user, the song listened, the time, etc.

This data comes as json files in a S3 bucket at AWS, and the log events have a schema defined in a file also in S3, where it's name and location are defined in the config `S3/LOG_JSONPATH`.

## Star schema

The following figure depicts the schema in which the data will be available for querying after the ETL process:

![alt text](https://github.com/matherthal/udacity-data-engineering/blob/main/data_warehouse/start_schema.png)

The facts are informations about the location of users, the level of service (i.e. free, paid) and their systems (browser, OS, etc). These facts refer to the song, the artist, the user and the time.

## ETL

The pipeline extracts data from S3 to 2 staging tables in Redshift, and then transforms the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

This script python performs queries that do the COPY of data from S3 to the Redshift and others that do the transformations and deduplication of data into the final tables. 

The ETL pipeline is built over Airflow where each step of the process is a task. 

## Requirements

This project has 2 infrastructure requirements:

- Redshift cluster
- Airflow

## How to use

Include the contents of the folders `dags` and `plugins` into the Airflow folders with the same name. 

Create 2 **connections** in Airflow:

- `redshift` with:
    - conn type
    - login
    - password
    - host
    - port
    - schema
- `aws_credentials` with:
    - conn type
    - login
    - password

Enable the new dag in Airflow. It'll start soon.