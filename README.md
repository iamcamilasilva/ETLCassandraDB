# Data Modeling with Cassandra

![](images/Apache_Cassandra-Logo.wine.png)

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results
.
## Project Overview

In this project, I'm gonna apply data modeling concepts with Apache Cassandra and complete an ETL pipeline using Python. To complete the project, I've  modelling the data by creating tables in Apache Cassandra to run queries. Those are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

## Datasets

In this project, I worked with `event_data` dataset. This directory of csv files is partitioned by date. Here are examples of filepaths to two files in the dataset:

`event_data/2018-11-01-events.csv`

`event_data/2018-11-02-events.csv`

## ETL Pipeline

- Running `etl.ipynb` first preprocesses the csv file, and then includes Apache Cassandra `CREATE` and `INSERT` statements to load processed records into relevant tables. The tables are tested by running `SELECT` statement cells.
- `etl.py` contains all ETL process as the same on `etl.ipynb`.
- `preprocessing_file.py` contains the preprocessing steps that creates a new csv files to be used for Apache Cassandra tables and is imported into `etl.py`.
