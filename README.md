# ENTREGA FINAL DATA ENGINEERING

## developed by Rodrigo Lopez <br/>
## REQUIREMENTS
  Have Docker
  Have an app password seted up in google
  Have a developer account in Spotify
  
## Description
  * Pulls data from Spotify API
  * Gets data from local file in raw_data folder
  * Uploads that to the a table in postgres in the docker

## Credentials
The credentials are taken from Airflow variables. 
The variables.txt file has a guide

## Usage
  1- make build
  2- make run
  3- enter localhost:8080
  4- user: airflow    pass: airflow
  5- set Airflow variables
  6- Activate DAG

