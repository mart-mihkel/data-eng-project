#!/bin/bash

airflow users create \
    --username admin \
    --password $AIRFLOW_ADMIN_PASSWORD \
    --firstname Big \
    --lastname Man \
    --role Admin \
    --email big@man.uk 

airflow users create \
    --username user \
    --password $AIRFLOW_USER_PASSWORD \
    --firstname Medium \
    --lastname Man \
    --role User \
    --email medium@man.uk 

airflow users create \
    --username viewer \
    --password $AIRFLOW_VIEWER_PASSWORD \
    --firstname Small \
    --lastname Man \
    --role Viewer \
    --email viewer@man.uk 
