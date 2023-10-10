#!/bin/sh

helm repo add apache-airflow https://airflow.apache.org
helm repo update apache-airflow

helm upgrade --install airflow apache-airflow/airflow \
	-f airflow-override-values.yaml \
	--namespace airflow --create-namespace 

