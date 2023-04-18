#!/bin/sh

helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
	--namespace airflowgabin --create-namespace \
	--set dags.persistence.enabled=false \
	--set dags.gitSync.enabled=true \
 	--set dags.gitSync.branch=main \
	--set dags.gitSync.repo=https://github.com/SPS-Lab/workflow \
	--set dags.gitSync.subPath=.
