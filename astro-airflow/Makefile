
access-scheduler:
	docker exec -it astro-airflow_060da1-scheduler-1 bash 

connect-network:
	docker network connect astro-airflow_060da1_default astro-airflow_060da1-scheduler-1
	docker network connect astro-airflow_060da1_default astro-airflow_060da1-webserver-1
	docker network connect astro-airflow_060da1_default astro-airflow_060da1-triggerer-1
	docker network connect astro-airflow_060da1_default astro-airflow_060da1-postgres-1
