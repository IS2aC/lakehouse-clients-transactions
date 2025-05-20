WORKERS ?= 3 # number of workers for scaling
AIRFLOW_NETWORK ?= astro-airflow_060da1_airflow

build:
	docker compose build  

build-nc:
	docker compose build --no-cache

build-progress:
	docker compose build --no-cache --progress=plain

down:
	docker compose down

run:
	make down && docker compose up

run-scaled:
	make down && docker compose up --scale spark-worker=$(WORKERS)

run-d:
	make down && docker compose up -d

stop:
	docker compose stop

submit:
	docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)

log-master:
	docker logs -f da-spark-master

# command shortcut for connection to airflow network
connect-airflow-network:
	docker network connect $(AIRFLOW_NETWORK) da-spark-master
	docker network connect $(AIRFLOW_NETWORK) cluster-spark-spark-worker-1
	docker network connect $(AIRFLOW_NETWORK) cluster-spark-spark-worker-2
	docker network connect $(AIRFLOW_NETWORK) cluster-spark-spark-worker-3
