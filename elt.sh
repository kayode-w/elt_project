docker compose up init-airflow

sleep 5

docker compose up -d #command to start all services in detached mode. Could specify a service name 
#to start only that service (docker compose up init-airflow webserver scheduler)