docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker-compose up -d
#docker exec $(docker ps -aqf "name=parser_scheduler_1") pip3 install -r /opt/airflow/dags/requirements.txt 
