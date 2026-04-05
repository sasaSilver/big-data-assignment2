stop:
	docker compose stop

down:
	docker compose down

down-all:
	docker compose down --volumes
	rm -rf app/data/*
	rm app/index.txt
	rm app/.index.txt.crc

up:
	docker compose up -d
	docker exec -it cluster-master bash -c "bash start-services.sh"

sh:
	docker exec -it cluster-master bash

prepare-data:
	docker exec -it cluster-master bash -c "bash prepare_data.sh"

index:
	docker exec -it cluster-master bash -c "bash index.sh"

search:
	docker exec -it cluster-master bash -c "bash search.sh \"$(q)\""

run-app:
	docker compose run --service-ports --name cluster-master --entrypoint bash cluster-master /app/app.sh

add-to-index:
	docker exec -it cluster-master bash -c "bash add_to_index.sh \"$(f)\""

fmt:
	ruff format .
	ruff check --fix .
