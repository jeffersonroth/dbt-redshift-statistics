.PHONY postgres-mock

postgres-mock:
	docker compose -f ./postgres-mock/docker-compose.yaml up --build