# Contrato de ejecución de la prueba (ver readme.md).
# Implementa cada objetivo como quieras (scripts, dbt, Snowflake CLI, docker...), pero respeta sus nombres,
# sus parámetros y su comportamiento: la evaluación los ejecuta tal cual.

SHELL := /bin/bash
-include .env
export

.PHONY: help source deploy run-day backfill stream test api web mcp stop selfcheck load-test

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

source: ## Levanta el sistema origen (localhost:3001) y Kafka (localhost:19092)
	docker compose up -d --build redpanda source-system

deploy: ## Crea o actualiza tus objetos en Snowflake (idempotente)
	@echo "TODO: implementa 'make deploy'"; exit 1

run-day: ## make run-day DATE=YYYY-MM-DD -> integra lo publicado hasta DATE (incluido)
	@test -n "$(DATE)" || (echo "Uso: make run-day DATE=YYYY-MM-DD"; exit 2)
	@echo "TODO: implementa 'make run-day' ($(DATE))"; exit 1

backfill: ## make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD
	@test -n "$(FROM)" -a -n "$(TO)" || (echo "Uso: make backfill FROM=YYYY-MM-DD TO=YYYY-MM-DD"; exit 2)
	@echo "TODO: implementa 'make backfill' ($(FROM)..$(TO))"; exit 1

stream: ## Arranca tu procesamiento de eventos en segundo plano y devuelve el control
	@echo "TODO: implementa 'make stream'"; exit 1

test: ## Tus tests (unitarios, de datos, de la API...)
	@echo "TODO: implementa 'make test'"; exit 1

api: ## Levanta tu API en :5000 en segundo plano y devuelve el control
	@echo "TODO: implementa 'make api'"; exit 1

web: ## Levanta tu dashboard en :8080 en segundo plano y devuelve el control
	@echo "TODO: implementa 'make web'"; exit 1

mcp: ## (Opcional) Arranca tu servidor MCP en :7000 en segundo plano
	@echo "No implementado (opcional)"; exit 1

stop: ## Para la API, el dashboard, el procesamiento de eventos y el MCP
	@echo "TODO: implementa 'make stop'"; exit 1

selfcheck: ## Comprueba que podemos leer tu vista, tu topic y tu API (API=... KAFKA=...)
	python -m pip install -q -r selfcheck/requirements.txt
	python selfcheck/selfcheck.py $(if $(API),--api $(API),) $(if $(KAFKA),--kafka $(KAFKA),)

load-test: ## Lanza el test de carga k6 contra tu API (TARGET_URL=http://host.docker.internal:5000)
	docker compose --profile test run --rm load-test
