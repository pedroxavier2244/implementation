# ETL System — Comandos de desenvolvimento e operação
# Uso: make <comando>

COMPOSE      = docker compose -f infra/docker-compose.yml
COMPOSE_HML  = docker compose -f infra/docker-compose.hml.yml --env-file .env.hml

.PHONY: up down build logs migrate shell-api shell-worker status \
        hml-up hml-down hml-logs hml-migrate hml-setup help

# ── Produção / Dev ────────────────────────────────────────────────────────────

up:                                 ## Sobe todos os serviços
	$(COMPOSE) up -d --build

down:                               ## Para todos os serviços
	$(COMPOSE) down

build:                              ## Reconstrói as imagens
	$(COMPOSE) build api worker-etl

logs:                               ## Logs em tempo real (api + worker)
	$(COMPOSE) logs -f api worker-etl

migrate:                            ## Aplica migrations Alembic
	$(COMPOSE) exec api alembic upgrade head

shell-api:                          ## Abre shell no container da API
	$(COMPOSE) exec api bash

shell-worker:                       ## Abre shell no container do worker
	$(COMPOSE) exec worker-etl bash

status:                             ## Status dos containers
	$(COMPOSE) ps

# ── HML ───────────────────────────────────────────────────────────────────────

hml-setup:                          ## Sobe HML do zero com dump de prod
	bash scripts/setup-hml.sh

hml-setup-quick:                    ## Sobe HML sem baixar novo dump
	bash scripts/setup-hml.sh --no-dump

hml-up:                             ## Sobe HML (sem rebuild)
	$(COMPOSE_HML) up -d

hml-down:                           ## Para HML
	$(COMPOSE_HML) down

hml-logs:                           ## Logs HML em tempo real
	$(COMPOSE_HML) logs -f api worker-etl

hml-migrate:                        ## Migrations no HML
	$(COMPOSE_HML) run --rm api alembic upgrade head

# ── Ajuda ─────────────────────────────────────────────────────────────────────

help:                               ## Lista todos os comandos
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
