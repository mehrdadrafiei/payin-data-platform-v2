# Docker Hub is blocked from some networks: HUB_REGISTRY=<mirror> make up   (or pre-pull and `docker tag` the images)
CH = docker compose exec -T clickhouse clickhouse-client --user admin --password ch_pw --format PrettyCompact --multiquery
PG = docker compose exec -T postgres psql -U payin -d payin -v ON_ERROR_STOP=1
LABEL ?= baseline

.PHONY: up down reset ps status mem logs logs-seed psql ch connector-status sim-stop sim-start add-installments add-installments-default fetch-driver bootstrap-metabase restart-metabase benchmark benchmark-quiet benchmark-explain test test-unit test-e2e dbt-test dbt-freshness dbt-logs rebuild lag spike-rmt spike-hmac

up:  ## starts everything; blocks until the one-shot jobs (seed, connector, history) are done
	-./scripts/fetch_metabase_driver.sh || echo 'WARNING: ClickHouse driver jar missing in metabase/plugins/'
	docker compose up -d --build
	./scripts/wait_healthy.sh
	@echo
	@echo 'Started. Debezium copies the seeded rows in a few minutes (make status). Metabase: http://localhost:3000  login admin@payin.local / Payin-demo-2026!'

down:
	docker compose down

reset:  ## DESTROYS all data (volumes), including bronze
	docker compose down -v

ps:
	docker compose ps -a

status:
	./scripts/status.sh

mem:
	docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}'

logs:
	docker compose logs -f --tail=50

logs-seed:
	docker compose logs seed seed-history register-connector metabase-bootstrap

psql:
	docker compose exec postgres psql -U payin -d payin

ch:
	docker compose exec clickhouse clickhouse-client --user admin --password ch_pw

connector-status:
	curl -s http://localhost:8083/connectors/payin-cdc/status; echo

sim-stop:  ## quiet source (e.g. for exact reconciliation)
	docker compose stop simulator

sim-start:
	docker compose start simulator

fetch-driver:  ## ClickHouse driver for Metabase (needs GitHub once)
	./scripts/fetch_metabase_driver.sh

restart-metabase:  ## after adding the driver jar
	docker compose up -d --build --force-recreate metabase

bootstrap-metabase:  ## re-run the idempotent Metabase setup
	docker compose run --rm --no-deps metabase-bootstrap

benchmark:  ## times the cards through Metabase, simulator running. LABEL=baseline|optimized
	SIM_STATE=running docker compose run --rm cli python -m payin.benchmark --label $(LABEL)

benchmark-quiet:  ## same, but with the simulator stopped during the measurement
	docker compose stop simulator
	SIM_STATE=stopped docker compose run --rm cli python -m payin.benchmark --label $(LABEL)
	docker compose start simulator

benchmark-explain:  ## baseline timings plus EXPLAIN (ANALYZE, BUFFERS) of every baseline query
	SIM_STATE=running docker compose run --rm cli python -m payin.benchmark --label baseline --explain

add-installments:  ## schema change, nullable column (run ONE of the two)
	$(PG) -c "ALTER TABLE core.transactions ADD COLUMN installments INTEGER"

add-installments-default:  ## schema change with a default: old rows are NOT emitted by CDC (known trap)
	$(PG) -c "ALTER TABLE core.transactions ADD COLUMN installments INTEGER NOT NULL DEFAULT 1"

spike-rmt:
	$(CH) < spike/rmt_test.sql

spike-hmac:
	python3 spike/hmac_check.py
test: test-unit test-e2e

test-unit:  ## fast logic tests (no stack needed except the image)
	docker compose run --rm --no-deps cli python -m pytest -q tests/test_logic.py

test-e2e:  ## CDC correctness against the running stack (about 1-2 minutes)
	docker compose run --rm cli python -m pytest -v tests/e2e

dbt-test:  ## run all dbt data tests now (full scans, about a minute at 5M rows)
	docker compose exec transformer dbtw test

dbt-freshness:  ## is bronze receiving data?
	docker compose exec transformer dbtw source freshness

dbt-logs:  ## what the transformer loop is doing
	docker compose logs -f --tail=40 transformer

lag:  ## offsets of bronze / silver / gold for both tables
	./scripts/status.sh | sed -n '/transformer:/,$$p'

rebuild:  ## drop silver + gold and let the transformer rebuild them from bronze (bronze is NOT touched)
	docker compose stop transformer
	docker compose exec -T clickhouse clickhouse-client --user admin --password ch_pw --multiquery -q "DROP DATABASE IF EXISTS silver SYNC; DROP DATABASE IF EXISTS gold SYNC; CREATE DATABASE silver; CREATE DATABASE gold"
	docker compose start transformer
