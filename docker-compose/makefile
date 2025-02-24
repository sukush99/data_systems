#for live data
build-docker-daily:
	docker compose -f docker-daily.yml build

start-docker-daily: build-docker-daily
	docker compose -f docker-daily.yml up -d

stop-docker-daily:
	docker compose -f docker-daily.yml down


#for live data
build-docker-backfill:
	docker compose -f docker-backfill.yml build

start-docker-backfill: build-docker-backfill
	docker compose -f docker-backfill.yml up -d

stop-docker-backfill:
	docker compose -f docker-backfill.yml down