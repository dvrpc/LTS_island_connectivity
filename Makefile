# Load environment variables from .env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

LTS_DB=lts

data:
	@echo "importing initial data..."
	python lts_island_connectivity/bh_firewall_read_data.py 

islands:
	@echo "creating islands for all networks"
	python lts_island_connectivity/network_islands.py

backup:
	@echo "creating backup of database."
	${PG_DUMP_PATH} ${DATABASE_URL} > ${DUMP_PATH}

create-db:
	@echo "Creating new PostgreSQL database: ${LTS_DB}"
	createdb ${LTS_DB}
	
all: create-db data islands 
	@echo "running all scripts"
