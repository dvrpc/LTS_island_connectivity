# Load environment variables from .env file
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

data:
	@echo "importing initial data..."
	python scripts/bh_firewall_read_data.py 

islands:
	@echo "creating islands for all networks"
	python scripts/network_islands.py

backup:
	@echo "creating backup of database."
	${PG_DUMP_PATH} ${DATABASE_URL} > ${DUMP_PATH}
	
all: data islands 
	@echo "running all scripts"
