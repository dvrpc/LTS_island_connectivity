data:
	@echo "importing initial data..."
	conda activate connectivity
	python scripts/read_data.py 

islands:
	@echo "creating islands for all networks"
	python scripts/network_islands.py

all: data islands 
	@echo "running all scripts"
