import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from pg_data_etl import Database

load_dotenv(find_dotenv())

# if using env vars
POSTGRES_URL = os.getenv("postgres_url")
GIS_URL = os.getenv("gis_url")
ENGINE = create_engine(POSTGRES_URL)
GIS_ENGINE = create_engine(GIS_URL)
DATA_ROOT = os.getenv("data_root")


# if using PG ETL library
db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")
data_folder = Path(os.getenv("data_root"))
