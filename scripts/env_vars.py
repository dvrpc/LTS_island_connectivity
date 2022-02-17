import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine

load_dotenv(find_dotenv())

POSTGRES_URL = os.getenv("postgres_url")
ENGINE = create_engine(POSTGRES_URL)
DATA_ROOT = os.getenv("data_root")
