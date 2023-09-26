import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
HOST = os.getenv("HOST")
UN = os.getenv("UN")
DB_NAME = os.getenv("DB_NAME")
PW = os.getenv("PW")
PORT = os.getenv("PORT")

GIS_DATABASE_URL = os.getenv("GIS_DATABASE_URL")
GIS_HOST = os.getenv("GIS_HOST")
GIS_USER = os.getenv("GIS_USER")
GIS_DB_NAME = os.getenv("GIS_DB_NAME")
GIS_PASSWORD = os.getenv("GIS_PASSWORD")
GIS_PORT = os.getenv("GIS_PORT")
