import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
HOST = os.getenv('HOST')
USER = os.getenv('USER')
DB_NAME = os.getenv('DB_NAME')
PASSWORD = os.getenv('PASSWORD')
PORT = os.getenv('PORT')

GIS_DATABASE_URL = os.getenv('GIS_DATABASE_URL')
GIS_HOST = os.getenv('GIS_HOST')
GIS_USER = os.getenv('GIS_USER')
GIS_DB_NAME = os.getenv('GIS_DB_NAME')
GIS_PASSWORD = os.getenv('GIS_PASSWORD')
GIS_PORT = os.getenv('GIS_PORT')
