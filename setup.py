from setuptools import find_packages, setup

setup(
    name="lts-island-connectivity",
    version="0.1.2",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "geoalchemy2",
        "geopandas",
        "network-routing @ git+https://github.com/dvrpc/network-routing",
        "pandana",
        "pg-data-etl @ git+https://github.com/mmorley0395/pg-data-etl",
        "pip-chill",
        "psycopg2-binary",
        "python-dotenv",
        "tqdm",
        "requests",
    ],
    entry_points={"console_scripts": ["connect = lts_island_connectivity.cli:cx"]},
)
