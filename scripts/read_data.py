"""
read_data.py
------------------
This script reads data from DVRPC's GIS portal
and inserts it into a Postgres database.

Requires geo-enabled postgres database (CREATE EXTENSION postgis;)
"""

from cmath import exp
import geopandas as gpd
from geoalchemy2 import WKTElement
from env_vars import ENGINE, GIS_ENGINE, db, gis_db
from pg_data_etl import Database
import os
from pathlib import Path


mask_layer = gis_db.gdf(
    """
    select * from boundaries.municipalboundaries m 
    where mun_name like 'Evesham Township'
    or mun_name like 'Maple Shade Township'
    or mun_name like 'Mansfield Township'
    and co_name = 'Burlington'""",
    geom_col="shape",
)
mask_layer = mask_layer.to_crs(26918)


def import_and_clip(
    sql_query=str,
    geom_col=str,
    full_layer_tablename=str,
    clipped_layer_tablename=str,
    gpd_kwargs={"if_exists": "replace"},
):
    gdf = gis_db.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    clipped = gpd.clip(gdf, mask_layer)
    print(f"importing {full_layer_tablename}, please wait...")
    db.import_geodataframe(
        gdf, full_layer_tablename, explode=True, gpd_kwargs={"if_exists": "replace"}
    )
    print(f"clipping {full_layer_tablename}, please wait...")
    db.import_geodataframe(
        clipped, clipped_layer_tablename, gpd_kwargs=gpd_kwargs, explode=True
    )


def main():
    import_and_clip(
        "select * from transportation.lts_network",
        "shape",
        full_layer_tablename="lts_full",
        clipped_layer_tablename="lts_clipped",
        gpd_kwargs={"if_exists": "replace"},
    )
    import_and_clip(
        "select * from demographics.ipd_2020",
        "shape",
        full_layer_tablename="ipd_2020",
        clipped_layer_tablename="ipd_2020_clipped",
        gpd_kwargs={"if_exists": "replace"},
    )


if __name__ == "__main__":
    main()
