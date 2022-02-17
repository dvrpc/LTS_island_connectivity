"""
read_data.py
------------------
This script reads data from DVRPC's GIS portal
and inserts it into a Postgres database.

Requires geo-enabled postgres database (CREATE EXTENSION postgis;)
"""

import geopandas as gpd
from geoalchemy2 import WKTElement
from env_vars import ENGINE


def read_lts():
    print("Gathering LTS Network")
    # import from GIS portal
    gdf = gpd.read_file(
        "https://opendata.arcgis.com/datasets/553b8f833da94bec99e64a28be12f34d_0.geojson"
    )

    # remove null geometries
    gdf = gdf[gdf.geometry.notnull()]

    # transform projection from 4326 to 26918
    gdf = gdf.to_crs(epsg=26918)

    # create geom column for postgis import
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=26918))

    # write geodataframe to postgis
    gdf.to_postgis("lts_network", con=ENGINE, if_exists="replace")


def read_ipd():
    print("Gathering IPD Tracts")
    # import from GIS portal
    gdf = gpd.read_file(
        "https://opendata.arcgis.com/datasets/60d8d376bbd942088b64a34794ef68ca_0.geojson"
    )

    # remove null geometries
    gdf = gdf[gdf.geometry.notnull()]

    # transform projection from 4326 to 26918
    gdf = gdf.to_crs(epsg=26918)

    # create geom column for postgis import
    gdf["geom"] = gdf["geometry"].apply(lambda x: WKTElement(x.wkt, srid=26918))

    # write geodataframe to postgis
    gdf.to_postgis("ipd_tracts", con=ENGINE, if_exists="replace")


def main():
    read_lts()
    read_ipd()


if __name__ == "__main__":
    main()
