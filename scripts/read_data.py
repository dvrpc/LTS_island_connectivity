"""
read_data.py
------------------
This script reads data from DVRPC's GIS portal
and inserts it into a Postgres database.

Requires geo-enabled postgres database (CREATE EXTENSION postgis;)
"""

import geopandas as gpd
from env_vars import ENGINE, GIS_ENGINE, db, gis_db


list_of_geos_to_clip = []


def import_data(
    sql_query=str,
    geom_col=str,
    full_layer_tablename=str,
    explode=True,
):
    print(f"initiating import of {full_layer_tablename}, please wait...")
    gdf = gis_db.gdf(sql_query, geom_col)
    gdf = gdf.to_crs(26918)
    db.import_geodataframe(
        gdf, full_layer_tablename, explode=explode, gpd_kwargs={"if_exists": "replace"}
    )
    list_of_geos_to_clip.append(full_layer_tablename)


def make_mask():
    mask_layer = db.gdf(
        """
        select index, mun_name, co_name, mun_type, landareaac, sq_feet, acres, geom from municipalboundaries  
        where mun_name like 'Evesham Township'
        or mun_name like 'Maple Shade Township'
        or mun_name like 'Mansfield Township'
        and co_name = 'Burlington'""",
        geom_col="geom",
    )
    mask_layer = mask_layer.to_crs(26918)
    return mask_layer


def clip_to_studyarea(table=str):
    print(f"clipping {table} to study area")
    db.execute(
        f"""
        drop table if exists {table}_clipped;
        create table {table}_clipped as(
            select a.* from {table} a
            inner join studyarea b
            on st_within(a.geom, b.geom)
            )
        """
    )


def make_low_stress_lts(lts_level=3):
    """Make a low stress network based on a certain threshold. Returns LTS network with
    all segments below specified lts_level (i.e. if write 'lts_level=3', it will create select LTS 1 and 2 as a new table.)"""
    gdf = db.gdf(
        f"select * from lts_full where lts_score::int < {lts_level}",
        geom_col="geom",
    )
    gdf = gdf.drop(columns=["level_0"])
    db.import_geodataframe(
        gdf, f"lts_stress_below_{lts_level}", gpd_kwargs={"if_exists": "replace"}
    )


def main():

    import_data(
        "select * from transportation.lts_network where typeno != '22' and typeno != '82'",
        "shape",
        full_layer_tablename="lts_full",
    )
    import_data(
        "select * from demographics.ipd_2020",
        "shape",
        full_layer_tablename="ipd_2020",
    )
    import_data(
        "select * from transportation.pedestriannetwork_lines",
        "shape",
        full_layer_tablename="ped_network",
    )
    import_data(
        "select * from boundaries.municipalboundaries",
        "shape",
        full_layer_tablename="municipalboundaries",
    )
    db.execute("drop table if exists studyarea")
    db.import_geodataframe(make_mask(), "studyarea")

    make_low_stress_lts(4)
    make_low_stress_lts(3)
    make_low_stress_lts(2)


if __name__ == "__main__":
    main()
