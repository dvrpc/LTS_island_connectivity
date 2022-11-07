import geopandas as gpd
from env_vars import db, gis_db

# input a collection of segments from the desired "gap" layer, i.e. your study area segment.
# perhaps use create or replace view here? so it can be updated dynamically
# use st_collect here to generate a unified segment


study_segment_uids = (
    463947,
    463954,
    463958,
    466686,
    490605,
    215362,
    215363,
    215365,
    217928,
    241799,
)


def create_study_segment(lts_gaps_table):

    # todo: consider how dynamic selection will work in web app. clicking segments will probably add/remove values from study_segment_uids list
    """Creates a study segment based on uids.
    lts_gaps_table = the table with appropriate gaps for that island selection
    (i.e. if you're using the lts_1_islands layer, you would input the lts1gaps table, which includes LTS 2,3,4 as gaps)
    """

    db.execute(
        f"""drop table if exists study_segment;
            create table study_segment as 
                select st_collect(geom) as geom, avg(lts_score::int) 
                from {lts_gaps_table} where uid in {study_segment_uids};
        """
    )


def generate_proximate_blobs(islands_table):
    """evaluates which islands touch the study segment.
    islands_table is the table that includes the islands you want to look at (e.g. lts_1_2_islands for islands that include lts 1 and 2)
    """
    db.execute(
        f"""drop table if exists blobs;
            create table blobs as(    
            select st_convexhull(a.geom) as geom, a.uid, a.size_miles, a.rgba,a.muni_names, a.muni_count 
                from data_viz.{islands_table} a 
                inner join study_segment b
                on st_intersects(a.geom,b.geom))"""
    )


def pull_stat(column: str, table: str, geom_type: str):
    """
    grabs the identified attribute (population, school, etc) within the study area blobs.\
    
    :param str column: the column you want to pull data from in your database
    :param str table: the table you want to pull data from in your database
    :param str geom_type: the type (point, line, or polygon) of your data

    todo: 
    add line type handler
    """
    geom_type = geom_type.lower()

    if geom_type == "polygon":
        gdf = db.gdf(
            f"""select 
                a.{column},
                    b.*, 
                    st_area(st_intersection(a.geom, b.geom)) / st_area(a.geom) as pct_overlap,
                    round(st_area(st_intersection(a.geom, b.geom)) / st_area(a.geom) * {column}) as {column}_in_blobs
                from {table} a, blobs b 
                where st_intersects(a.geom, b.geom)
    """,
            "geom",
        )
        sum_poly = round(gdf[f"{column}_in_blobs"].sum())
        return sum_poly

    if geom_type == "point":
        gdf = db.gdf(
            f"""select 
                a.{column},
                    b.*
                from {table} a, blobs b 
                where st_intersects(a.geom, b.geom)"""
        )

        # sums the points in all blobs along study area
        point_sum = len(gdf.index)
        return point_sum


create_study_segment("lts2gaps")
generate_proximate_blobs("lts_1_2_islands")

print(pull_stat("type", "essential_services", "point"))
print(pull_stat("totpop2020", "censusblock2020_demographics", "polygon"))
