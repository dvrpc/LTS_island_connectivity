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


create_study_segment("lts2gaps")
generate_proximate_blobs("lts_1_2_islands")


# pseduocode / todo
# convex hull around each island, grab census block data
# return report of how many people
# bigger idea: run this for every single gap in a study area, return a table of gaps prioritized by x factor
