import geopandas as gpd
from env_vars import db, gis_db

# input a collection of segments from the desired "gap" layer, i.e. your study area segment.

# study_segment_uids = (
#     463947,
#     463954,
#     463958,
#     466686,
#     490605,
#     215362,
#     215363,
#     215365,
#     217928,
#     241799,
# )
study_segment_uids = (
    323,
    324,
    325,
    326,
    327,
    245458,
    245459,
    245580,
    245581,
    245582,
    245583,
    245584,
    248834,
    248835,
    248836,
    248837,
    248838,
    248839,
    248840,
    494299,
    494300,
    494301,
)


def create_study_segment(lts_gaps_table):

    # todo: consider how dynamic selection will work in web app. clicking segments will probably add/remove values from study_segment_uids list
    """Creates a study segment based on uids.
    lts_gaps_table = the table with appropriate gaps for that island selection
    (i.e. if you're using the lts_1_islands layer, you would input the lts1gaps table, which includes LTS 2,3,4 as gaps)
    """

    # maybe remove gaps table and just pull from full network here
    db.execute(
        f"""drop table if exists study_segment;
            create table study_segment as 
                select st_collect(geom) as geom, avg(lts_score::int) 
                from {lts_gaps_table} where uid in {study_segment_uids};
        """
    )


def generate_proximate_blobs(islands_table):
    """evaluates which islands touch the study segment. returns total mileage of low-stress islands connected by new study_segment.
    islands_table is the table that includes the islands you want to look at (e.g. lts_1_2_islands for islands that include lts 1 and 2)

    """
    db.execute("drop table if exists blobs")
    gdf = db.gdf(
        f"""select st_convexhull(a.geom) as geom, a.uid, a.size_miles, a.rgba,a.muni_names, a.muni_count 
                from data_viz.{islands_table} a 
                inner join study_segment b
                on st_intersects(a.geom,b.geom)
                where geometrytype(st_convexhull(a.geom)) = 'POLYGON'""",
        geom_col="geom",
    )
    db.import_geodataframe(
        gdf, "blobs", gpd_kwargs={"if_exists": "replace"}, explode=True
    )
    mileage = gdf["size_miles"].sum()
    return round(mileage)


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
        df = db.df(
            f"""select count(a.{column}), a.{column} from {table} a, blobs b
                where st_intersects(a.geom, b.geom)
                group by a.{column}"""
        )
        # zips up dataframe containing count by column attribute of point
        df_dict = df.to_dict("records")
        return df_dict


create_study_segment("lts2gaps")
print(generate_proximate_blobs("lts_1_2_islands"))

print(pull_stat("type", "essential_services", "point"))
print(pull_stat("totpop2020", "censusblock2020_demographics", "polygon"))
