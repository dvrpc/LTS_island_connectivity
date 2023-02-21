from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


dvrpc_ids = (
        485093,485094,485098,485097,485095,485096,485099,485100,514979,514980,485019,485020,474758,474757,485092,485091,474792,474791,474789,474790,485089,485090,514981,514982
        )


def create_study_segment(lts_gaps_table):

    # todo: consider how dynamic selection will work in web app. clicking segments will probably add/remove values from dvrpc_ids list
    """Creates a study segment based on uids.
    lts_gaps_table = the table with appropriate gaps for that island selection
    (i.e. if you're using the lts_1_islands layer, you would input the lts1gaps table, which includes LTS 2,3,4 as gaps)
    """

    # maybe remove gaps table and just pull from full network here
    db.execute(
        f"""drop table if exists study_segment;
            create table study_segment as 
                select st_collect(geom) as geom, avg(lts_score::int) 
                from {lts_gaps_table} where dvrpc_id in {dvrpc_ids};
        """
    )

def buffer_study_segment(distance:int=30):
    """Creates a buffer around the study segment. Default for distance is 30m (100 ft) assuming your data is using meteres"""
    
    db.execute(f"""drop table if exists study_segment_buffer CASCADE;
                create table study_segment_buffer as
                    select st_buffer(geom, {distance}) as geom from study_segment
                    """)


def generate_proximate_blobs(islands_table):
    """Evaluates which islands touch the study segment. returns total mileage of low-stress islands connected by new study_segment.
    islands_table is the table that includes the islands you want to look at (e.g. lts_1_2_islands for islands that include lts 1 and 2)

    """
    db.execute(
        f"""drop table if exists blobs;
            create table blobs as
            select st_concavehull(a.geom, .8) as geom, a.uid, a.size_miles, a.rgba,a.muni_names, a.muni_count 
                from data_viz.{islands_table} a 
                inner join study_segment b
                on st_intersects(a.geom,b.geom)
                where geometrytype(st_convexhull(a.geom)) = 'POLYGON'""",
    )
    mileage_q = """select sum(size_miles) from blobs"""
    return round(db.query_as_singleton(mileage_q))


def pull_stat(column: str, table: str, geom_type: str, polygon:str="blobs"):
    """
    grabs the identified attribute (population, school, etc) within the study area blobs.
    
    :param str column: the column you want to pull data from in your database
    :param str table: the table you want to pull data from in your database
    :param str geom_type: the type (point, line, or polygon) of your data
    :param str polygon: the polygon that has what you're interested in. default is blob, but could also be a buffer of road segment.

    """
    geom_type = geom_type.lower()

    if geom_type == "polygon":
        q = f"""
            with total as(
	            select round(st_area(st_intersection(a.shape, b.geom)) / st_area(a.shape) * a.{column}) as {column}_in_blobs
	            from {table} a, {polygon} b
	            where st_intersects (a.shape, b.geom))
            select round(sum({column}_in_blobs)) from total
    """
        sum_poly = db.query_as_singleton(q)
        return round(sum_poly)

    if geom_type == "point":
        df = db.df(
            f"""select count(a.{column}), a.{column} from {table} a, {polygon} b
                where st_intersects(a.shape, b.geom)
                group by a.{column}"""
        )
        # zips up dataframe containing count by column attribute of point
        df_dict = df.to_dict("records")
        return df_dict

    if geom_type == "line":
        df = db.df(
            f"""
                select a.{column}, st_length(a.shape)/1609 as miles from {table} a, {polygon} b
                    where st_intersects(a.shape, b.geom)
                    group by a.{column}, st_length(a.shape) 
                """
        )
        df_dict = df.to_dict("records")
        return df_dict

def pull_geometry(input_table:str, export_table:str, overlap_table:str, columns:str):
    
    """
    Creates a view of the needed geometry for later use by the API.

    """
    db.execute(f"""create or replace view geo_export_{export_table} as select {columns}, shape from {input_table} a inner join {overlap_table} b on st_intersects(a.shape, b.geom)""")
    print("geometry exported to views, ready to use")


if __name__ == "__main__":
    create_study_segment("lts2gaps")
    buffer_study_segment()
    print(generate_proximate_blobs("lts_1_2_islands"))
    print(pull_stat("type", "fdw_gis.eta_essentialservicespts", "point"))
    print(pull_stat("totpop2020", "fdw_gis.censusblock2020_demographics", "polygon"))
    print(pull_stat("hislat2020", "fdw_gis.censusblock2020_demographics", "polygon"))
    print(pull_stat("nonwhite", "fdw_gis.censusblock2020_demographics", "polygon"))
    print(pull_stat("circuit", "fdw_gis.circuittrails", "line"))
    print(pull_stat("munname", "fdw_gis.nets", "point"))
    print(pull_stat("bike", "fdw_gis.bikepedcrashes", "point", "study_segment_buffer"))
    print(pull_stat("ped", "fdw_gis.bikepedcrashes", "point", "study_segment_buffer"))
    pull_geometry("fdw_gis.bikepedcrashes", "bikepedcrashes", "study_segment_buffer", "bike, ped")
