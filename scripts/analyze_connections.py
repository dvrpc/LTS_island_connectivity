from pg_data_etl import Database
db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


dvrpc_ids = (
    578711,
    578712,
    408624,
    408623,
    449502,
    449501,
    449506,
    449505,
    449503,
    449504,
    449511,
    449512,
    449510,
    449509,
    449508,
    449507,
    399718,
    399717,
    399763,
    399764,
    399730,
    399729,
    399671,
    399672,
    449514,
    449513,
    402969,
    402970,
    399782,
    399781,
    399740,
    399739,
    402968,
    402967,
    399722,
    399721,
    399773,
    399774,
    449516,
    449515,
    449518,
    449517,
    421871,
    421872,
    449522,
    449521,
    449520,
    449519,
    405807,
    405808,
    418490,
    418489,
    449526,
    449525,
    402964,
    402963,
    410816,
    410815,
    449523,
    449524,
    405806,
    405805,
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


def generate_proximate_blobs(islands_table):
    """evaluates which islands touch the study segment. returns total mileage of low-stress islands connected by new study_segment.
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
    return db.query_as_singleton(mileage_q) 


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
        q = f"""
            with total as(
	            select round(st_area(st_intersection(a.geom, b.geom)) / st_area(a.geom) * a.totpop2020) as {column}_in_blobs
	            from {table} a, blobs b
	            where st_intersects (a.geom, b.geom))
            select sum({column}_in_blobs) from total
    """
        sum_poly = db.query_as_singleton(q) 
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


if __name__ == "__main__":
    create_study_segment("lts2gaps")
    print(generate_proximate_blobs("lts_1_2_islands"))
    print(pull_stat("type", "essential_services", "point"))
    print(pull_stat("totpop2020", "censusblock2020_demographics", "polygon"))
