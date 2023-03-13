from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


dvrpc_ids = (
        448494,448493,401463,401464,401465,401466,401462,401461,405416,405415,405414,405413,405411,405412,405409,405410,405407,405408,405405,405406,405403,405404,405401,405402,405399,405400,405398,405397,405396,405395
        )

class StudySegment:
    def __init__(self, segment_ids: tuple, highest_comfort_level: int = 2) -> None:

        self.segment_ids = segment_ids
        self.highest_comfort_level = highest_comfort_level
        self.__create_study_segment()
        self.__buffer_study_segment()
        self.miles = self.__generate_proximate_blobs()
        self.has_isochrone = None
        self.__decide_scope()
        self.__handle_parking_lots()

        self.total_pop = self.pull_stat(
            "totpop2020", "fdw_gis.censusblock2020_demographics", "polygon"
        )
        self.nonwhite = self.pull_stat(
            "nonwhite", "fdw_gis.censusblock2020_demographics", "polygon"
        )
        self.hisp_lat = self.pull_stat(
            "hislat2020", "fdw_gis.censusblock2020_demographics", "polygon"
        )
        self.circuit = self.pull_stat("circuit", "fdw_gis.circuittrails", "line")
        self.jobs = self.pull_stat("coname", "fdw_gis.nets", "point")
        self.bike_crashes = self.pull_stat(
            "bike", "fdw_gis.bikepedcrashes", "point", "data_viz.study_segment_buffer"
        )
        self.ped_crashes = self.pull_stat(
            "ped", "fdw_gis.bikepedcrashes", "point", "data_viz.study_segment_buffer"
        )
        self.crash_export = self.pull_geometry(
            "fdw_gis.bikepedcrashes",
            "bikepedcrashes",
            "data_viz.study_segment_buffer",
            "bike, ped",
        )
        self.essential_serves = self.pull_stat(
            "type",
            "fdw_gis.eta_essentialservicespts",
            "point",
            "data_viz.parkinglot_union_lts_islands",
        )
        self.rail_stations = self.pull_stat(
            "type",
            "fdw_gis.passengerrailstations",
            "point",
            "data_viz.parkinglot_union_lts_islands",
        )

    def __create_study_segment(self):

        """
        Creates a study segment based on uids.

        lts_gaps_table = the table with appropriate gaps for that island selection
        (i.e. if you're using the lts_1_islands layer, you would input the lts1gaps table, which includes LTS 2,3,4 as gaps)
        """

        db.execute(
            f"""drop table if exists data_viz.study_segment;
                create table data_viz.study_segment as 
                    select st_collect(shape) as geom, avg(lts_score::int) 
                    from lts{self.highest_comfort_level}gaps where dvrpc_id in {self.segment_ids};
            """
        )

    def __buffer_study_segment(self, distance: int = 30):
        """
        Creates a buffer around the study segment. Default for distance is 30m (100 ft) assuming your data is using meteres"""

        db.execute(
            f"""drop table if exists data_viz.study_segment_buffer CASCADE;
                    create table data_viz.study_segment_buffer as
                        select st_buffer(geom, {distance}) as geom from data_viz.study_segment
                        """
        )

    def __generate_proximate_blobs(self):

        """
        Evaluates which islands touch the study segment. returns total mileage of low-stress islands connected by new study_segment.

        """
        db.execute(
            f"""drop table if exists blobs CASCADE;
                create table blobs as
                select st_concavehull(a.geom, .85) as geom, a.uid, a.size_miles, a.rgba,a.muni_names, a.muni_count 
                    from data_viz.lts_{self.highest_comfort_level} a 
                    inner join data_viz.study_segment_buffer b
                    on st_intersects(a.geom,b.geom)
                    where geometrytype(st_convexhull(a.geom)) = 'POLYGON';
                create or replace view data_viz.blobs_union as 
                    select 1 as uid, st_union(a.geom) as geom, sum(size_miles) from public.blobs a;
                    """,
        )
        mileage_q = """select sum(size_miles) from blobs"""
        return round(db.query_as_singleton(mileage_q))

    def __handle_parking_lots(self, join_table:str='blobs_union'):
        
        """
        Grabs proximate parking lots and their associated land uses, returns all.

        This helps avoid undercounting where essential services might not be on an island, but accessible from the segment via the parking lot."""
        
        if self.has_isochrone == True:
            join_table = 'isochrone'
        elif self.has_isochrone == False:
            join_table = 'blobs_union'

        db.execute(f"""
        create or replace view data_viz.proximate_lu as 
            select a.uid, a.geom from fdw_gis.landuse_selection a
                inner join data_viz.study_segment_buffer b
                on st_intersects (a.geom, b.geom);
        create or replace view data_viz.proximate_lu_and_touching as
            select 1 as uid, st_union(st_union(a.geom), st_union(b.geom)) as geom
                from data_viz.proximate_lu a
                inner join fdw_gis.landuse_selection b 
                on st_touches(a.geom, b.geom)
            where b.lu15subn like 'Parking%'
                or b.lu15subn like 'Institutional%'
                or b.lu15subn like 'Commercial%'
                or b.lu15subn = 'Recreation: General'
                or b.lu15subn = 'Transportation: Rail Right-of-Way'
                or b.lu15subn = 'Transportation: Facility';
        create or replace view data_viz.parkinglot_union_lts_islands as
            select 1 as uid, st_union(a.geom, b.geom) as geom from data_viz.proximate_lu_and_touching a, data_viz.{join_table} b;
        """
        )

    def __create_isochrone(self, travel_time:int=15):
        """
        Creates isochrone based on study_segment
        """
        db.execute(f"""
        drop materialized view if exists data_viz.isochrone;
        create materialized view data_viz.isochrone as 
         with nodes as (
         SELECT *
          FROM pgr_drivingDistance(
            'SELECT dvrpc_id as id, source, target, traveltime_min as cost FROM lts_stress_below_3',
            array(select "source" from lts3nodes a
                 inner join lts_stress_below_3 b
                 on a.id = b."source" 
                 where b.dvrpc_id in (449935,449936,402687,415783,415784)), 
             15, false) as di
         JOIN lts3nodes pt
         ON di.node = pt.id)
         select 1 as uid, st_concavehull(st_union(b.geom), .8) as geom from nodes a
         inner join lts_stress_below_3 b 
         on a.id = b."source"
                   """)


    def __decide_scope(self, mileage:int=1000):
        """
        Decides if isochrone should be created or not based on mileage of connected islands
        """
        if self.miles > mileage:
            self.__create_isochrone()
            self.has_isochrone = True
        else:
            self.has_isochrone = False


    def pull_stat(
        self, column: str, table: str, geom_type: str, polygon: str = "blobs"
    ):
        """
        grabs the identified attribute (population, school, etc) within the study area blobs.

        :param str column: the column you want to pull data from in your database
        :param str table: the table you want to pull data from in your database
        :param str geom_type: the type (point, line, or polygon) of your data
        :param str polygon: the polygon that has what you're interested in. default is blob, but could also be a buffer of road segment.

        """
        if self.has_isochrone == True and polygon == 'blobs':
            polygon = "data_viz.isochrone"
        elif self.has_isochrone == False and polygon != 'blobs':
            polygon = polygon
        
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
            return int(round(sum_poly, -2))
        
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
        
    def pull_geometry(
        self, input_table: str, export_table: str, overlap_table: str, columns: str
    ):

        """
        Creates a view of the needed geometry for later use by the API.

        """
        db.execute(
            f"""create or replace view data_viz.geo_export_{export_table} as select {columns}, shape from {input_table} a inner join {overlap_table} b on st_intersects(a.shape, b.geom)"""
        )
        return "geometry exported to views, ready to use"


a = StudySegment(dvrpc_ids)
attrs = vars(a)

for i in attrs:
    print(i, attrs[i])
