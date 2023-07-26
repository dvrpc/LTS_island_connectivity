from pg_data_etl import Database
import pandas as pd
import json

db = Database.from_config("lts", "localhost")

# network_type = input("what is the network type?")
# dvrpc_ids = input("what are the segment ids?")
# dvrpc_ids = tuple(int(x) for x in dvrpc_ids.split(","))
name = input("what is the segment name?")
# username = input(
#     "what is your first initial, last name? e.g., jane roberts = jroberts")


class StudySegment:
    def __init__(
        self,
        network_type: str,
        segment_ids: tuple,
        segment_name: str,
        username: str,
        highest_comfort_level: int = 2,
    ) -> None:
        self.network_type = network_type
        self.segment_ids = segment_ids
        self.segment_name = segment_name
        self.username = username
        self.highest_comfort_level = highest_comfort_level
        self.__setup_study_segment_tables()
        self.__create_study_segment()
        self.__buffer_study_segment()
        self.__generate_proximate_islands()
        # self.miles = self.__generate_proximate_blobs()
        # self.has_isochrone = None
        # self.__decide_scope()
        # self.__handle_parking_lots()

        # self.total_pop = self.pull_stat(
        #     "totpop2020", "censusblock2020_demographics", "polygon"
        # )
        # self.nonwhite = self.pull_stat(
        #     "nonwhite", "censusblock2020_demographics", "polygon"
        # )
        # self.hisp_lat = self.pull_stat(
        #     "hislat2020", "censusblock2020_demographics", "polygon"
        # )
        # self.circuit = self.pull_stat("circuit", "circuittrails", "line")
        # self.jobs = self.pull_stat("coname", "nets", "point")
        # self.bike_crashes = self.pull_stat(
        #     "bike", "bikepedcrashes", "point", "data_viz.study_segment_buffer"
        # )
        # self.ped_crashes = self.pull_stat(
        #     "ped", "bikepedcrashes", "point", "data_viz.study_segment_buffer"
        # )
        # self.crash_export = self.pull_geometry(
        #     "bikepedcrashes",
        #     "bikepedcrashes",
        #     "data_viz.study_segment_buffer",
        #     "bike, ped",
        # )
        # self.essential_services = self.pull_stat(
        #     "type",
        #     "eta_essentialservicespts",
        #     "point",
        #     "data_viz.parkinglot_union_lts_islands",
        # )
        # self.rail_stations = self.pull_stat(
        #     "type",
        #     "passengerrailstations",
        #     "point",
        #     "data_viz.parkinglot_union_lts_islands",
        # )
        # self.pull_islands()
        # self.summarize_stats()
        # self.convert_wkt_to_geom()

    def __setup_study_segment_tables(self):
        for value in ["user_segments", "user_buffers", "user_islands", "user_blobs"]:
            if value == 'user_segments':
                seg_ids = "seg_ids INTEGER[],"
                seg_name = "seg_name VARCHAR,"
            else:
                seg_ids = ""
                seg_name = ""

            query = f"""
                CREATE TABLE IF NOT EXISTS {self.network_type}.{value}(
                    id SERIAL PRIMARY KEY,
                    username VARCHAR,
                    {seg_ids}
                    {seg_name}
                    geom GEOMETRY
                );
            """
            db.execute(query)

    def __check_segname(self):
        """Checks to see if segment is already in DB"""

        segs = db.query(
            f'select seg_name from {self.network_type}.user_segments')

        # flattens list returned from db.query
        flat_segs = [item for sublist in segs for item in sublist]

        return flat_segs

    def __create_study_segment(self):
        """
        Creates a study segment based on uids.

        lts_gaps_table = the table with appropriate gaps for that island selection
        (i.e. if you're using the lts_1_islands layer, you would input the lts1gaps table, which includes LTS 2,3,4 as gaps)
        """
        if self.network_type == "sidewalk":
            gaps_table = f"{self.network_type}.ped_network_gaps"
            ids = "objectid"
        elif self.network_type == "lts":
            gaps_table = f"{self.network_type}.lts{self.highest_comfort_level}gaps"
            ids = "dvrpc_id"
        else:
            print("something went wrong, pick lts or sidewalk for self.network_type.")

        gaps = db.query_as_singleton(
            f"select st_collect(geom) as geom from {gaps_table} where {ids} in {self.segment_ids}")

        self.segment_ids = list(self.segment_ids)

        db_segments = self.__check_segname()

        if self.segment_name in db_segments:
            raise ValueError("Value must be unique")
        else:
            db.execute(
                f"""
                INSERT INTO {self.network_type}.user_segments 
                (id, username, seg_ids, seg_name, geom)
                VALUES (DEFAULT, %s, %s, %s, %s)
                """, (self.username, self.segment_ids, self.segment_name, gaps)
            )

    def __buffer_study_segment(self, distance: int = 30):
        """
        Creates a buffer around the study segment. 
        Default for distance is 30m (100 ft) assuming your data is using meters
        """

        db.execute(
            f"""
                insert into {self.network_type}.user_buffers
                select id, username, st_buffer(geom, {distance}) as geom 
                from {self.network_type}.user_segments
                where seg_name = '{self.segment_name}'
            """
        )

    def __generate_proximate_islands(self):
        """
        Finds islands proximate to study segment and adds them to the user_islands table
        in the DB
        """

    def __generate_proximate_blobs(self):
        """
        Evaluates which islands touch the study segment. 
        Returns total mileage of low-stress islands connected by new study_segment.

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
                    select 1 as uid, st_union(st_union(a.geom, b.geom)) as geom from public.blobs a, data_viz.study_segment_buffer b
                    """,
        )
        mileage_q = """select sum(size_miles) from blobs"""
        return round(db.query_as_singleton(mileage_q))

    def __handle_parking_lots(self, join_table: str = "blobs_union"):
        """
        Grabs proximate parking lots and their associated land uses, returns all.

        This helps avoid undercounting where essential services might not be on an
        island, but accessible from the segment via the parking lot.
        """

        if self.has_isochrone == True:
            join_table = "isochrone"
        elif self.has_isochrone == False:
            join_table = "blobs_union"

        db.execute(
            f"""
        create or replace view data_viz.proximate_lu as 
            select a.uid, a.geom from landuse_selection a
                inner join data_viz.study_segment_buffer b
                on st_intersects (a.geom, b.geom);
        create or replace view data_viz.proximate_lu_and_touching as
            select 1 as uid, st_union(st_union(a.geom), st_union(b.geom)) as geom
                from data_viz.proximate_lu a
                inner join landuse_selection b 
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

    def __low_stress_touching_study_buffer(self):
        """Study segment does not exist in lts_stress_below segment, so grab any touching segments that do."""

        touching_segs = db.query_as_singleton(
            f"""
        select array_agg(dvrpc_id)
        from data_viz.study_segment_buffer a 
        inner join lts_stress_below_{self.highest_comfort_level} b
        on st_intersects(a.geom, b.geom)
        """
        )

        return touching_segs

    def __create_isochrone(self, travel_time: int = 15):
        """
        Creates isochrone based on study_segment
        """
        ls_touching_segment = self.__low_stress_touching_study_buffer()
        ls_touching_segment = tuple(ls_touching_segment)

        db.execute(
            f"""
        drop materialized view if exists data_viz.isochrone;
        create materialized view data_viz.isochrone as 
         with nodes as (
         SELECT *
          FROM pgr_drivingDistance(
            'SELECT dvrpc_id as id, source, target, traveltime_min as cost FROM lts_stress_below_3',
            array(select "source" from lts3nodes a
                 inner join lts_stress_below_3 b
                 on a.id = b."source" 
                 where b.dvrpc_id in {ls_touching_segment}), 
             15, false) as di
         JOIN lts3nodes pt
         ON di.node = pt.id)
         select 1 as uid, st_concavehull(st_union(st_centroid(b.geom)), .8) as geom from nodes a
         inner join lts_stress_below_3 b 
         on a.id = b."source"
                   """
        )

    def __decide_scope(self, mileage: int = 1000):
        """
        Decides if isochrone should be created or not based on mileage of connected islands
        """
        if self.miles > mileage:
            self.__create_isochrone()
            self.has_isochrone = True
        else:
            self.has_isochrone = False

    def pull_stat(
        self,
        column: str,
        table: str,
        geom_type: str,
        polygon: str = "data_viz.blobs_union",
    ):
        """
        grabs the identified attribute (population, school, etc) within the study area blobs.

        :param str column: the column you want to pull data from in your database
        :param str table: the table you want to pull data from in your database
        :param str geom_type: the type (point, line, or polygon) of your data
        :param str polygon: the polygon that has what you're interested in. default is blobs_union, but could also be a buffer of road segment.

        """
        if self.has_isochrone == True and polygon == "data_viz.blobs_union":
            polygon = "data_viz.isochrone"
        elif self.has_isochrone == False and polygon != "data_viz.blobs_union":
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

    def pull_islands(self):
        """Pulls islands connecting to study segment, returns geojson for use in web viewer."""

        db.execute(
            f"""create or replace view data_viz.accessed_islands as
            select 1 as uid, st_union(a.geom) as geom
            from data_viz.lts_{self.highest_comfort_level} a 
            inner join data_viz.study_segment_buffer b
            on st_intersects(a.geom,b.geom)
            where geometrytype(st_convexhull(a.geom)) = 'POLYGON';
            """
        )
        geojson = db.query_as_singleton(
            """select st_asgeojson(geom) from data_viz.accessed_islands"""
        )

        return geojson

    def pull_geometry(
        self, input_table: str, export_table: str, overlap_table: str, columns: str
    ):
        """
        Creates a view of the needed geometry for later use by the API.

        """
        db.execute(
            f"""create or replace view data_viz.geo_export_{export_table} as 
            select {columns}, shape from {input_table} a 
            inner join {overlap_table} b on st_intersects(a.shape, b.geom)"""
        )
        return "geometry exported to views, ready to use"

    def summarize_stats(self):
        """
        Summarizes all connections that a segment makes
        """
        attrs = vars(self)
        df = pd.json_normalize(json.loads(json.dumps(attrs, indent=2)))
        for value in [
            "circuit",
            "jobs",
            "essential_services",
            "rail_stations",
            "bike_crashes",
            "ped_crashes",
        ]:
            df[f"{value}"] = df[f"{value}"].apply(json.dumps)
        col_to_move = df.pop("geom")
        df.insert(len(df.columns), "geom", col_to_move)

        db.import_dataframe(df, "summaries.all", {"if_exists": "append"})

    def convert_wkt_to_geom(self):
        q1 = """alter table summaries.all alter column geom type geometry"""
        db.execute(q1)


a = StudySegment("sidewalk", (20, 21), name, "mmorley")

b = StudySegment("lts", (540277, 540278, 540280, 540279,
                 540276, 540275, 510870, 510869), name, "mmorley")
