from pg_data_etl import Database
import pandas as pd
import json
from geoalchemy2 import WKTElement
from sqlalchemy import create_engine, text

db = Database.from_config("lts", "localhost")


class StudySegment:
    def __init__(
        self,
        network_type: str,
        feature: dict,
        username: str,
        highest_comfort_level: int = 2,
    ) -> None:
        self.highest_comfort_level = highest_comfort_level
        self.network_type = network_type
        self.feature = feature
        self.geometry = feature['geometry']
        self.properties = feature['properties']
        self.segment_name = self.properties['name']
        self.username = username
        self.__setup_study_segment_tables()
        self.study_segment_id = self.__create_study_segment(
            self.geometry, self.username, self.network_type)
        self.__buffer_study_segment()
        self.__generate_proximate_islands()
        self.__generate_proximate_blobs()
        self.has_isochrone = None
        self.miles = self.__generate_mileage()
        self.__decide_scope()
        self.__handle_parking_lots()

        self.total_pop = self.pull_stat(
            self.study_segment_id, "totpop2020", "censusblock2020_demographics", "polygon")
        # self.nonwhite = self.pull_stat(
        #     self.study_segment_id, "nonwhite", "censusblock2020_demographics", "polygon"
        # )
        self.hisp_lat = self.pull_stat(
            self.study_segment_id, "hislat2020", "censusblock2020_demographics", "polygon"
        )
        self.circuit = self.pull_stat(
            self.study_segment_id, "circuit", "circuittrails", "line")
        self.jobs = self.pull_stat(
            self.study_segment_id, "coname", "nets_2015", "point")
        self.bike_crashes = self.pull_stat(
            self.study_segment_id, "bike", "bikepedcrashes", "point",
        )
        self.ped_crashes = self.pull_stat(
            self.study_segment_id, "ped", "bikepedcrashes", "point",
        )
        self.essential_services = self.pull_stat(
            self.study_segment_id,
            "type",
            "essential_services",
            "point",
        )
        self.rail_stations = self.pull_stat(
            self.study_segment_id,
            "type",
            "passengerrailstations",
            "point",
        )
        self.summarize_stats()

    def __setup_study_segment_tables(self):
        for value in ["user_segments",
                      "user_buffers",
                      "user_islands",
                      "user_blobs",
                      "user_isochrones"]:
            if value == 'user_segments':
                seg_name = "seg_name VARCHAR,"
            else:
                seg_name = ""

            query = f"""
                CREATE TABLE IF NOT EXISTS {self.network_type}.{value}(
                    id SERIAL PRIMARY KEY,
                    username VARCHAR,
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

    def __create_study_segment(self, geojson_dict: dict, username: str, network_type: str):
        """
        Creates a study segment / study segments based on user's drawn geometry.

        """
        engine = create_engine(db.uri)

        segment_name = feature.get('properties')['name']

        db_segments = self.__check_segname()

        if self.segment_name in db_segments:
            raise ValueError(
                "Project name was already used. Try another name.")

        if self.geometry.get('type') == 'LineString':
            coordinates = self.geometry.get('coordinates', [])

            # Convert coordinates to WKT LineString format
            coord_str = ", ".join([f"{x} {y}" for x, y in coordinates])
            line_wkt = f"LINESTRING({coord_str})"

            wkt_element = WKTElement(line_wkt, srid=4326)

            table_name = f"{network_type}.user_segments"
            query = f"""
            INSERT INTO {table_name}
            (id, username, seg_name, geom)
            VALUES (DEFAULT, :username, :seg_name, ST_Transform(ST_GeomFromText(:geom, 4326), 26918))
            """

            query = text(query)

            engine.execute(query, username=username,
                           seg_name=segment_name, geom=wkt_element.desc)

        print("creating study segment, please wait..")

        study_segment_id = db.query_as_singleton(f"""
            select id from {self.network_type}.user_segments a
            where a.seg_name = '{self.segment_name}'""")

        return study_segment_id

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
        Finds islands proximate to study segment buffer and adds them to the
        user_islands table in the DB. each uid in islands_uids is one island, not the
        ids of the underlying segments.
        """

        print("generating proximate islands, please wait..")

        db.execute(
            f"""
                alter table {self.network_type}.user_islands
                add column if not exists size_miles float;
                insert into {self.network_type}.user_islands
                    select
                        a.id,
                        a.username,
                        st_collectionextract(st_collect(b.geom)) as geom,
                        sum(b.size_miles)
                    from {self.network_type}.user_buffers a
                    inner join {self.network_type}.{self.network_type}{self.highest_comfort_level}_islands b
                    on st_intersects(a.geom,b.geom)
                    inner join {self.network_type}.user_segments c
                    on a.id = c.id
                    where c.seg_name = '{self.segment_name}'
                    group by a.id
            """)

    def __generate_proximate_blobs(self):
        """
        Creates 'blobs' around each island collection. 
        Note that this query also unions the islands with the study segment buffer.

        """

        db.execute(
            f"""
                insert into {self.network_type}.user_blobs
                select a.id, a.username, st_union(st_concavehull(a.geom, .85), c.geom) as geom
                from {self.network_type}.user_islands a
                inner join {self.network_type}.user_segments b
                on a.id = b.id
                inner join {self.network_type}.user_buffers c
                on a.id = c.id
                where b.seg_name = '{self.segment_name}'
            """
        )

    def __handle_parking_lots(self):
        """
        Grabs proximate parking lots and their associated land uses, returns all.

        This helps avoid undercounting where essential services might not be on an
        island, but accessible from the segment via the parking lot and the parking
        lot's adjacent land use. 

        The first cte grabs proximate land uses to the join table (blobs or isochrone). 

        The second cte grabs relevent land uses that touch those segments. 

        Join table is updated with the geom of both plus the original join data.

        Exterior ring query removes holes formed by unmatched LUs. 

        """
        print("folding in proximate parking lots and associated lu's, please wait..")

        if self.has_isochrone is True:
            join_table = f"{self.network_type}.user_isochrones"
        elif self.has_isochrone is False:
            join_table = f"{self.network_type}.user_blobs"

        db.execute(
            f"""
                WITH proximate_lu AS (
                    SELECT ST_Union(a.geom) as geom, c.id, c.seg_name
                    FROM landuse_2015 a
                    INNER JOIN {join_table} b
                    ON ST_Intersects(a.geom, b.geom)
                    INNER JOIN {self.network_type}.user_segments c
                    ON b.id = c.id
                    WHERE c.seg_name = '{self.segment_name}'
                    AND (
                        a.lu15subn LIKE 'Parking%'
                        OR a.lu15subn LIKE 'Institutional%'
                        OR a.lu15subn LIKE 'Commercial%'
                        OR a.lu15subn = 'Recreation: General'
                        OR a.lu15subn = 'Transportation: Rail Right-of-Way'
                        OR a.lu15subn = 'Transportation: Facility')
                    GROUP BY c.id, c.seg_name
                ),
                proximate_lu_and_touching AS (
                    SELECT a.id, a.seg_name, ST_Union(b.geom) as geom
                    FROM proximate_lu a
                    INNER JOIN landuse_2015 b
                    ON ST_Touches(a.geom, b.geom)
                    WHERE a.seg_name = '{self.segment_name}'
                    AND (
                        b.lu15subn LIKE 'Parking%'
                        OR b.lu15subn LIKE 'Institutional%'
                        OR b.lu15subn LIKE 'Commercial%'
                        OR b.lu15subn = 'Recreation: General'
                        OR b.lu15subn = 'Transportation: Rail Right-of-Way'
                        OR b.lu15subn = 'Transportation: Facility')
                    GROUP BY a.id, a.seg_name
                )
                UPDATE {join_table} AS b
                SET geom = st_makepolygon(st_exteriorring(ST_Union(ST_Union(a.geom, c.geom), b.geom)))
                FROM proximate_lu a
                INNER JOIN proximate_lu_and_touching c
                ON a.id = c.id
                WHERE b.id = a.id;

        """
        )

    def __create_isochrone(self, travel_time: int = 15):
        """
        Creates isochrone based on study_segment
        """

        db.execute(
            f"""
            insert into {self.network_type}.user_isochrones
            WITH arrays AS (
                select a.id as id, --id of user segment, tied to blobs, buffer, etc
                a.username,
                a.seg_name,
                array_agg(c.{self.ids}) as ids --ids in low stress table
                FROM {self.network_type}.user_segments a
                INNER JOIN {self.network_type}.user_buffers b ON a.id = b.id
                INNER JOIN {self.ls_table} c ON st_intersects(b.geom, c.geom)
                WHERE a.seg_name = '{self.segment_name}'
                group by a.id
            ),
            nodes AS (
                SELECT *
                FROM pgr_drivingDistance(
                    'SELECT {self.ids} as id, source, target, traveltime_min as cost FROM {self.ls_table}', -- example: ls_stress_below_3
                    (SELECT array_agg("source") FROM {self.network_type}.{self.nodes_table} a
                     INNER JOIN {self.ls_table} b ON a.id = b."source"
                     WHERE b.{self.ids}= ANY((SELECT ids FROM arrays)::integer[])), -- Using ANY with integer array
                    {travel_time}, false
                ) AS di
                JOIN {self.network_type}.{self.nodes_table} pt ON di.node = pt.id
            )
            SELECT (select id from arrays) as id, (select username from arrays), st_concavehull(st_union(st_centroid(b.geom)), .85) AS geom
            FROM nodes a
            INNER JOIN {self.ls_table} b ON a.id = b."source"
            WHERE (select seg_name from arrays) = '{self.segment_name}';
            """
        )

    def __generate_mileage(self):
        """Returns the mileage of the segment"""

        print("calculating mileage of proximate islands, please wait..")
        q = db.query_as_singleton(
            f"""select size_miles from {self.network_type}.user_islands a
                inner join {self.network_type}.user_segments b
                on a.id = b.id
                where b.seg_name = '{self.segment_name}'""")
        return q

    def __decide_scope(self, mileage: int = 1000):
        """
        Decides if isochrone should be created or not based on mileage of connected islands
        """
        if self.miles > mileage:
            print(f"mileage of nearby islands > {mileage}, creating isochrone")
            self.__create_isochrone()
            self.has_isochrone = True
        else:
            self.has_isochrone = False

    def pull_stat(
        self,
        study_segment_id: int,
        column: str,
        table: str,
        geom_type: str,
    ):
        """
        grabs the identified attribute (population, school, etc) within the study area blobs.

        :param str column: the column you want to pull data from in your database
        :param str table: the table you want to pull data from in your database
        :param str geom_type: the type (point, line, or polygon) of your data, ie what is in the polygon shape.
        :param str polygon: the polygon you want stats for.

        """

        print(f"pulling stat from {table} table, please wait..")

        if self.has_isochrone is True:
            polygon = f"{self.network_type}.user_isochrones"
        elif self.has_isochrone is False:
            polygon = f"{self.network_type}.user_blobs"

        # only report crashes on study segment- not in entire low-stress area around it
        if table.endswith("crashes"):
            polygon = f"{self.network_type}.user_buffers"
        else:
            pass

        geom_type = geom_type.lower()

        if geom_type == "polygon":
            q = f"""
                with total as(
                    select round(st_area(st_intersection(a.geom, b.geom)) / st_area(a.geom) * a.{column}) as {column}_in_blobs
                    from {table} a, {polygon} b
                    where (st_intersects (a.geom, b.geom))
                    and (b.id = {self.study_segment_id}))
                select round(sum({column}_in_blobs)) from total
        """
            sum_poly = db.query_as_singleton(q)
            return int(round(sum_poly, -2))

        if geom_type == "point":
            df = db.df(
                f"""select count(a.{column}), a.{column} from {table} a, {polygon} b
                    where st_intersects(a.geom, b.geom)
                    and (b.id = {self.study_segment_id})
                    group by a.{column}"""
            )
            # zips up dataframe containing count by column attribute of point
            df_dict = df.to_dict("records")
            return df_dict

        if geom_type == "line":
            df = db.df(
                f"""
                    select a.{column}, st_length(a.geom)/1609 as miles from {table} a, {polygon} b
                        where st_intersects(a.geom, b.geom)
                        and (b.id = {self.study_segment_id})
                        group by a.{column}, st_length(a.geom)
                    """
            )
            df_dict = df.to_dict("records")
            return df_dict

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

        db.import_dataframe(df, "summaries.all", {"if_exists": "append"})
