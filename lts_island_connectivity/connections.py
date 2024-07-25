from pg_data_etl import Database
import json
from geoalchemy2 import WKTElement
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import re
import requests
from pathlib import Path
from psycopg2 import OperationalError


class SegmentNameConflictError(Exception):
    """Exception raised when the segment name already exists."""

    pass


class StudySegment:
    def __init__(
        self,
        network_type: str,
        feature: dict,
        username: str,
        highest_comfort_level: int = 2,
        overwrite: bool = False,
        pg_config_filepath: str = None,
    ) -> None:
        self.db = Database.from_config("localhost", pg_config_filepath)
        self.network_type = network_type
        self.highest_comfort_level = highest_comfort_level
        segment_tablenames = self.__update_highest_comfort_level()
        self.highest_comfort_level = segment_tablenames[0]
        self.ls_table = segment_tablenames[1]
        self.ids = segment_tablenames[2]
        self.nodes_table = segment_tablenames[3]
        self.feature = feature
        self.geometry = feature["geometry"]
        self.properties = feature["properties"]
        self.segment_name = self.__sanitize_name()
        self.username = username
        self.__setup_study_segment_tables()
        self.study_segment_id = self.__create_study_segment(
            self.geometry, self.username, self.network_type, overwrite
        )
        self.__buffer_study_segment()
        self.__generate_proximate_islands()
        self.has_isochrone = None
        self.miles = self.__generate_mileage()
        self.has_isochrone = self.__decide_scope()
        self.__generate_proximate_blobs()
        self.__handle_parking_lots()
        self.__update_mileage()

        self.total_pop = self.pull_stat(
            self.study_segment_id,
            "total_pop",
            "censustract2020_demographics",
            "polygon",
        )
        self.disabled = self.pull_stat(
            self.study_segment_id,
            "disabled",
            "censustract2020_demographics",
            "polygon",
        )
        self.ethnic_minority = self.pull_stat(
            self.study_segment_id,
            "ethnic_minority",
            "censustract2020_demographics",
            "polygon",
        )
        self.female = self.pull_stat(
            self.study_segment_id,
            "female",
            "censustract2020_demographics",
            "polygon",
        )
        self.foreign_born = self.pull_stat(
            self.study_segment_id,
            "foreign_born",
            "censustract2020_demographics",
            "polygon",
        )
        self.lep = self.pull_stat(
            self.study_segment_id,
            "lep",
            "censustract2020_demographics",
            "polygon",
        )
        self.low_income = self.pull_stat(
            self.study_segment_id,
            "low_income",
            "censustract2020_demographics",
            "polygon",
        )
        self.older_adult = self.pull_stat(
            self.study_segment_id,
            "older_adult",
            "censustract2020_demographics",
            "polygon",
        )
        self.racial_minority = self.pull_stat(
            self.study_segment_id,
            "racial_minority",
            "censustract2020_demographics",
            "polygon",
        )
        self.youth = self.pull_stat(
            self.study_segment_id,
            "youth",
            "censustract2020_demographics",
            "polygon",
        )
        self.circuit = self.pull_stat(
            self.study_segment_id, "circuit", "circuittrails", "line"
        )
        self.jobs = self.pull_stat(
            self.study_segment_id, "total_jobs", "lodes_2020", "polygon"
        )
        self.bike_ped_crashes = self.pull_crashes(
            self.study_segment_id,
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

    def __get_name(self):
        return self.properties.get("name") or self.properties.get("Name")

    def __sanitize_name(self):
        """Remove non-standard characters from the segment name"""
        segment_name = self.__get_name()
        return re.sub(r"[^a-zA-Z0-9 ]", "", segment_name)

    def __update_highest_comfort_level(self):
        if self.network_type == "lts":
            self.highest_comfort_level = self.highest_comfort_level
            self.ls_table = f"lts_stress_below_{self.highest_comfort_level + 1}"
            self.ids = "dvrpc_id"
            self.nodes_table = (
                f"{self.network_type}{self.highest_comfort_level + 1}nodes"
            )
        elif self.network_type == "sidewalk":
            self.highest_comfort_level = ""
            self.ls_table = "ped_network"
            self.ids = "objectid"
            self.nodes_table = f"{self.network_type}nodes"
        else:
            raise ValueError("Network type is unexpected, should be sidewalk or lts")
        return [self.highest_comfort_level, self.ls_table, self.ids, self.nodes_table]

    def __setup_study_segment_tables(self):
        for value in [
            "user_segments",
            "user_buffers",
            "user_islands",
            "user_blobs",
            "user_isochrones",
        ]:
            if value == "user_segments":
                query = f"""
                    CREATE TABLE IF NOT EXISTS {self.network_type}.{value}(
                        id SERIAL PRIMARY KEY,
                        username VARCHAR,
                        seg_name VARCHAR,
                        network_type VARCHAR,
                        highest_comfort_level INT,
                        ls_table VARCHAR,
                        ids VARCHAR,
                        nodes_table VARCHAR,
                        has_isochrone BOOL,
                        miles REAL,
                        total_pop INT,
                        disabled INT,
                        ethnic_minority INT,
                        female INT,
                        foreign_born INT,
                        lep INT,
                        low_income INT,
                        older_adult INT,
                        racial_minority INT,
                        youth INT,
                        circuit JSON,
                        total_jobs INT,
                        bike_ped_crashes JSON,
                        essential_services JSON,
                        rail_stations JSON,
                        deleted BOOL,
                        shared BOOL,
                        geom GEOMETRY
                    );
                """
            else:
                query = f"""
                    CREATE TABLE IF NOT EXISTS {self.network_type}.{value}(
                        id SERIAL PRIMARY KEY,
                        username VARCHAR,
                        geom GEOMETRY
                    );
                """
            self.db.execute(query)

    def __check_segname(self):
        """Checks to see if segment is already in DB"""

        segs = self.db.query(
            f"select seg_name from {self.network_type}.user_segments where username = '{self.username}'"
        )

        # flattens list returned from db.query
        flat_segs = [item for sublist in segs for item in sublist]

        return flat_segs

    def __create_study_segment(
        self,
        geojson_dict: dict,
        username: str,
        network_type: str,
        overwrite: bool = False,
    ):
        """
        Creates a study segment / study segments based on user's drawn geometry.
        """
        engine = create_engine(self.db.uri)
        Session = sessionmaker(bind=engine)
        session = Session()

        segment_name = self.segment_name

        db_segments = self.__check_segname()

        if segment_name in db_segments and not overwrite:
            raise SegmentNameConflictError("Project name already used.")

        if overwrite:
            delete_query = text(
                f"""
                DELETE FROM {network_type}.user_segments
                WHERE seg_name = :seg_name AND username = :username
            """
            )
            session.execute(
                delete_query, params={"seg_name": segment_name, "username": username}
            )
            session.commit()

        if self.geometry.get("type") == "LineString":
            coordinates = self.geometry.get("coordinates", [])
            coord_str = ", ".join([f"{x} {y}" for x, y in coordinates])
            line_wkt = f"LINESTRING({coord_str})"
        elif self.geometry.get("type") == "MultiLineString":
            multi_coordinates = self.geometry.get("coordinates", [])
            lines = []
            for line in multi_coordinates:
                coord_str = ", ".join([f"{x} {y}" for x, y in line])
                lines.append(f"({coord_str})")
            line_wkt = f"MULTILINESTRING({', '.join(lines)})"

        else:
            raise ValueError("Geojson must be of type LineString or MultiLineString")

        wkt_element = WKTElement(line_wkt, srid=4326)
        table_name = f"{network_type}.user_segments"
        query = f"""
        INSERT INTO {table_name}
        (id, username, seg_name, geom)
        VALUES (DEFAULT, :username, :seg_name, ST_Transform(ST_GeomFromText(:geom, 4326), 26918))
        """

        query = text(query)

        session.execute(
            query,
            params={
                "username": username,
                "seg_name": segment_name,
                "geom": wkt_element.desc,
            },
        )
        session.commit()

        study_segment_id = self.db.query_as_singleton(
            f"""
            select id from {self.network_type}.user_segments a
            where a.seg_name = '{self.segment_name}'
            and a.username = '{self.username}'"""
        )

        return study_segment_id

    def __buffer_study_segment(self, distance: int = 30):
        """
        Creates a buffer around the study segment.
        Default for distance is 30m (100 ft) assuming your data is using meters
        """

        self.db.execute(
            f"""
                insert into {self.network_type}.user_buffers
                select id, username, st_buffer(geom, {distance}) as geom
                from {self.network_type}.user_segments
                where seg_name = '{self.segment_name}'
                and username = '{self.username}'
            """
        )

    def __generate_proximate_islands(self):
        """
        Finds islands proximate to study segment buffer and adds them to the
        user_islands table in the DB. each uid in islands_uids is one island, not the
        ids of the underlying segments.
        """

        print("generating proximate islands, please wait..")

        self.db.execute(
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
                    and c.username = '{self.username}'
                    group by a.id
            """
        )

    def __generate_proximate_blobs(self):
        """
        Creates 'blobs' around each island collection.
        Note that this query also unions the islands with the study segment buffer.

        """

        if self.has_isochrone is True:
            pass
        else:
            self.db.execute(
                f"""
                    insert into {self.network_type}.user_blobs
                    select a.id, a.username, st_union(st_buffer(a.geom, 100), c.geom) as geom
                    from {self.network_type}.user_islands a
                    inner join {self.network_type}.user_segments b
                    on a.id = b.id
                    inner join {self.network_type}.user_buffers c
                    on a.id = c.id
                    where b.seg_name = '{self.segment_name}'
                    and b.username = '{self.username}'
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
        else:
            print("something went wrong with isochrone scope")

        self.db.execute(
            f"""
            WITH proximate_lu AS (
                SELECT a.geom, c.id, c.seg_name, a.lu15subn
                FROM landuse_2015 a
                INNER JOIN {self.network_type}.user_buffers b
                ON ST_Intersects(a.geom, b.geom)
                INNER JOIN {self.network_type}.user_segments c
                ON b.id = c.id
                WHERE c.seg_name = '{self.segment_name}'
                AND c.username = '{self.username}'
                AND (
                    a.lu15subn LIKE 'Parking%'
                    OR a.lu15subn LIKE 'Institutional%'
                    OR a.lu15subn LIKE 'Commercial%'
                    OR a.lu15subn = 'Recreation: General'
                    OR a.lu15subn = 'Transportation: Rail Right-of-Way'
                    OR a.lu15subn = 'Transportation: Facility')
            ),
            proximate_lu_and_touching AS (
                SELECT st_collect(b.geom, a.geom) as geom
                FROM proximate_lu a
                inner JOIN landuse_2015 b
                ON ST_Touches(a.geom, b.geom)
                Where (
                    b.lu15subn LIKE 'Parking%'
                    OR b.lu15subn LIKE 'Institutional%'
                    OR b.lu15subn LIKE 'Commercial%'
                    OR b.lu15subn = 'Recreation: General'
                    OR b.lu15subn = 'Transportation: Rail Right-of-Way'
                    OR b.lu15subn = 'Transportation: Facility')
            )
            UPDATE {join_table} AS b
            SET geom = ST_Union(a.geom, b.geom)
            from proximate_lu_and_touching a
            where b.id = {self.study_segment_id}

        """
        )

    def __create_isochrone(self, travel_time: int = 15):
        """
        Creates isochrone based on study_segment
        """

        try:
            sql = f"""
                    alter table {self.network_type}.user_isochrones
                    add column if not exists miles FLOAT;
                    insert into {self.network_type}.user_isochrones
                    WITH arrays AS (
                        select a.id as id, --id of user segment, tied to blobs, buffer, etc
                        a.username,
                        a.seg_name,
                        array_agg(c.{self.ids}) as ids --ids in low stress table
                        FROM {self.network_type}.user_segments a
                        INNER JOIN {self.network_type}.user_buffers b ON a.id = b.id
                        INNER JOIN {self.network_type}.{self.ls_table} c ON st_intersects(b.geom, c.geom)
                        WHERE a.seg_name = '{self.segment_name}'
                        AND a.username = '{self.username}'
                        group by a.id
                    ),
                    nodes AS (
                        SELECT *
                        FROM pgr_drivingDistance(
                            'SELECT {self.ids} as id, source, target, traveltime_min as cost FROM {self.network_type}.{self.ls_table}', -- example: ls_stress_below_3
                            (SELECT array_agg("source") FROM {self.network_type}.{self.nodes_table} a
                             INNER JOIN {self.network_type}.{self.ls_table} b ON a.id = b."source"
                             WHERE b.{self.ids}= ANY((SELECT ids FROM arrays)::integer[])), -- Using ANY with integer array
                            {travel_time}, false
                        ) AS di
                        JOIN {self.network_type}.{self.nodes_table} pt ON di.node = pt.id
                    ),
                    node_buffer as (
                        select ST_Union(ST_Buffer(pt.geom, 1000)) AS geom
                        from nodes n
                        join {self.network_type}.{self.nodes_table} pt ON n.node = pt.id
                )
                    select (select id from arrays) as id, (select username from arrays), st_union(st_buffer(a.geom, 100)) as geom, round(st_length(st_union(a.geom))/1609) as miles
                        from {self.network_type}.{self.ls_table} a
                        where st_intersects(a.geom, (select geom from node_buffer))
                        and (select seg_name from arrays) = '{self.segment_name}';

                    """
            self.db.execute(sql)
        except OperationalError:
            print(
                f"failed to create isochrone for this segment, {self.segment_name} for some reason."
            )

    def __update_mileage(self):
        if self.has_isochrone is True:
            try:
                query = f"""
                select a.miles from {self.network_type}.user_isochrones a
                inner join {self.network_type}.user_segments b
                on a.id=b.id
                where b.username = '{self.username}'
                and b.seg_name= '{self.segment_name}'
                """
                self.miles = self.db.query_as_singleton(query)
            except Exception as e:
                print(f"An error occurred: {e}")
                print(f"Failed query: {query}")
                raise RuntimeError(f"Error updating : {e}")
        else:
            pass

    def __generate_mileage(self):
        """Returns the mileage of the segment or handles cases where mileage is nothing."""

        print("calculating mileage of proximate islands, please wait..")

        try:
            q = self.db.query_as_singleton(
                f"""SELECT size_miles FROM {self.network_type}.user_islands a
                    INNER JOIN {self.network_type}.user_segments b
                    ON a.id = b.id
                    WHERE b.seg_name = '{self.segment_name}'
                    AND b.username = '{self.username}'"""
            )
        except IndexError:
            q = 0.0

        return q

    def __decide_scope(self, mileage: int = 300):
        """
        Decides if isochrone should be created or not based on mileage of connected islands
        """
        if self.miles > mileage:
            print(f"mileage of nearby islands > {mileage}, creating isochrone")
            self.__create_isochrone()
            self.has_isochrone = True
        else:
            self.has_isochrone = False
        return self.has_isochrone

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
                    and (b.id = {self.study_segment_id})
                    and a.{column} >= 0)
                select round(sum({column}_in_blobs)) from total
        """
            sum_poly = self.db.query_as_singleton(q)
            if sum_poly is None:
                return None
            else:
                return int(round(sum_poly, -2))

        if geom_type == "point":
            df = self.db.df(
                f"""select count(a.{column}), a.{column} from {table} a, {polygon} b
                    where st_intersects(a.geom, b.geom)
                    and (b.id = {self.study_segment_id})
                    group by a.{column}"""
            )
            # zips up dataframe containing count by column attribute of point
            df_dict = df.to_dict("records")
            return df_dict

        if geom_type == "line":
            df = self.db.df(
                f"""
                select
                    {column},
                    sum(miles) as miles
                from (
                    select
                        a.{column},
                        st_length(a.geom)/1609 as miles
                    from
                        {table} a,
                        {polygon} b
                    where
                        st_intersects(a.geom, b.geom)
                        and (b.id = {self.study_segment_id})
                ) as subquery
                group by
                    {column};
                    """
            )
            df_dict = df.to_dict("records")
            return df_dict

    def pull_crashes(self, study_segment_id: int):
        """
        Grabs crash data from DVRPC's crash API for each polygon in a MultiPolygon GeoJSON.
        """

        geo = self.db.query(
            f"""SELECT st_asgeojson(st_transform(st_union(geom), 4326))
            FROM {self.network_type}.user_buffers
            WHERE id = {study_segment_id}"""
        )
        geojson = json.loads(geo[0][0])

        total_bike_crashes = 0
        total_ped_crashes = 0

        if geojson["type"] == "MultiPolygon":
            for polygon in geojson["coordinates"]:
                polygon_geojson = json.dumps(
                    {"type": "Polygon", "coordinates": polygon}
                )

                r = requests.get(
                    f"https://cloud.dvrpc.org/api/crash-data/v1/summary?geojson={polygon_geojson}"
                )
                data = r.json()

                for year, year_data in data.items():
                    try:
                        if year_data["mode"]:
                            total_bike_crashes += year_data["mode"].get("Bicyclists", 0)
                            total_ped_crashes += year_data["mode"].get("Pedestrians", 0)
                    except TypeError:
                        print(year_data)

        else:
            r = requests.get(
                f"https://cloud.dvrpc.org/api/crash-data/v1/summary?geojson={geo[0][0]}"
            )
            try:
                data = r.json()
                for year, year_data in data.items():
                    try:
                        if year_data["mode"]:
                            total_bike_crashes += year_data["mode"].get("Bicyclists", 0)
                            total_ped_crashes += year_data["mode"].get("Pedestrians", 0)
                    except TypeError:
                        print(year_data)
                total_crashes = {
                    "Total Bike Crashes": total_bike_crashes,
                    "Total Pedestrian Crashes": total_ped_crashes,
                }
                return [total_crashes]
            except requests.exceptions.JSONDecodeError:
                data = r
                return ["error with crash api"]

    def update_study_seg(self, column: str, value):
        """
        Summarizes all connections that a segment makes

        :param str column: the column to update in the user_segments table
        :param value: the value you want to put into that column
        """

        try:
            if isinstance(value, str):
                set_statement = f"set {column} = '{value}'"
            elif isinstance(value, (int, float)):
                set_statement = f"set {column} = {value}"
            elif isinstance(value, list):
                value = json.dumps(value)
                set_statement = f"set {column} = '{value}'::json"
            elif value is None:
                set_statement = f"set {column} = NULL"
            else:
                value = json.dumps(value)
                set_statement = f"set {column} = '{value}'::json"

            query = f"""
                update {self.network_type}.user_segments
                {set_statement}
                where id = {self.study_segment_id}
                and username = '{self.username}'
            """
            self.db.execute(query)
        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"Failed query: {query}")
            raise RuntimeError(f"Error updating {column}: {e}")

    def summarize_stats(self):
        cols = {
            "network_type": self.network_type,
            "highest_comfort_level": self.highest_comfort_level,
            "ls_table": self.ls_table,
            "ids": self.ids,
            "nodes_table": self.nodes_table,
            "has_isochrone": self.has_isochrone,
            "miles": self.miles,
            "total_pop": self.total_pop,
            "disabled": self.disabled,
            "ethnic_minority": self.ethnic_minority,
            "female": self.female,
            "foreign_born": self.foreign_born,
            "lep": self.lep,
            "low_income": self.low_income,
            "older_adult": self.older_adult,
            "racial_minority": self.racial_minority,
            "youth": self.youth,
            "circuit": self.circuit,
            "total_jobs": self.jobs,
            "bike_ped_crashes": self.bike_ped_crashes,
            "essential_services": self.essential_services,
            "rail_stations": self.rail_stations,
        }

        if cols["highest_comfort_level"] is None or cols["highest_comfort_level"] == "":
            cols["highest_comfort_level"] = 0

        for key, value in cols.items():
            self.update_study_seg(key, value)


if __name__ == "__main__":
    feature = {
        "id": "e6b633b53c6e142d4a29aa24c6669fc8",
        "type": "Feature",
        "properties": {"name": "philly"},
        "geometry": {
            "coordinates": [
                [-75.16631560655193, 39.94005421415005],
                [-75.15783254981699, 39.93901901924613],
            ],
            "type": "LineString",
        },
    }
    StudySegment(
        "lts",
        feature,
        "mmorley0395",
        overwrite=True,
        pg_config_filepath=Path.home() / "repos" / ".test" / "database_connections.cfg",
    )
