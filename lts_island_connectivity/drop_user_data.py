from pg_data_etl import Database

# this file should not be used after production is launched! only useful while testing segments in early dev stages.
db = Database.from_config("localhost")

if __name__ == "__main__":
    for value in ["sidewalk", "lts"]:
        for table in ["blobs", "buffers", "islands", "segments", "isochrones"]:
            db.execute(f"drop table if exists {value}.user_{table}")
