# LTS_island_connectivity

A python module to analyze the connectivity benefit of bike and pedestrian projects in terms of number of people, jobs, etc... connected.

## Setup/Installation
### Depenedencies
PostGIS
pgRouting (for isochrone creation)
ogr2ogr


#### 1. Create a virtual environment in the project directory with:
```shell
python -m venv ve
```

#### 2. Activate it:
`. ve/bin/activate` and then install the requirements. `pip install -r requirements.txt`

If you prefer Conda, there is also an environment.yml.
`conda env create environment.yml` 

Activate the environment with:
`conda activate connectivity`

#### 3. Setup pg-data-etl
For now, this repo makes some use of [a fork of the pg-data-etl](https://github.com/mmorley0395/pg-data-etl) tool. The tool is installed when you clone this repo,
but you need to make the configuration file for it. With your virtual environment activated, type:

```shell
pg make-config-file
```

Otherwise, just create a file in your root directory at `/USERHOME/.pg-data-etl/database_connections.cfg`.

Here's an example.

```
[DEFAULT]
pw = this-is-a-placeholder-password
port = 5432
super_db = postgres
super_un = postgres
super_pw = this-is-another-placeholder-password

[localhost]
host = localhost
un = postgres
pw = your-password-here
db_name = your-db-name 
```

This file doesn't need to be recreated for other projects that use pg-data-etl, just add any credentials for new connections. 

In the future, pg-data-etl may be removed from this repo, in favor of more direct ORM and less dependency management. 

One of the dependencies of this repo, [network-routing](https://github.com/dvrpc/network-routing/tree/master/network_routing), also uses pg-data-etl, 
so unless that is refactored, it remains necessary to install and set up here.

You also need a .env file, for the parts of the project that don't use pg-data-etl. This includes credentials to your DB and the DVRPC GIS DB, which has been redacted below.

```
PG_DUMP_PATH='/usr/bin/pg_dump'
BACKUP_FILENAME="backup.sql"
DUMP_PATH="/path/${BACKUP_FILENAME}"

DATABASE_URL = postgresql://user:pw@localhost:port/lts
HOST = localhost
UN = user
DB_NAME = lts
PW = pw
PORT = port

GIS_DATABASE_URL = postgresql://user:ps@host:port/db
GIS_HOST = 
GIS_USER = 
GIS_DB_NAME = 
GIS_PASSWORD = 
GIS_PORT = 
```

### Makefile

Be sure that you have the above dependencies installed and configured.

In order for the makefile to work, you have to be either the superuser or another postgres user with create db privileges. You can do the following and add yourself as a user.
```
psql -U postgres
CREATE USER myuser WITH PASSWORD 'securepassword';
ALTER ROLE myuser CREATEDB;
\q
```

Run `make all` to import all data and build the islands for this analysis. The Makefile in this repo shows the steps used with that command.

Make all will create the db, enable postgis and pgrouting on the lts database that you created, and will load all data from the DVRPC postgres server and any other sources.

Note that this only works behind the DVRPC firewall.

If you want to move any of this data to a server, run the makefile behind the firewall, then make a PG_dump of the DB and pg_restore it on your server.

### CLI
If you don't want to stand up the associated FastAPI application and React app, a CLI is included for convenience. All options are below. If you don't have access to the command, make sure venv
is activated and type `pip install --editable .` You'll see the segment be processed in the command line, and outputs can be viewed in your database IDE (like dbeaver) or psql. 

```
❯ connect --help
Usage: connect [OPTIONS]

  Runs the connections.py file, point to a geojson path on your machine.

Options:
  --network_type TEXT             type of network: lts or sidewalk ONLY
  --geojson_path TEXT             path to geojson of feature(s), handles
                                  feature and feature collection  [required]
  --username TEXT                 username for db purposes
  --highest_comfort_level INTEGER
                                  highest comfort level, best to leave at 2
  --overwrite BOOLEAN             whether or not to overwrite
  --pg_config_filepath TEXT       filepath for pg_config if other than default
  --help                          Show this message and exit.
```

### Backups
You can use the `make backup` command to make a backup of your database. To do so, you need a .env file with the following variables. (or you can just handle the backup on your own)

```
# location of pg_dump binary (find with the 'which pg_dump' command)
PG_DUMP_PATH='/usr/bin/pg_dump' 

# uri of the database you're backing up
DATABASE_URL='postgresql://user:pw@host:port/database'

# name of your backup file. use double quotes.
BACKUP_FILENAME="backup.sql"

#location of your backup file. use double quotes here too.
DUMP_PATH="/home/user/project/backups/${BACKUP_FILENAME}"
```

Move the backup to your out-of-firewall machine (likely your server) and restore it with psql.

`psql -U your_username -h your_host -p your_port -d target_database < backup.sql`

## Usage
The primary entrypoint for using this tool is the StudySegment class in the connections.py file. 

When you instantiate a StudySegment object, you must provide a network type ("lts" or "sidewalk"), a feature (a single object geojson), and a username, which will be used to organize
segments in the database.

Here's an example python file, where a geoJSON feature is evaluated using the LTS network. This feature represents a stressful road. The class object creates a variety of tables in postgres, 
including finding which low-stress "islands" touch the feature, and how many people live on those islands. 

```
from lts_island_connectivity import StudySegment

# geoJSON dict
feature = {
   "id":"0394c9b353713495d441e6de2f12bb7b",
   "type":"Feature",
   "properties":{
      "name":"real1"
   },
   "geometry":{
      "coordinates":[
         [
            -74.97010900490982,
            39.882133479501164
         ],
         [
            -74.96537580171305,
            39.87931669481782
         ],
         [
            -74.96182589931531,
            39.87729669245934
         ],
         [
            -74.95653726921309,
            39.87483183834513
         ],
         [
            -74.95115204312707,
            39.87214449044336
         ],
         [
            -74.94458351624205,
            39.868975137961485
         ]
      ],
      "type":"LineString"
   }
}

StudySegment("lts", feature, "mmorley")
```

## License
This project uses the GPL(v3) license. 