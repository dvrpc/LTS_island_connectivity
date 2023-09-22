# LTS_island_connectivity

A python module to analyze the connectivity benefit of bike and pedestrian projects in terms of number of people, jobs, etc... connected.

## Depenedencies
PostGIS
pgRouting (for isochrone creation)

## Environment

You can create a virtual environment in the project directory with
`python -m venv ve`

Activate it with 
`. ve/bin/activate` and then install the requirements. `pip install -r requirements.txt`

If you prefer Conda, there is also an environment.yml.
`conda env create environment.yml` 

Activate the environment with:
`conda activate connectivity`

## Makefile

Be sure you've created a Postgres database called "lts", and that you have the above dependencies installed.

Run `Make all` to import all data and build the islands for this analysis. 

Make all will enable postgis and pgrouting on the lts database that you created, and will load all data from the DVRPC postgres server and any other sources.

Note that this only works behind the DVRPC firewall.

If you want to move any of this data to a server, run the makefile behind the firewall, then make a PG_dump of the DB and pg_restore it on your server.

### Backups
you can use the `make backup` command to make a backup of your database. To do so, you need a .env file with the following variables. (or you can just handle the backup on your own)

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

move the backup to your out-of-firewall machine (likely server) and restore it with psql.

note you need to create the target db. go ahead and create postgis and pgrouting extensions too. then run:

`psql -U your_username -h your_host -p your_port -d target_database < backup.sql`

## TODO

:white_check_mark: import data scripts

:white_check_mark: generate islands

:white_check_mark: generate blobs

:white_check_mark: grab census data (block level), parse percentage overlap

:white_check_mark: refactor to use classes

:white_check_mark: add clear button to webmap

:white_check_mark: modularize for sidewalks

:black_square_button: make API , hook to webmap

:black_square_button: update how crashes are pulled (setup PA/NJ)


