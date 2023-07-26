# LTS_island_connectivity

New approach to LTS Network Connectivity

Webmap for segment selection:
[Link](https://dvrpc.github.io/LTS_island_connectivity/)

## Depenedencies
PostGIS
pgRouting (for isochrone creation)

## Environment

Use:
`conda env create environment.yml` 

Activate the environment with:
`conda activate connectivity`

## Makefile

Be sure you've created a Postgres database called "LTS", and that you have the above dependencies installed.

Run `Make all` to import all data and build the islands for this analysis. 

Note that this only works behind the firewall. 

If you want to move any of this data to a server, run the makefile behind the firewall, then make a PG_dump of the DB and pg_restore it on your server.

### Backups
you can use the `make backup` command to make a backup of your database. To do so, you need a .env file with the following variables:

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

## TODO

:white_check_mark: import data scripts

:white_check_mark: generate islands

:white_check_mark: generate blobs

:white_check_mark: grab census data (block level), parse percentage overlap

:white_check_mark: refactor to use classes

:white_check_mark: add clear button to webmap

:black_square_button: modularize for sidewalks

:black_square_button: make API , hook to webmap

:black_square_button: update how crashes are pulled (setup PA/NJ)


