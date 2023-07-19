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

## TODO

:white_check_mark: import data scripts

:white_check_mark: generate islands

:white_check_mark: generate blobs

:white_check_mark: grab census data (block level), parse percentage overlap

:white_check_mark: refactor to use classes

:white_check_mark: add clear button to webmap

:black_square_button: make API , hook to webmap

:black_square_button: update how crashes are pulled (setup PA/NJ)


