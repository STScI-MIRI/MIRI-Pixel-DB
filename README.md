# MIRI-Pixel-DB
Code repository for the MIRI DSII project "Tracing the lives of MIRI detector pixels"

Created by: J. Brendan Hagan and Sarah Kendrew

Welcome to the code respository for the MIRI Pixel Database! This work uses the postgresql database technology, and it uses the sqlalchemy python package to interface with postgresql. The database is hosted on Amazon Web Services (AWS).

One of the principal challenges of operating the Mid-Infrared Instrument (MIRI) onboard the James Webb Space Telescope (JWST) will lie in creating accurate calibrations of the non-ideal and complex behaviours of the MIRI detectors. In preparation for this challenge, we have constructed a database to house MIRI calibration data, allowing analysts to query individual pixels on the MIRI detectors, and corresponding pixel ramps, for every image taken for MIRI calibration exposures. Such a database will allow the MIRI instrument team to work more effectively in characterizing detector effects that we have neither fully understood yet nor found robust solutions for correction. Additionally, this database would allow future development of new data-driven/machine learning-based calibration strategies. By utilizing the latest in cloud computing and database technologies, our database will provide instrument analysts with quick and easy access to calibration data. Currently in a prototyping stage, the MIRI Pixel Database can become a powerful tool for observing trends and monitoring our detectors’ health, with the potential to minimize the time required for dedicated calibration observations. 

## Database Architecture
A UML diagram of the MIRI Pixel DB can be found in this repository - see `miri_pixel_db_uml.png`. This diagram was generated with code found in the `MIRI_pixel_BD.ipynb` under the "Generate UML Diagram for MIRI Pixel DB" section.

## Setup 
To build and use this DB, clone this repository, cd into the repository directory and do:
- `pip install -r requirements.txt`
This will install the external packages: sqlalchemy, psycopg2, pandas, and the jwst pipeline.
- install version 12.1 of postgresql, found here: https://www.enterprisedb.com/downloads/postgres-postgresql-downloads 
- run this line to create the miri_pixel_db database and the tables in it: `python  miri_pixel_db_code/db_init.py`

To access this database from the command line, do the following:
- Add this line to .bashrc file:  `export PATH=/Library/PostgreSQL/12/bin:$PATH`
- Enter this line in termal: `psql -U postgres`
- command line to list all databases in postgresql:  `\list`

## Continuous Integration and Unit Test
This repository uses Travis CI. To manually run the unit test, go to base directory and run  ```pytest -q -s``` .

## References/Tutorials
 - Why we chose psycopg2 as driver for postgresql: https://wiki.postgresql.org/wiki/Python and http://initd.org/psycopg/
 - In-house tutorial using sqlalchemy: https://gist.github.com/bourque/6653dd69dadb3c1ee3d2ed6a9f3db2e5
 - Tutorial using sqlalchemy and postgresql: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html

## Insights to Data Volume and Computation Time with FULL MIRI exposure
Detailed timing / code profiling in `code_profile_info.txt` file in this repository.

Test: adding a single FULL exposure to DB:
- Raw data exposure is FULL array, 5 integrations with 20 groups each - raw data has dimensions (5, 20, 1024, 1032)
- We add data to the MIRI Pixel DB from the following files:
    -  The uncalibrated FITS file (264.2 MB) 
    -  The "*_ramp.fits" file (1.06 GB)
    -  The "_rateints.fits" file (105.7 MB) 
- This totals 1.43 GB of FITS data (not all of which is added to the DB - e.g. not all FITS extensions from these files are added to DB). From this 1.43 GB of data, there is 27 GB of data generated that is added to DB for this single exposure.
- This is approximately 19 GB of data in the DB per 1 GB of FITS data (a crude estimate)
- After adding this one exposure to the DB, the tables had the following sizes:
    - 'detectors': '24 kB',
    - 'exposures': '48 kB',
    - 'correctedramps': '3176 MB',
    - 'correctedexposures': '48 kB',
    - 'pixels': '84 MB',
    - 'ramps': '1087 MB',
    - 'groups': '8991 MB',
    - 'correctedgroups': '14 GB'
- On my personal machine (2.9 GHz Intel Core i9  with 32 GB 2400 MHz DDR4 memory), it took ~ 29 minutes to add raw exposure data to DB and approximately 197 minutes to add corrected exposure data to DB (~3.3 hours total, excluding any JWST pipeline operations). 

## Data Selection
MIRI Pixel DB demonstration will be on JPL8 data taken on the FPM-101 detector. We have selected 332 uncalibrated, raw exposures (FITS), totaling 166.9 GB from the JPL8 test campaign. This data comes from the following JPL8 tests:
- `09_Mode_Switch_no3_pt2`
- `10_Long_Pers`
- `12_Anneals_pt2`
