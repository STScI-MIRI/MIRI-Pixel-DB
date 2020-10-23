#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 10:28:20 2019

@author: hagan

This is a python script that, when called, will take a LVL1 FITS exposure and add it to the DB. This script:
    1) Creates a pipeline ready FITS file (if one does not already exist) for LVL1 exposure if it is 'JPL' or 'OTIS' ground test data
    2) Adds the raw exposure info to the DB
    3) Checks if a *_ramp.fits file exists - if not it will run the JWST Detector1Pipeline to create the *_ramp.fits and *_rateint.fits (or *_rate.fits if single integration)
    4) Adds the corrected exposure info to the DB """

from sqlalchemy import Table
from exposuresdb import generate_structured_coordinates, insert_pixel_detector_info, add_raw_exposure_to_db, add_corrected_exposure_to_db
from miridb import init_db, load_miri_tables, load_engine
from pipefits import create_pipeline_ready_file, generate_corrected_ramp
import os
import time

def run_jwst_pipeline_jpl8(raw_exposure_filepath, reference_directory, pipeline_directory):
    """ These overrides specific to JPL8 data. """
    linearity_override_file = reference_directory + 'MIRI_JPL_RUN8_FPM101_JPL_LINEARITY_07.05.00.fits'
    saturation_override_file = reference_directory + 'MIRI_JPL_RUN8_FPM101_SATURATION_MEDIAN_07.02.00.fits'
    rscd_override_file = reference_directory + 'MIRI_JPL_RUN8_RSCD_07.04.00.fits'
    generate_corrected_ramp(raw_exposure_filepath, linearity_override = linearity_override_file, saturation_override = saturation_override_file, rscd_override = rscd_override_file, skip_dark = True, output_path = pipeline_directory)

""" Function to add a exposure to the DB.
    For JPL8 data, we override the reference files used for:
     - linearity
     - saturation
     - rscd
    This method is specific to JPL8 data because of the specific JPL8 reference file overrides provided, and we currently skip the dark correction for JPL8..
    Future development: This could be handled more intelligently by just supplying a config file that specify reference file overrides - in doing so we could generalize this method and use it for all LVL1 FITS exposure data.
    Look into supplying .pmap file?"""
def add_raw_and_corrected_exposure_to_db(data_genesis, data_origin, full_data_path, data_coords, ref_coords_reshape, session, connection, exposures, ramps, groups, correctedexposures, correctedramps, reference_directory, data_directory):
    """ Create pipeline ready file for LVL1 exposure """
    create_pipeline_ready_file(full_data_path, data_genesis, data_directory)
    #raw_exposure_filepath_pre = data_directory + '/' + os.path.basename(full_data_path)
    raw_exposure_filepath_pre = 'exposures/' + os.path.basename(full_data_path)
    raw_exposure_filepath = raw_exposure_filepath_pre.replace(".fits","_pipe.fits")
    ### full_data_path.replace(".fits","_pipe.fits")
    """ Add raw exposure to DB"""
    print('Start adding raw exposure to DB')
    start = time.process_time()
    add_raw_exposure_to_db(raw_exposure_filepath, data_genesis, data_coords, ref_coords_reshape, session, connection, exposures, ramps)
    print('Finished adding raw exposure to DB: ' + str(time.process_time() - start))
    """ Call JWST pipeline if *_ramp.fits file does not exist"""
    corrected_ramp_fn = raw_exposure_filepath.replace(".fits","_ramp.fits")
    if not os.path.exists(corrected_ramp_fn):
        if data_origin == 'jpl8':
            run_jwst_pipeline_jpl8(raw_exposure_filepath, reference_directory, data_directory)
        elif data_origin == 'test':
            generate_corrected_ramp(raw_exposure_filepath, skip_dark = True, output_path = data_directory)
        # elif data_origin == 'jpl9'
        # elif data_origin == 'OTIS'
        # elif data_origin == 'Flight'
    else:
        print('Corrected Ramp File Already Exists, so JWST pipeline was not executed.')
    """ Add corrected exposure to DB """
    print('Start adding corrected exposure to DB')
    start = time.process_time()
    add_corrected_exposure_to_db(corrected_ramp_fn, session, connection, exposures, groups, ramps, correctedexposures, correctedramps)
    print('Finished adding corrected exposure to DB: ' + str(time.process_time() - start))


""" To run this script from the command line, do:
    $ python  miridb_script_file_location data_origin full_data_path reference_directory connection_string
    where:
    miridb_script_file_location = miridb_script.py (or filepath to miridb_script.py)
    data_origin = JPL8, JPL9, OTIS, Flight etc. Right now only JPL8 supported.
    reference_directory = directory location of the folder conatining the reference files be used as overrides in the JWST Detector1Pipeline.
    password = password to access the MIRI Pixel DB - ask developers for access (J. Brendan Hagan <hagan@stsci.edu>, Sarah Kendrew <sarah.kendrew@esa.int>)
"""
import sys
if __name__ == '__main__':
    data_origin = sys.argv[1].lower()
    full_data_path = sys.argv[2]
    reference_directory = sys.argv[3]
    connection_string = sys.argv[4]

    data_directory = os.path.dirname(full_data_path) + '/'

    engine = load_engine(connection_string)
    session, base, connection, cursor = init_db(engine)
    load_miri_tables(base)

    detectors = Table('detectors',  base.metadata, autoload=True, autoload_with=engine)
    num_rows_detectors_table = session.query(detectors).count()

    """ create and insert detector/pixel values"""
    if num_rows_detectors_table == 0:
        insert_pixel_detector_info(connection)

    """ Load in tables that will be queried """
    exposures = Table('exposures',  base.metadata, autoload=True, autoload_with=engine)
    ramps = Table('ramps',  base.metadata, autoload=True, autoload_with=engine)
    groups = Table('groups',  base.metadata, autoload=True, autoload_with=engine)
    correctedexposures = Table('correctedexposures',  base.metadata, autoload=True, autoload_with=engine)
    correctedramps = Table('correctedramps',  base.metadata, autoload=True, autoload_with=engine)

    """ data_coords and ref_coords_reshape variables the same for every exposure (and all subarrays)"""
    data_coords, ref_coords_reshape = generate_structured_coordinates()

    """ Parallelization could happen here, over 'full_data_path' and 'reference_directory' variable - would need to make some modifications to this script"""
    if data_origin == 'jpl8' or data_origin == 'test':
        data_genesis = 'JPL'
        add_raw_and_corrected_exposure_to_db(data_genesis, data_origin, full_data_path, data_coords, ref_coords_reshape, session, connection, exposures, ramps, groups, correctedexposures, correctedramps, reference_directory, data_directory = data_directory)
    else:
        print('Method to add ' + data_origin + ' exposure not yet supported with this script')
