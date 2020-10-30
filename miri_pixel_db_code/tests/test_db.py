'''
Created on Mon Sep 17 10:28:20 2019

@author: @author: J. Brendan Hagan

This is a unit test for the MIRI Pixel Database.
This unit test takes a small JPL8 exposure as input and then:
    1) Creates a new file with the correct data format and metadata/FITS headers to feed to the JWST pipeline (*_pipe.fits)
    2) Calls the JWST calwebb_detector1 pipeline on the *_pipe.fits file. The JWST pipeline creates and saves the corrected
       ramp file (*_ramp.fits) and the countrate product (*_rateints.fits, or *_rate.fits if exposure is just 1 integration)
    3) The data from the _pipe.fits file gets transformed and placed into the database, populating the exposures, ramps, and groups tables.
    4) The data from the *_ramp.fits and *_rateints.fits files gets transformed and placed into the database, populating the correctedexposures,
       correctedramps, and correctedgroups tables
    5) Once this data has been added to the database, we perform a number of assertions to check that the data has been added correctly to the DB
    6) Then we check the "cascade delete" design by inputting the exposure name we wish to delete from the database (i.e. the one we just added)
    7) Perform a number of assertions to check that all the data pertaining to that exposure has been deleted from all the appropriate tables in the DB.

The exposure chosen for testing is a SUB64 JPL8 exposure. The SUB64 exposure is ideal because it is:
    1) small (suitable for unit testing). The *_pipe.fits file has 5 integrations, each with 50 groups
    2) Useful for checking coordinate transformations (i.e. pixel ids for SUB64 pixels).

We call the miridb_script.py file to add this exposure to the DB.

'''
from sqlalchemy import Table
import sys
sys.path.append("..")
from miridb import init_db, load_engine
import time
import glob, os
from subprocess import call

def test_db_unit():

    user = 'postgres'
    db_name = 'miri_pixel_db'
    connection_string = 'postgresql+psycopg2://' + user + '@localhost/' + db_name
    engine = load_engine(connection_string)
    session, base, connection, cursor = init_db(engine)
    test_exp = 'MIRI_5582_89_S_20180308-010230_SCE1_pipe.fits'
    pyscript = 'miridb_script.py'
    orig_exp = test_exp.replace('_pipe','')

    ''' load in the tables from the MIRI DB '''
    table_names = engine.table_names()
    table_dir = {table_name : Table(table_name,  base.metadata, autoload=True, autoload_with=engine) for table_name in table_names}
    num = session.query(table_dir['exposures'].c.exp_id).filter(table_dir['exposures'].c.exp == test_exp).count()
    ''' assert that no exposure lives on the DB '''
    assert num == 0

    ''' calling the  miridb_script.py file to add exposure to DB'''
    working_dir = os.path.dirname(os.getcwd()) + '/MIRI-Pixel-DB/miri_pixel_db_code/'
    script_path = working_dir + pyscript
    exposure_path = working_dir + 'tests/exposures/' + orig_exp
    run_cmd = ['python',script_path,'test',exposure_path,'None',connection_string]
    print("Script Path:",script_path)
    print("Command Line cmd:",' '.join(run_cmd))
    start = time.time()
    call(run_cmd)
    print('\nFinished Adding Exp to DB: ' + str(time.time() - start))

    ''' here we check that the fits data has been added correctly to the DB'''
    exposuresQ = session.query(table_dir['exposures'].c.exp_id).filter(table_dir['exposures'].c.exp == test_exp)
    expid = exposuresQ.scalar()
    rampsQ = session.query(table_dir['ramps'].c.ramp_id).filter(table_dir['ramps'].c.exp_id == expid)
    groupsQ = session.query(table_dir['groups'].c.group_id).join(table_dir['ramps']).join(table_dir['exposures']).filter(table_dir['exposures'].c.exp_id == expid)
    correctedexposuresQ = session.query(table_dir['correctedexposures'].c.corrected_exp_id).filter(table_dir['correctedexposures'].c.exp_id == expid)
    corr_expid = correctedexposuresQ.scalar()
    correctedrampsQ = session.query(table_dir['correctedramps'].c.corr_ramp_id).filter(table_dir['correctedramps'].c.corrected_exp_id == corr_expid)
    correctedgroupsQ = session.query(table_dir['correctedgroups'].c.corr_group_id).join(table_dir['correctedramps']).join(table_dir['correctedexposures']).filter(table_dir['correctedexposures'].c.corrected_exp_id == corr_expid)

    ''' '''
    test_query = session.query(table_dir['ramps']).filter(table_dir['ramps'].c.exp_id == expid)
    first_test_query = test_query.first()
    first_pix_id = first_test_query[1]
    num_ramps_with_pix_id_for_test_exp = session.query(table_dir['ramps'].c.exp_id).filter(table_dir['ramps'].c.exp_id == expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()
    num_corr_ramps_with_pix_id_for_test_exp = session.query(table_dir['correctedramps'].c.corrected_exp_id).join(table_dir['ramps']).filter(table_dir['ramps'].c.pixel_id == first_pix_id).count()
    number_groups_with_given_pix_id = session.query(table_dir['groups'].c.group_id).join(table_dir['ramps']).filter(table_dir['ramps'].c.exp_id == expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()
    number_corr_groups_with_given_pix_id = session.query(table_dir['correctedgroups'].c.corr_group_id).join(table_dir['correctedramps']).join(table_dir['ramps']).filter(table_dir['correctedramps'].c.corrected_exp_id == corr_expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()

    assert exposuresQ.count() == 1
    assert correctedexposuresQ.count() == 1
    assert rampsQ.count() == 23040 # should have 23040 rows in ramps tabkle for this exposure (72*64*50 = 23040, where 72*64 is size of SUB64, and we have 5 integrations, each with 50 groups)
    assert correctedrampsQ.count() == 23040 # same number should be added for the corrected ramps
    assert groupsQ.count() == 1152000 # This number is just 23040*5, i.e. number of ramps mutiplied by number of groups in a ramp
    assert correctedgroupsQ.count() == 1152000 # same number should be added for the corrected groups
    assert first_pix_id == 802897  # this is the first pixel ID for the first pixel in the SUB64 array - pixel ID corresponds to pixel ID on the FULL array.
    assert num_ramps_with_pix_id_for_test_exp == 5 # checking the number of ramps returned for a given pixel ID - should be 5 as there are 5 integrations.
    assert num_corr_ramps_with_pix_id_for_test_exp == 5 # same number but for corrected ramps
    assert number_groups_with_given_pix_id == 250 # return the correct number of groups for a given pixel ID - 5*50 =250
    assert number_corr_groups_with_given_pix_id == 250  # same number but for the corrected groups

    start = time.time()
    ''' 
    here we test the 'cascade delete' indexing -  given the name of the exposure, it should delete everything associated with it from the DB - so entries
    for this exposure in the exposures, correctedexposures, ramps, correctedramps, groups, and correctedgroups tables should vanish.
    '''
    d = table_dir['exposures'].delete().where(table_dir['exposures'].c.exp == test_exp)
    d.execute()
    print('Finished Deleting Exp from DB: ' + str(time.time() - start))

    ''' next two blocks query the database for the exposure again, but this time we make sure it has been deleted'''
    exposuresQ = session.query(table_dir['exposures'].c.exp_id).filter(table_dir['exposures'].c.exp == test_exp)
    rampsQ = session.query(table_dir['ramps'].c.ramp_id).filter(table_dir['ramps'].c.exp_id == expid)
    groupsQ = session.query(table_dir['groups'].c.group_id).join(table_dir['ramps']).join(table_dir['exposures']).filter(table_dir['exposures'].c.exp_id == expid)
    correctedexposuresQ = session.query(table_dir['correctedexposures'].c.corrected_exp_id).filter(table_dir['correctedexposures'].c.exp_id == expid)
    correctedrampsQ = session.query(table_dir['correctedramps'].c.corr_ramp_id).filter(table_dir['correctedramps'].c.corrected_exp_id == corr_expid)
    correctedgroupsQ = session.query(table_dir['correctedgroups'].c.corr_group_id).join(table_dir['correctedramps']).join(table_dir['correctedexposures']).filter(table_dir['correctedexposures'].c.corrected_exp_id == corr_expid)

    assert exposuresQ.count() == 0
    assert correctedexposuresQ.count() == 0
    assert rampsQ.count() == 0
    assert correctedrampsQ.count() == 0
    assert groupsQ.count() == 0
    assert correctedgroupsQ.count() == 0

    ''' we delete the files generated from the pipeline off of the VM - not super necessary for VM but helpful for testing locally'''
    test_folder = working_dir + 'tests/exposures/'
    generated_files = glob.glob(test_folder + '*_pipe*.fits')
    assert len(generated_files) >= 3
    [os.remove(file) for file in generated_files]
    generated_files = glob.glob(test_folder + '*_pipe*.fits')
    assert len(generated_files) == 0
    print('Finished Test')
