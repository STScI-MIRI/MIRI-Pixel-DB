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
    table_names = engine.table_names()
    table_dir = {table_name : Table(table_name,  base.metadata, autoload=True, autoload_with=engine) for table_name in table_names}
    num = session.query(table_dir['exposures'].c.exp_id).filter(table_dir['exposures'].c.exp == test_exp).count()
    assert num == 0

    working_dir = os.path.dirname(os.getcwd()) + '/MIRI-Pixel-DB/miri_pixel_db_code/'
    script_path = working_dir + pyscript
    exposure_path = working_dir + 'tests/exposures/' + orig_exp
    run_cmd = ['python',script_path,'test',exposure_path,'None',connection_string]
    print("Script Path:",script_path)
    print("Command Line cmd:",' '.join(run_cmd))
    start = time.time()
    call(run_cmd)

    print('\nFinished Adding Exp to DB: ' + str(time.time() - start))

    exposuresQ = session.query(table_dir['exposures'].c.exp_id).filter(table_dir['exposures'].c.exp == test_exp)
    expid = exposuresQ.scalar()
    rampsQ = session.query(table_dir['ramps'].c.ramp_id).filter(table_dir['ramps'].c.exp_id == expid)
    groupsQ = session.query(table_dir['groups'].c.group_id).join(table_dir['ramps']).join(table_dir['exposures']).filter(table_dir['exposures'].c.exp_id == expid)
    correctedexposuresQ = session.query(table_dir['correctedexposures'].c.corrected_exp_id).filter(table_dir['correctedexposures'].c.exp_id == expid)
    corr_expid = correctedexposuresQ.scalar()
    correctedrampsQ = session.query(table_dir['correctedramps'].c.corr_ramp_id).filter(table_dir['correctedramps'].c.corrected_exp_id == corr_expid)
    correctedgroupsQ = session.query(table_dir['correctedgroups'].c.corr_group_id).join(table_dir['correctedramps']).join(table_dir['correctedexposures']).filter(table_dir['correctedexposures'].c.corrected_exp_id == corr_expid)

    test_query = session.query(table_dir['ramps']).filter(table_dir['ramps'].c.exp_id == expid)
    first_test_query = test_query.first()
    #first_ramp_id = first_test_query[0]
    first_pix_id = first_test_query[1]
    num_ramps_with_pix_id_for_test_exp = session.query(table_dir['ramps'].c.exp_id).filter(table_dir['ramps'].c.exp_id == expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()
    num_corr_ramps_with_pix_id_for_test_exp = session.query(table_dir['correctedramps'].c.corrected_exp_id).join(table_dir['ramps']).filter(table_dir['ramps'].c.pixel_id == first_pix_id).count()
    number_groups_with_given_pix_id = session.query(table_dir['groups'].c.group_id).join(table_dir['ramps']).filter(table_dir['ramps'].c.exp_id == expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()
    number_corr_groups_with_given_pix_id = session.query(table_dir['correctedgroups'].c.corr_group_id).join(table_dir['correctedramps']).join(table_dir['ramps']).filter(table_dir['correctedramps'].c.corrected_exp_id == corr_expid, table_dir['ramps'].c.pixel_id == first_pix_id).count()

    assert exposuresQ.count() == 1
    assert correctedexposuresQ.count() == 1
    assert rampsQ.count() == 23040
    assert correctedrampsQ.count() == 23040
    assert groupsQ.count() == 1152000
    assert correctedgroupsQ.count() == 1152000
    assert first_pix_id == 802897
    assert num_ramps_with_pix_id_for_test_exp == 5
    assert num_corr_ramps_with_pix_id_for_test_exp == 5
    assert number_groups_with_given_pix_id == 250
    assert number_corr_groups_with_given_pix_id == 250

    start = time.time()
    d = table_dir['exposures'].delete().where(table_dir['exposures'].c.exp == test_exp)
    d.execute()
    print('Finished Deleting Exp from DB: ' + str(time.time() - start))

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

    # test_folder = working_dir + 'tests/exposures/'
    # generated_files = glob.glob(test_folder + '*_pipe*.fits')
    # assert len(generated_files) >= 3
    # [os.remove(file) for file in generated_files]
    # generated_files = glob.glob(test_folder + '*_pipe*.fits')
    # assert len(generated_files) == 0
    print('Finished Test')
