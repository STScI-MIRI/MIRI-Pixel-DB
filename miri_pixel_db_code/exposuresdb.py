#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 10:28:20 2019

@author: J. Brendan Hagan

This package contains all the methods necessary to add exposure data to the MIRI Pixel DB.

"""
from astropy.io import fits
import numpy as np
import os
from datetime import datetime
import itertools
import pandas as pd
from io import StringIO
import time

""" Uncomment these 4 lines below to profile functions using the @profile decorator"""
# import line_profiler
# import atexit
# profile = line_profiler.LineProfiler()
# atexit.register(profile.print_stats)


""" The function below, add_rows_to_table, is the fastest way I found to insert many rows into a postgresql table. This method
is faster than using:
1) psycopg2.extras.execute_batch
2) df.to_sql('ramps', cursor, if_exists='append') - where df is a pandas df
3) using bulk_insert_mappings doing:
    df = pd.DataFrame(pixels_vals)
    recs = df.to_dict(orient="records")
    session.bulk_insert_mappings(Pixels, recs)
It is surprising that 'df.to_sql' is slower than using df.to_csv with copy_from - I verified this myself, and (as of November 20, 2019)) it
seems to be the consensus online that the df.to_csv method below is currently the
fastest way to insert a large number of rows into a single table.

Note: to use this method to insert arrays into our db, it is required that we use the prep_ramps_for_db function on
the array data. Even with the prep_ramps_for_db run time, this is by far the fastest method for insertion
that I have found.

More info on this solution found here:
https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
https://www.codementor.io/bruce3557/graceful-data-ingestion-with-sqlalchemy-and-pandas-pft7ddcy6 """
def add_rows_to_table(df, table_name, connection):
    """add rows to table via a pandas DataFrame"""
    output = StringIO()
    df.to_csv(output, sep='\t',header=False,index=False)
    output.seek(0)
    cursor = connection.cursor()
    columns_mine = tuple(df.columns)
    cursor.copy_from(output, table_name, null="", columns = columns_mine)
    connection.commit()


""" This function is only necessary because postgresql requires arrays to be in curly braces for ingestion. In python,
curly braces (i.e. {}) indicate a 'set', which will return an unordered list of unique elemnts, which is not what we want.
Thus it's necessary to convert the arrays into strings and manually pad them with curly braces. A more elegant solution may exist?"""
def prep_ramps_for_db(all_ramps):
    all_ramps_pre = all_ramps.tolist()
    all_ramps_fin = [str(ramp).replace("[", "{", 1).replace("]", "}", 1) for ramp in all_ramps_pre]
    return all_ramps_fin


""" Transform image data cube so that each element in the data cube is the ramp for a single pixel"""
def transform_ramp(lvl1_ramp):
    return np.transpose([frame.flatten() for frame in lvl1_ramp])


""" Prep data to be fed into the DB"""
def get_ramps_and_groups_column_data(ramp_data):
    pixel_data_for_each_ramp = [transform_ramp(ramp) for ramp in ramp_data]
    all_ramps = np.array(list(itertools.chain.from_iterable(pixel_data_for_each_ramp)))
    all_groups = all_ramps.flatten()
    return all_ramps, all_groups


""" This function takes a dq value as input and breaks it down into a list of the different flags
    example -  in: DQ_value_interpret(287312209) out: [268435456, 16777216, 2097152, 2048, 256, 64, 16, 1]
    list of DQ flags here: http://jwst-reffiles.stsci.edu/source/data_quality.html"""
def DQ_value_interpret(first_var,possible_DQ_vals):
    DQs = []
    while first_var != 0:
        differences = [(first_var - val) for val in possible_DQ_vals]
        positive_differences = [x for x in differences if x >= 0]
        dq_val = possible_DQ_vals[len(positive_differences)-1]
        DQs.append(dq_val)
        first_var = first_var - dq_val
    return DQs


""" Returns a list of 0's and 1's, indicating which dq flags were found
    for a given dq_flag. If dq_flag is sum of multiple flag values, we call DQ_value_interpret
    function to return all the dq_flags present in that dq_val"""
def return_dq_flags(dq_val,possible_DQ_vals,dq_value_pose_dict, num_dqs):
    false_list = [0] * num_dqs
    if dq_val == 0:
        return false_list
    elif dq_val in possible_DQ_vals:
        false_list[dq_value_pose_dict[dq_val]] = 1
        return false_list
    else:
        dq_val_list = DQ_value_interpret(dq_val,possible_DQ_vals)
        for i in dq_val_list:
            false_list[dq_value_pose_dict[i]] = 1
        return false_list

#@profile
def generate_detectors_pixels_entries():
    """code to generate data to enter into 'pixels' and 'detectors tables'"""
    numrows = 1280
    numcols = 1032
    num_nonref_rows = 1024
    unique_detector_names = ['MIRIMAGE', 'MIRIFULONG', 'MIRIFUSHORT']
    unique_detector_ids = [493, 494, 495]
    """line to generate data to enter into 'detectors'"""
    detectors_vals_pre = {'detector_id':unique_detector_ids, 'name':unique_detector_names, 'ncols':([numcols] * 3), 'nrows':([numrows] * 3)};
    detectors_vals = pd.DataFrame(detectors_vals_pre)
    pixIDs=range(1,numrows*numcols+1)
    """generating the row and column numbers for each pixel id"""
    rowNums = np.array([[num] * numcols for num in range(1,numrows+1)]).flatten()
    colNums = np.transpose(np.array([[num] * numrows for num in range(1,numcols+1)])).flatten()
    """identifying which pix ids are reference pixels"""
    num_Ref_Pixels = (numrows - num_nonref_rows)*numcols
    number_Image_pixels = numrows*numcols - num_Ref_Pixels
    ref_Pixel_Boolean = (np.array([[0]*number_Image_pixels + [1]*num_Ref_Pixels])).flatten()
    """data input to the pixels table"""
    pixels_vals_pre = {'pixel_id':pixIDs,'row_id':rowNums,'col_id':colNums,'ref_pix':ref_Pixel_Boolean}
    pixels_vals = pd.DataFrame(pixels_vals_pre)
    return detectors_vals, pixels_vals


""" Complememnt of a list"""
def complement(first, second):
    second = set(second)
    return [item for item in first if item not in second]


""" Partition list into chunks of size n"""
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


""" Mapping image coordinates to a coordinate space where every 5th column is a column of reference pixels"""
def ref_mapping(x,y):
    return int(x+np.floor(x/4)),y


""" Method to get the pixel coordinates for a subarray (including reference pixel coordinates)"""
def get_pixel_coordinates_for_subarray(data_coords, ref_coords_reshape, first_pix, size):
    transpose_dat_coords = data_coords.transpose()
    """inserting reference pixels into every 5th column in the full frame array"""
    dat_thing = np.array([np.append(transpose_dat_coords[k*4:(k+1)*4],[ref_coords_reshape[k]],axis=0) for k in range(0,len(ref_coords_reshape))])
    full_array_with_ref_columns = np.array([item for sublist in dat_thing for item in sublist]).transpose()
    """extracting the subarray coordinates with reference columns from the full array"""
    p1_0 = np.array(first_pix)-1
    p2_0 = p1_0 + np.array(size)
    p1 = np.array(ref_mapping(p1_0[0],p1_0[1]))
    p2 = np.array(ref_mapping(p2_0[0],p2_0[1]))
    result = full_array_with_ref_columns[p1[1]:p2[1],p1[0]:p2[0]]
    reference_pixel_coords_final = result[:,4::5]
    data_pixel_coords_final = np.delete(result, np.s_[4::5],1)
    return data_pixel_coords_final.flatten(), reference_pixel_coords_final.flatten()


""" Here we build a data structure to mirror the dimensions of the FULL array with reference pixels - the data structure
    contains all the integer pixel coordinates. The outputs here are used in the get_pixel_coordinates_for_subarray method above"""
def generate_structured_coordinates():
    numrows = 1280
    numcols = 1032
    pixIDs=range(1,numrows*numcols+1)
    all_partitioned_coords = np.array(list(chunks(pixIDs,1032)))
    nrows = 1024
    data_coords = all_partitioned_coords[:nrows]
    ref_coords = all_partitioned_coords[nrows:]
    ref_coords_reshape = np.array(list(chunks(ref_coords.flatten(),nrows)))
    return data_coords, ref_coords_reshape


""" List of dq_flags and their associated values - taken from here: http://jwst-reffiles.stsci.edu/source/data_quality.html"""
dq_val_ref = {
 1: 'do_not_use',
 2: 'saturated',
 4: 'jump_det',
 8: 'dropout',
 16: 'reserved_16',
 32: 'reserved_32',
 64: 'reserved_64',
 128: 'reserved_128',
 256: 'unreliable_error',
 512: 'non_science',
 1024: 'dead',
 2048: 'hot',
 4096: 'warm',
 8192: 'low_qe',
 16384: 'rc',
 32768: 'telegraph',
 65536: 'nonlinear',
 131072: 'bad_ref_pixel',
 262144: 'no_flat_field',
 524288: 'no_gain_value',
 1048576: 'no_lin_corr',
 2097152: 'no_sat_check',
 4194304: 'unreliable_bias',
 8388608: 'unreliable_dark',
 16777216: 'unreliable_slope',
 33554432: 'unreliable_flat',
 67108864: 'open',
 134217728: 'adj_open',
 268435456: 'unreliable_reset',
 536870912: 'msa_failed_open',
 1073741824: 'other_bad_pixel'}


""" Create and insert detector/pixel values"""
def insert_pixel_detector_info(connection):
    detectors_vals, pixels_vals = generate_detectors_pixels_entries()
    add_rows_to_table(pixels_vals, 'pixels', connection)
    add_rows_to_table(detectors_vals, 'detectors', connection)


""" Determine the subarray being used from FITS header and then use
    get_pixel_coordinates_for_subarray function to return the associated pixel_ids for that subarray"""
def generate_pixel_coordinates_from_header(hdr,data_coords,ref_coords_reshape):
    first_pix = [hdr['SUBSTRT1'],hdr['SUBSTRT2']]
    size = [hdr['SUBSIZE1'],hdr['SUBSIZE2']]
    data_pixel_coords_final, reference_pixel_coords_final = get_pixel_coordinates_for_subarray(data_coords, ref_coords_reshape, first_pix, size)
    return data_pixel_coords_final, reference_pixel_coords_final


""" Generate the exposure row to add to the Exposures table for a given raw FITS file/exposure"""
def generate_exposure_row(data_genesis,raw_ramp_header,exposure_table_column_names):
    """ gathering values for the row to add to the exposure table"""
    exposure_table_ngroups = raw_ramp_header['NGROUPS']
    exposure_table_nints = raw_ramp_header['NINTS']
    exposure_table_readmode = raw_ramp_header['READPATT']
    """ need try, except statements for DATE-OBS, TIME-OBS, DATE-END, and TIME-END because JPL data has filler values
        for these keywords, e.g. DATE-END = 'yyyy-mm-dd' and TIME-END= 'hh:mm:ss' """
    try:
        exposure_table_t0 = datetime.fromisoformat(raw_ramp_header['DATE-OBS'] + ' ' + raw_ramp_header['TIME-OBS'])
    except ValueError:
        exposure_table_t0 = datetime.fromisoformat('1111-11-11 11:11:11')
    try:
        exposure_table_t1 = datetime.fromisoformat(raw_ramp_header['DATE-END'] + ' ' + raw_ramp_header['TIME-END'])
    except ValueError:
        exposure_table_t1 = datetime.fromisoformat('1111-11-11 11:11:11')
    exposure_table_exptime = raw_ramp_header['EXPTIME']
    exposure_table_inttime = raw_ramp_header['INTTIME']
    """ collecting data for entering a new row into the exposure table"""
    exposure_table_subarray_name = raw_ramp_header['SUBARRAY']
    sca_id = raw_ramp_header['SCA_ID']
    exposure_table_filename = raw_ramp_header['FILENAME']#.replace('_pipe.fits','.fits')
    exposure_table_input = [exposure_table_filename, sca_id, data_genesis, exposure_table_ngroups, exposure_table_nints,
                            exposure_table_subarray_name, exposure_table_readmode,  exposure_table_t0, exposure_table_t1,
                            exposure_table_exptime, exposure_table_inttime]
    exposure_row = dict(zip(exposure_table_column_names, exposure_table_input))
    return exposure_row, exposure_table_filename


""" Generate the corrected exposure row to add to the CorrectedExposures table for a given "_ramp.fits" file ouput from the JWST Detector1Pipeline"""
def generate_corrected_exposure_row(corrected_header,corrected_exposure_table_column_names, exp_id):
    """gathering values for the row to add to the correctedexposures table"""
    corrected_exp = corrected_header['FILENAME']
    pipeline_step_keywords = ['S_DARK','S_DQINIT', 'S_FRSTFR', 'S_GRPSCL', 'S_IPC', 'S_JUMP', 'S_LASTFR', 'S_LINEAR', 'S_REFPIX', 'S_RSCD', 'S_SATURA']
    pipeline_ref_file_keywords = ['R_DARK', 'R_GAIN', 'R_IPC', 'R_LINEAR', 'R_MASK', 'R_READNO', 'R_RSCD', 'R_SATURA']
    ref_files = []
    for key in pipeline_ref_file_keywords:
        try:
            ref_files.append(corrected_header[key])
        except KeyError:
            ref_files.append('N/A')
    complete_skipped_dict = {'COMPLETE':True,'SKIPPED':False}
    pipeline_step_keyword_values = []
    for key in pipeline_step_keywords:
        try:
            pipeline_step_keyword_values.append(complete_skipped_dict[corrected_header[key]])
        except KeyError:
            pipeline_step_keyword_values.append(False)
    keywordvalues_1 = [corrected_header[key] for key in  ['CAL_VER', 'CRDS_VER', 'CAL_VCS']]
    """putting together the data to be entered into the correctedexposures table"""
    corrected_exposure_wanted_keyword_values = keywordvalues_1 + pipeline_step_keyword_values + ref_files
    corrected_exposure_table_input = [corrected_exp, exp_id] + corrected_exposure_wanted_keyword_values
    corrected_exposure_row = dict(zip(corrected_exposure_table_column_names, corrected_exposure_table_input))
    return corrected_exposure_row


""" Function to prep and insert a raw MIRI exposure (i.e. uncalibrated LVL1 data product) into the database - this includes
    insertions into the Exposures, Ramps, and Groups tables"""
#@profile
def add_raw_exposure_to_db(raw_exposure_filepath, data_genesis, data_coords, ref_coords_reshape, session, connection, exposures, ramps):
    raw_ramp_hdu = fits.open(raw_exposure_filepath)
    raw_ramp_header = raw_ramp_hdu[0].header ### raw_ramp_header used by exposure_row AND ramp_rows, group_rows
    ramp_data = raw_ramp_hdu[1].data
    raw_ramp_hdu.close()
    """ primary key generated automatically when rows enter into exposure table"""
    exposure_table_column_names = complement(exposures.columns.keys(),exposures.primary_key.columns.keys())
    """ generate the exposure row and insert it into the exposures table"""
    exposure_row, exposure_table_filename = generate_exposure_row(data_genesis, raw_ramp_header, exposure_table_column_names)
    exposures.insert().execute(exposure_row)
    """ generate the indiviadual ramp and group values to be inserted into the DB"""
    all_ramps, all_groups = get_ramps_and_groups_column_data(ramp_data)
    all_ramps_enter = prep_ramps_for_db(all_ramps)
    """ grab number of integrations"""
    dim_ramp_data = ramp_data.shape
    int_num = dim_ramp_data[0]
    """ grab the exp_id associated with the filename exposure_table_filename -  need this exp_id to insert ramps"""
    exp_id = session.query(exposures.c.exp_id).filter(exposures.c.exp == exposure_table_filename).scalar()
    ramp_len = dim_ramp_data[1]
    all_ints = list(range(1,int_num+1))
    """ here we generate the int number associated with each ramp"""
    ramp_ints_pre = []
    num_pixels = dim_ramp_data[2] * dim_ramp_data[3]
    for i in all_ints:
        ramp_ints_pre.append([i] * num_pixels)
    ramp_ints = list(itertools.chain.from_iterable(ramp_ints_pre))
    """ grab the pixel coordinates for the given subarray - subarray info contined in raw_ramp_header"""
    data_pixel_coords_final, reference_pixel_coords_final = generate_pixel_coordinates_from_header(raw_ramp_header, data_coords, ref_coords_reshape)
    """ multiply pixel coords by int_num to get pixel_id values for all the ramps"""
    all_pix_coords = list(data_pixel_coords_final) * int_num
    all_exp_ids = [exp_id] * len(all_pix_coords)
    """ create a dictionary of all the ramp data, convert to a pandas dataframe, and do fast insert with add_rows_to_table function"""
    ramps_table_dict = {'pixel_id': all_pix_coords, 'exp_id': all_exp_ids, 'intnumber': ramp_ints, 'ramp':all_ramps_enter}
    df_ramps = pd.DataFrame(ramps_table_dict)
    add_rows_to_table(df_ramps, 'ramps', connection)
    """ query for all the ramp_ids associated with a gievn exp_id. ramp_ids are retuened in the order in which they were inserted for that exp_id"""
    ramp_id_query = session.query(ramps.c.ramp_id).filter(ramps.c.exp_id == exp_id)
    """ create the ramps_id values to insert into the groups table"""
    ramp_id_query_vals = [([num[0]] * ramp_len) for num in ramp_id_query]
    group_ramp_ids = list(itertools.chain.from_iterable(ramp_id_query_vals))
    """ create the group_number values to insert into the groups table"""
    all_group_nums = list(itertools.chain.from_iterable([list(range(1,ramp_len+1))] * ramp_id_query.count()))
    """ create a dictionary of all the group data, convert to a pandas dataframe, and do fast insert with add_rows_to_table function"""
    groups_table_dict = {'ramp_id': group_ramp_ids, 'group_number': all_group_nums,'raw_value':all_groups}
    df_groups = pd.DataFrame(groups_table_dict)
    add_rows_to_table(df_groups, 'groups', connection)


""" Function to prep and insert a corrected MIRI exposure (i.e. a corrected ramp file, "_ramp.fits", output by the JWST Detector1Pipeline) into the database - this includes
    insertions into the CorrectedExposures, CorrectedRamps, and CorrectedGroups tables"""
#@profile
def add_corrected_exposure_to_db(corrected_ramp_fn, session, connection, exposures, groups, ramps, correctedexposures, correctedramps):
    """ Read in data from FITS file"""
    corrected_ramp_hdu = fits.open(corrected_ramp_fn)
    corrected_header = corrected_ramp_hdu[0].header
    corrected_ramp_data = corrected_ramp_hdu[1].data
    pix_group_dq_data = corrected_ramp_hdu[3].data
    pix_err_data = corrected_ramp_hdu[4].data
    corrected_ramp_hdu.close()
    """ grab the raw exposure filename - jwst pipeline inserts '_ramp' at the end of the filename"""
    exposure_table_filename = os.path.basename(corrected_ramp_fn).replace('_ramp.fits','.fits')
    """ grab exp_id associated with the exposure_table_filename, grab ramp_ids associated with that exp_id"""
    exp_id = session.query(exposures.c.exp_id).filter(exposures.c.exp == exposure_table_filename).scalar()
    ramp_ids_pre = session.query(ramps.c.ramp_id).filter(ramps.c.exp_id == exp_id)
    ramp_ids = [r[0] for r in ramp_ids_pre]
    """ generate the corrected exposure row for insert into the Corrected Exposures table"""
    corrected_exposure_table_column_names = complement(correctedexposures.columns.keys(),correctedexposures.primary_key.columns.keys())
    corrected_exposure_row = generate_corrected_exposure_row(corrected_header,corrected_exposure_table_column_names,exp_id)
    correctedexposures.insert().execute(corrected_exposure_row)
    """ lines below transform data so that each element in the list is the ramp for a given pixel"""
    all_corrected_ramps, all_corrected_groups = get_ramps_and_groups_column_data(corrected_ramp_data)
    all_dq_ramps, all_dq_groups = get_ramps_and_groups_column_data(pix_group_dq_data)
    all_err_ramps, all_err_groups = get_ramps_and_groups_column_data(pix_err_data)
    all_corrected_ramps_enter = prep_ramps_for_db(all_corrected_ramps)
    all_dq_ramps_enter = prep_ramps_for_db(all_dq_ramps)
    all_err_ramps_enter = prep_ramps_for_db(all_err_ramps)
    """ code to extract slope data to be inserted into the correctedpixelramps table. If the exposure has >1 integration, *_rateints.fits file is created, which is where
        we pull the slope values for each integration. If exposure is only 1 integration, then the JWST pipeline does not create *_rateints.fits
        file, and we get the slope value for the single intgeration from the *_rate.fits file."""
    exposure_table_nints = len(corrected_ramp_data)
    if exposure_table_nints == 1:
        slope_file = corrected_ramp_fn.replace("_ramp.fits","_rate.fits")
    else:
        slope_file = corrected_ramp_fn.replace("_ramp.fits","_rateints.fits")
    slope_hdu = fits.open(slope_file)
    slope_data = slope_hdu[1].data
    """ .byteswap().newbyteorder() needed in line below to avoid the ValueError described here: https://github.com/astropy/astropy/issues/1156"""
    slope_data_per_pixel = slope_data.flatten().byteswap().newbyteorder()
    slope_hdu.close()
    dims_ramps = all_dq_ramps.shape
    """ query for the corrected_exp_id based on the corrected exposure filename"""
    corrected_exp_id = session.query(correctedexposures.c.corrected_exp_id).filter(correctedexposures.c.corrected_exp == corrected_header['FILENAME']).scalar()
    """ create a constant array of corrected_exp_ids to insert into the ramps table"""
    corrected_exp_ids = [corrected_exp_id] * dims_ramps[0]
    """ Defining all possible DQ vals - the three lines below could be moved outside of this function, however they are very fast to execute"""
    possible_dq_vals = [2**k for k in range(0,31)]
    dq_value_pose_dict = dict(zip(possible_dq_vals, range(0,len(possible_dq_vals))))
    num_dqs = len(possible_dq_vals)
    """ This block of code interprets the values found in the dq_ramps and produces a boolean for each DQ flag for each ramp
        (True if ramp array contains DQ flag, False otherwise) and a boolean for each DQ flags for each group (True if group DQ int value contains DQ flag, False otherwise)"""
    number_of_ramps = len(all_dq_ramps)
    ramp_len = dims_ramps[1]
    dq_matrices = np.empty((number_of_ramps, len(possible_dq_vals), ramp_len),dtype=int)
    ramp_dq_vectors = np.empty((number_of_ramps,len(possible_dq_vals)),dtype=bool)
    for i in range(0,number_of_ramps):
        new_dq_ramp = np.array([return_dq_flags(flag, possible_dq_vals, dq_value_pose_dict, num_dqs) for flag in all_dq_ramps[i]])
        dq_matrix = np.transpose(new_dq_ramp)
        ramp_dq = [1 in row for row in dq_matrix]
        dq_matrices[i] = dq_matrix
        ramp_dq_vectors[i] = ramp_dq
    group_dq_flags = np.concatenate((dq_matrices), axis=1)
    tf_vals_each_flag = np.transpose(ramp_dq_vectors)
    dq_names = dq_val_ref.values()
    dq_group_val_dict = dict(zip(dq_names,group_dq_flags))
    dq_ramp_val_dict = dict(zip(dq_names,tf_vals_each_flag))
    """ create first part of corrected ramps dictionary, without the DQ_Flag information"""
    corrected_ramps_table_dict = {'ramp_id': ramp_ids, 'corrected_exp_id': corrected_exp_ids, 'slope_value': slope_data_per_pixel, 'corrected_ramp': all_corrected_ramps_enter,
             'dq_ramp': all_dq_ramps_enter, 'err_ramp': all_err_ramps_enter}
    """ update the corrected ramps dictionary with the DQ_Flag information, and then generate a pandas dataframe from this dictionary,
        and finally do a fast insert with add_rows_to_table function"""
    corrected_ramps_table_dict.update(dq_ramp_val_dict)
    df_corrected_ramps = pd.DataFrame(corrected_ramps_table_dict)
    add_rows_to_table(df_corrected_ramps, 'correctedramps', connection)
    """"query for all the group_ids associated with the exp_id, and create the foreign group_ids to insert into the CorrectedGroups table"""
    group_ids_pre = session.query(groups.c.group_id).join(ramps).filter(ramps.c.exp_id == exp_id)
    """ the postgresql query returns a list of tuples, all containing 1 element - to get a flat list, we need to perform this next line"""
    group_ids = [g[0] for g in group_ids_pre] # possible optimization https://dba.stackexchange.com/questions/2973/how-to-insert-values-into-a-table-from-a-select-query-in-postgresql
    """ query the corrrected ramps table to return all the corrected ramps ids associated with the corrected_exp_id,
        and make corrected_ramp_id foreign key for each corrected group entry"""
    corrected_ramp_ids_pre = session.query(correctedramps.c.corr_ramp_id).filter(correctedramps.c.corrected_exp_id == corrected_exp_id)
    corrected_ramp_ids = [([num[0]] * ramp_len) for num in corrected_ramp_ids_pre]
    corrected_group_ramp_ids = list(itertools.chain.from_iterable(corrected_ramp_ids))
    """ create the group numbers to be inserted into the CorrectedGroups table for the 'group_number' column"""
    all_group_nums = list(itertools.chain.from_iterable([list(range(1,ramp_len+1))] * corrected_ramp_ids_pre.count()))
    """ create first part of corrected groups dictionary, without the DQ_Flag information"""
    corrected_groups_table_dict = {'group_id': group_ids,
                                         'corr_ramp_id':corrected_group_ramp_ids, 'group_number': all_group_nums,
                                         'corrected_value':all_corrected_groups, 'dq_value':all_dq_groups,'error_value':all_err_groups}
    """ update the corrected groups dictionary with the DQ_Flag information, and then generate a pandas dataframe from this dictionary,
        and finally do a fast insert with add_rows_to_table function"""
    corrected_groups_table_dict.update(dq_group_val_dict)
    df_corrected_groups = pd.DataFrame(corrected_groups_table_dict)
    add_rows_to_table(df_corrected_groups, 'correctedgroups', connection)
