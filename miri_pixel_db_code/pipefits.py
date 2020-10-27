#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 10:28:20 2019

@author: hagan


This methods found in this package are used to prep JPL and OTIS ground test data to feed to the JWST Detector1Pipeline.
"""

from astropy.io import fits
import os.path
import numpy as np
from jwst.pipeline import Detector1Pipeline

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

### This function splits the reference pixels at the top of an image and creates a REFOUT extension to store them
def split_data_and_refout(hdulist):
    hdr = hdulist[0].header
    ROWSTART = hdr['ROWSTART']
    ROWSTOP = ROWSTART + hdr['NAXIS2']*0.8 - 1
    nrows = ROWSTOP - ROWSTART + 1
    # make sure they're integers with a nearest integer calculation
    nrows = int(nrows + 0.5)
    fulldata = hdulist[0].data
    detectordata = fulldata[:, :nrows]
    refoutdata = fulldata[:, nrows:]
    refout = np.array([np.array(list(chunks(dat.flatten(),nrows))).transpose() for dat in refoutdata])
    number_ramps = hdr['NGROUP']
    ramp_data = np.array(list(chunks(detectordata,number_ramps)))
    ref_pix_ramp_data = np.array(list(chunks(refout,number_ramps)))
    primaryhdu = fits.PrimaryHDU(header = hdr)
    scihdu = fits.ImageHDU(name = 'SCI')
    scihdu.data = ramp_data
    refhdu = fits.ImageHDU(name = 'REFOUT')
    refhdu.data = ref_pix_ramp_data
    new_hdu_list = fits.HDUList(hdus = [primaryhdu,scihdu,refhdu])
    return new_hdu_list

def Generate_JPL_Pipeline_Ready_File(file_path, output_dir):
    jpl_hdu = fits.open(file_path)
    #data_dir = os.path.dirname(file_path) + '/'
    ### expected NAXIS1, NAXIS2 keywords for subarray data (dimensions include reference pixels)
    subarray_keywords = [[[1032, 1280], 'FULL'],
                         [[288, 280], 'MASK1065'],
                         [[256, 320], 'SUB256'],
                         [[288, 280], 'MASK1140'],
                         [[320, 380], 'MASKLYOT'],
                         [[512, 640], 'BRIGHTSKY'],
                         [[72, 80], 'SUB64'],
                         [[136, 160], 'SUB128'],
                         [[72, 520], 'SLITLESSPRISM'],
                         [[288, 280], 'MASK1550']]
    detector_info = {'MIRIMAGE':493, 'MIRIFULONG':494, 'MIRIFUSHORT':495} ### need to force JPL data to be one of these in order to work for JWST pipeline
    jpl_hdr = jpl_hdu[0].header
    NAXIS1 = jpl_hdr['NAXIS1']
    NAXIS2 = jpl_hdr['NAXIS2']
    orig_size_jpl = [NAXIS1,NAXIS2]
    try:
        SUBARRAY = subarray_keywords[list(np.array(subarray_keywords)[:,0]).index(orig_size_jpl)][1]
    except ValueError:
        SUBARRAY = 'GENERIC'
    jpl_hdr.rename_keyword('NGROUPS','NGROUP')
    hdu_object_list = split_data_and_refout(jpl_hdu)
    jpl_hdu.close()
    hdr = hdu_object_list[0].header
    hdr.rename_keyword('DATE_END','DATE-END') ### this keyword not needed for feeding to JWST pipeline - used later for inserting into database
    hdr.rename_keyword('TIME_END','TIME-END') ### this keyword not needed for feeding to JWST pipeline - used later for inserting into database
    hdr.rename_keyword('DATE_OBS','DATE-OBS')
    hdr.rename_keyword('TIME_OBS','TIME-OBS')
    hdr.rename_keyword('NINT','NINTS')
    hdr.rename_keyword('NFRAME','NFRAMES')
    hdr.rename_keyword('NGROUP','NGROUPS')
    NREFIMG = int(NAXIS2*0.2)
    '''JPL data incorrectly uses COLSTART value - see http://poppy.as.arizona.edu/dhas/ for more details: "...it was determined that for JPL testing the COLSTART keyword in the header is incorrect. This version of the DHAS fixes the COLSTART value in the software. It does not update the COLSTART in the RAW DATA. "'''
    if hdr['ORIGIN'] == 'JPL':
        hdr['COLSTART'] = int(0.2*hdr['COLSTART'] + 0.8) ### COLSTART correction for JPL data
        hdr['GROUPGAP'] = 0   ### HARD-CODED - is this always 0? GROUPGAP is "The number of dropped frames in between groups."
        hdr['DET_JPL'] = hdr['DETECTOR'] ### keep 'DETECTOR' keyword from JPL, store in new keyword 'DET_JPL'
        hdr['DETECTOR'] = 'MIRIMAGE' ### HARD-CODED
        hdr['READPATT'] = 'FAST'     ### HARD-CODED
        hdr['SCAIDJPL'] = hdr['SCA_ID'] ### keep 'SCA_ID' keyword from JPL, store in new keyword 'SCAIDJPL'
        hdr['SCA_ID'] = detector_info[hdr['DETECTOR']]    ### taken from HARD-CODED 'DETECTOR' keyword
        hdr['OBS_ID'] = str(hdr['OBS_ID']) ### 'OBS_ID' is integer in JPL data, pipeline expects string
    hdr['SUBARRAY'] = SUBARRAY
    hdr['SUBSTRT2'] = hdr['ROWSTART']
    hdr['SUBSTRT1'] = (hdr['COLSTART']*4 - 3) ### int((COLSTART - 1)*0.8 + 1) <-- SUBSTRT1 formula using JPL 'COLSTART' as is.
    hdr['SUBSIZE1'] = NAXIS1
    hdr['SUBSIZE2'] = NAXIS2 - NREFIMG
    hdr_filename = os.path.basename(file_path)
    pipeline_ready_file = hdr_filename.replace(".fits", "_pipe.fits")
    #pipeline_ready_file = hdr['FILENAME'].replace(".fits","_pipe.fits")
    hdr['FILENAME'] = pipeline_ready_file
    output_path = output_dir + pipeline_ready_file
    #data_dir + pipeline_ready_file
    hdu_object_list.writeto(output_path)
    hdu_object_list.close()


def run_jwst_pipeline_jpl8(raw_exposure_filepath, reference_directory, pipeline_directory):
    """ These overrides specific to JPL8 data. """
    linearity_override_file = reference_directory + 'MIRI_JPL_RUN8_FPM101_JPL_LINEARITY_07.05.00.fits'
    saturation_override_file = reference_directory + 'MIRI_JPL_RUN8_FPM101_SATURATION_MEDIAN_07.02.00.fits'
    rscd_override_file = reference_directory + 'MIRI_JPL_RUN8_RSCD_07.04.00.fits'
    generate_corrected_ramp(raw_exposure_filepath, linearity_override = linearity_override_file, saturation_override = saturation_override_file, rscd_override = rscd_override_file, skip_dark = True, output_path = pipeline_directory)

def Generate_OTIS_Pipeline_Ready_File(file_path):
    hdu_object_list_pre = fits.open(file_path)
    data_dir = os.path.dirname(file_path) + '/'
    hdr_pre = hdu_object_list_pre[0].header
    first_pix = [int(hdr_pre['COLCORNR']),int(hdr_pre['ROWCORNR'])]
    size = [hdr_pre['NAXIS1'],hdr_pre['NAXIS2']-hdr_pre['NREFIMG']]
    subarray_name = grab_subname(first_pix,size)
    hdu_object_list = split_data_and_refout(hdu_object_list_pre)
    hdu_object_list_pre.close()
    hdr = hdu_object_list[0].header
    ### editing fits headers to feed to the jwst pipeline
    hdr.rename_keyword('READOUT','READPATT')
    hdr['SUBARRAY'] = subarray_name
    hdr['SUBSTRT1'] = first_pix[0]
    hdr['SUBSTRT2'] = first_pix[1]
    hdr['SUBSIZE1'] = size[0]
    hdr['SUBSIZE2'] = size[1]
    hdr['EXP_TYPE'] = 'MIR_IMAGE'
    hdr.rename_keyword('NINT','NINTS')
    hdr.rename_keyword('NFRAME','NFRAMES')
    hdr.rename_keyword('NGROUP','NGROUPS')
    pipeline_ready_file = hdr['FILENAME'].replace(".fits","_pipe.fits")
    output_path = data_dir + pipeline_ready_file
    hdu_object_list.writeto(output_path)
    print(output_path)
    hdu_object_list.close()

def grab_subname(first_pix,size):
    pixel_info_dict = {
        'FULL' : [[1, 1], [1032, 1024]],
        'ILLUM' : [[360, 1], [668, 1024]],
        'BRIGHTSKY' : [[457, 51], [512, 512]],
        'SUB256' : [[413, 51], [256, 256]],
        'SUB128' : [[1, 889], [136, 128]],
        'SUB64' : [[1, 779], [72, 64]],
        'SLITLESSPRISM' : [[1, 529], [72, 416]],
        'MASK1065' : [[1, 19], [288, 224]],
        'MASK1140' : [[1, 245], [288, 224]],
        'MASK1550' : [[1, 467], [288, 224]],
        'MASKLYOT' : [[1, 717], [320, 304]]
    }
    sub_info = [first_pix,size]
    subarray_name = list(pixel_info_dict.keys())[list(pixel_info_dict.values()).index(sub_info)]
    return subarray_name

# def create_pipeline_ready_file(full_data_path, data_genesis, output_dir):
#     ### generate a pipeline ready file
#     try:
#         if data_genesis == 'JPL':
#             print('full_data_path = ',full_data_path)
#             print('output_dir = ', output_dir)
#             Generate_JPL_Pipeline_Ready_File(full_data_path,output_dir)
#         elif data_genesis == 'OTIS':
#             Generate_OTIS_Pipeline_Ready_File(full_data_path)
#         else:
#             print('Unexpected data_genesis for this method - OTIS or JPL LVL1 data expected')
#     except OSError:
#         print('Pipeline ready fits file has already been generated for this file')


def create_pipeline_ready_file(full_data_path, data_genesis, output_dir):
    ### generate a pipeline ready file
    print('full_data_path = ', full_data_path)
    print('output_dir = ', output_dir)
    Generate_JPL_Pipeline_Ready_File(full_data_path, output_dir)




""" This function calls the calwebb_detector1 pipeline step  - currently written with JPL data in mind, hence the various options for reference file overrides and skipping pipeline steps.
Read more here: https://jwst-pipeline.readthedocs.io/en/latest/jwst/pipeline/calwebb_detector1.html
skip pipeline steps: https://stsci-ins.basecamphq.com/projects/11477312-jwst-pipeline/posts/101399961/comments"""
def generate_corrected_ramp(pipeline_ready_file, dark_override = None, linearity_override = None, saturation_override = None, rscd_override = None, mask_override = None, skip_dark = False, output_path = None):
    mypipeline = Detector1Pipeline()
    mypipeline.save_calibrated_ramp = True
    mypipeline.save_results = True
    if dark_override:
        mypipeline.dark_current.override_dark = dark_override #dark_ref_override
    if linearity_override:
        mypipeline.linearity.override_linearity = linearity_override
    if saturation_override:
        mypipeline.saturation.override_saturation = saturation_override
    if rscd_override:
        mypipeline.rscd.override_rscd = rscd_override
    if mask_override:
        mypipeline.dq_init.override_mask = mask_override
    if skip_dark:
        mypipeline.dark_current.skip = True
    if output_path:
        mypipeline.output_dir = output_path
    result = mypipeline.run(pipeline_ready_file)
    return result

#hdr['COLSTOP'] = COLSTART + NAXIS1*0.25 - 1 ### this is not used, but this is how to calculate COLSTOP keyword assuming COLSTART is correct
#hdr['ROWSTOP'] = ROWSTART + NAXIS2*0.8 - 1  ### this is not used
