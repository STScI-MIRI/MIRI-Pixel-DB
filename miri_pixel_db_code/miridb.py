#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 10:28:20 2019

@author: hagan

The methods in this package are used to define/create the tables in the MIRI Pixel DB. Other methods are provided to interact with / perform operations on the DB.
"""
from sqlalchemy import create_engine, Column, String, Boolean, Float, ForeignKey, Integer, DateTime, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.dialects.postgresql import ARRAY
import numpy as np


"""Method to delete a table from the DB"""
def delete_table(table_name_to_be_deleted, password, base):
    try:
        table_to_delete = base.metadata.tables.get(table_name_to_be_deleted)
        engine = load_engine(password)
        base = declarative_base(engine)
        table_to_delete.drop(checkfirst=True)
        base.metadata.reflect(bind = engine)
    except AttributeError:
        print("%s table does not exist, so it cannot be deleted" % table_name_to_be_deleted)

"""example: psql_string = 'SELECT pg_size_pretty( pg_database_size(\'postgres\') )' """
def enter_psql_command(engine, psql_string):
    with engine.connect() as con:
        rs = con.execute(psql_string).fetchall()
    return rs

"""
finding active queries running on your db - taken from
https://medium.com/little-programming-joys/finding-and-killing-long-running-queries-on-postgres-7c4f0449e86d
"""
def cancel_active_queries(engine):
    psql_string = """SELECT
      pid,
      now() - pg_stat_activity.query_start AS duration,
      query,
      state
    FROM pg_stat_activity
    WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';"""
    out = enter_psql_command(engine, psql_string)
    if out != []:
        index_list = np.where(np.array([row[-1] for row in out]) == 'active')[0]
        if len(index_list) != 0:
            pids = [row[0] for row in out]
            active_pids = [pids[num] for num in index_list]
            for pid in active_pids:
                psql_string = 'SELECT pg_cancel_backend(' + str(pid) + ');'
                print(enter_psql_command(engine, psql_string))
        else:
            print('No active processes running')
    else:
        print('No processes of any type are running')

"""function to return size of a table using direct postgresql command"""
def get_size_of_table(engine, table_name):
    psql_string = 'SELECT pg_size_pretty( pg_total_relation_size(\''+ table_name +'\') )'
    res = enter_psql_command(engine, psql_string)
    return res[0][0]

def load_engine():
    user = 'postgres'
    db_name = 'miri_pixel_db'
    #connection_string = 'postgresql+psycopg2://' + user + ':' + password + '@localhost:5433/' + db_name
    connection_string = 'postgresql+psycopg2://' + user + '@localhost/' + db_name
    return create_engine(connection_string, echo=False, pool_timeout=100000)


def init_db(engine):
    """Return session, base, engine, connection, cursor, connection_string objects for connecting to the database.
    connection_string : str
        The connection string to connect to the database. The
        connection string should take the form:
        ``dialect+driver://username:password@host:port/database``
    session : sesson object
        Provides a holding zone for all objects loaded or associated
        with the database.
    base : base object
        Provides a base class for declarative class definitions.
    engine : engine object
        Provides a source of database connectivity and behavior."""
    base = declarative_base(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    base.metadata.reflect(bind = engine)
    connection = engine.raw_connection()
    cursor = connection.cursor()
    return session, base, connection, cursor

def load_miri_tables(base):

    class Detectors(base):
        """ORM for the detectors table"""
        __tablename__ = 'detectors'
        __table_args__ = {'extend_existing': True}
        detector_id = Column(Integer(), primary_key=True,nullable=False)
        name = Column(String(255))
        ncols = Column(Integer())
        nrows = Column(Integer())

    class Pixels(base):
        """ORM for the pixels table"""
        __tablename__ = 'pixels'
        __table_args__ = {'extend_existing': True}
        pixel_id = Column(Integer(), primary_key=True, nullable=False)
        row_id = Column(Integer())
        col_id = Column(Integer())
        ref_pix = Column(Boolean())

    class Exposures(base):
        """ORM for the Exposures table"""
        __tablename__ = 'exposures'
        __table_args__ = {'extend_existing': True}
        exp_id = Column(Integer(), primary_key=True, autoincrement=True) #new
        exp = Column(String(255), nullable=False, unique = True)   #new
        detector_id = Column(Integer(),ForeignKey('detectors.detector_id'))
        data_genesis = Column(String(255))
        ngroups = Column(Integer())
        nints = Column(Integer())
        subarray = Column(String(255))
        readmode = Column(String(255))
        t0 = Column(DateTime)
        t1 = Column(DateTime)
        exptime = Column(Float())
        inttime = Column(Float())
        corr_exp_rel = relationship("CorrectedExposures", backref=backref('exposures', passive_deletes=True))
        ramps_rel = relationship("Ramps", backref=backref('exposures', passive_deletes=True))

    class CorrectedExposures(base):
        """ORM for the CorrectedExposures table"""
        __tablename__ = 'correctedexposures'
        __table_args__ = {'extend_existing': True}
        corrected_exp_id = Column(Integer(), primary_key=True, autoincrement=True)   #new
        corrected_exp = Column(String(255), nullable=False, unique = True)   #new
        exp_id = Column(Integer(), ForeignKey('exposures.exp_id', ondelete="cascade"), index = True) # why indexing is necessary: https://dba.stackexchange.com/questions/37034/very-slow-delete-in-postgresql-workaround
        pipeline_version = Column(String(255))
        crds_version = Column(String(255))
        cal_software_version_control_num = Column(String(255))
        dark_subtraction = Column(Boolean())
        dqinit = Column(Boolean())
        first_frame_correction = Column(Boolean())
        grpscl = Column(Boolean())
        ipc = Column(Boolean())
        jumpdet = Column(Boolean())
        last_frame_correction = Column(Boolean())
        linearity = Column(Boolean())
        ref_pix_correction = Column(Boolean())
        rscd = Column(Boolean())
        saturation_check = Column(Boolean())
        dark_ref_file = Column(String(255))
        gain_ref_file = Column(String(255))
        ipc_ref_file = Column(String(255))
        linear_ref_file = Column(String(255))
        mask_ref_file = Column(String(255))
        readnoise_ref_file = Column(String(255))
        rscd_ref_file = Column(String(255))
        saturation_ref_file = Column(String(255))
        corr_ramp_rel = relationship("CorrectedRamps", backref=backref('correctedexposures', passive_deletes=True))


    class Ramps(base):
        """ORM for the Ramps table"""
        __tablename__ = 'ramps'
        __table_args__ = {'extend_existing': True}
        ramp_id = Column(Integer(), primary_key=True, autoincrement=True)   #new
        pixel_id = Column(Integer(),ForeignKey('pixels.pixel_id'))
        exp_id = Column(Integer(),ForeignKey('exposures.exp_id',ondelete="cascade"), index = True)
        intnumber = Column(Integer())
        ramp = Column(ARRAY(Integer, dimensions = 1))
        UniqueConstraint(exp_id, pixel_id, intnumber, name='unique_ramp_constraint')
        groups_rel = relationship("Groups", backref=backref('ramps', passive_deletes = True))

    class Groups(base):
        """ORM for the Groups table"""
        __tablename__ = 'groups'
        __table_args__ = {'extend_existing': True}
        group_id = Column(Integer(), primary_key=True, autoincrement=True)   #new
        ramp_id = Column(Integer(),ForeignKey('ramps.ramp_id',ondelete="cascade"), index = True)
        group_number = Column(Integer())
        raw_value = Column(Integer())
        UniqueConstraint(ramp_id, group_number, name = 'unique_group_constraint')

    class CorrectedRamps(base):
        """ORM for the CorrectedRamps table"""
        __tablename__ = 'correctedramps'
        __table_args__ = {'extend_existing': True}
        corr_ramp_id = Column(Integer(), primary_key=True, autoincrement=True)   #new
        ramp_id = Column(Integer(),ForeignKey('ramps.ramp_id', ondelete="cascade"), index = True)
        corrected_exp_id = Column(Integer(), ForeignKey('correctedexposures.corrected_exp_id', ondelete="cascade"), index = True)
        slope_value = Column(Float())
        corrected_ramp = Column(ARRAY(Float, dimensions = 1))
        dq_ramp = Column(ARRAY(Integer, dimensions = 1))
        err_ramp = Column(ARRAY(Float, dimensions = 1))
        do_not_use = Column(Boolean())
        saturated = Column(Boolean())
        jump_det = Column(Boolean())
        dropout = Column(Boolean())
        reserved_16 = Column(Boolean())
        reserved_32 = Column(Boolean())
        reserved_64 = Column(Boolean())
        reserved_128 = Column(Boolean())
        unreliable_error = Column(Boolean())
        non_science = Column(Boolean())
        dead = Column(Boolean())
        hot = Column(Boolean())
        warm = Column(Boolean())
        low_qe = Column(Boolean())
        rc = Column(Boolean())
        telegraph = Column(Boolean())
        nonlinear = Column(Boolean())
        bad_ref_pixel = Column(Boolean())
        no_flat_field = Column(Boolean())
        no_gain_value = Column(Boolean())
        no_lin_corr = Column(Boolean())
        no_sat_check = Column(Boolean())
        unreliable_bias = Column(Boolean())
        unreliable_dark = Column(Boolean())
        unreliable_slope = Column(Boolean())
        unreliable_flat = Column(Boolean())
        open = Column(Boolean())
        adj_open = Column(Boolean())
        unreliable_reset = Column(Boolean())
        msa_failed_open = Column(Boolean())
        other_bad_pixel = Column(Boolean())
        UniqueConstraint(corrected_exp_id, ramp_id, name='unique_corrected_ramp_constraint')
        corr_groups_rel = relationship("CorrectedGroups", backref=backref('correctedramps', passive_deletes=True))


    class CorrectedGroups(base):
        """ORM for the CorrectedGroups table"""
        __tablename__ = 'correctedgroups'
        __table_args__ = {'extend_existing': True}
        corr_group_id = Column(Integer(), primary_key=True, autoincrement=True)   #new
        group_id = Column(Integer(),ForeignKey('groups.group_id', ondelete="cascade"), index = True)
        corr_ramp_id = Column(Integer(),ForeignKey('correctedramps.corr_ramp_id',ondelete="cascade"), index = True)
        group_number = Column(Integer())
        corrected_value = Column(Float())
        dq_value = Column(Integer())
        error_value = Column(Float())
        do_not_use = Column(Boolean())
        saturated = Column(Boolean())
        jump_det = Column(Boolean())
        dropout = Column(Boolean())
        reserved_16 = Column(Boolean())
        reserved_32 = Column(Boolean())
        reserved_64 = Column(Boolean())
        reserved_128 = Column(Boolean())
        unreliable_error = Column(Boolean())
        non_science = Column(Boolean())
        dead = Column(Boolean())
        hot = Column(Boolean())
        warm = Column(Boolean())
        low_qe = Column(Boolean())
        rc = Column(Boolean())
        telegraph = Column(Boolean())
        nonlinear = Column(Boolean())
        bad_ref_pixel = Column(Boolean())
        no_flat_field = Column(Boolean())
        no_gain_value = Column(Boolean())
        no_lin_corr = Column(Boolean())
        no_sat_check = Column(Boolean())
        unreliable_bias = Column(Boolean())
        unreliable_dark = Column(Boolean())
        unreliable_slope = Column(Boolean())
        unreliable_flat = Column(Boolean())
        open = Column(Boolean())
        adj_open = Column(Boolean())
        unreliable_reset = Column(Boolean())
        msa_failed_open = Column(Boolean())
        other_bad_pixel = Column(Boolean())
        UniqueConstraint(corr_ramp_id, group_id, name = 'unique_corrected_group_constraint')
