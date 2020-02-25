#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 17 10:28:20 2019

@author: hagan

Script to create the tables in 'miri_pixel_db'
"""
from miridb import init_db, load_miri_tables, load_engine
import os

os.system('psql -c \'create database miri_pixel_db;\' -U postgres')  # create miri_pixel_db
engine = load_engine()
session, base, connection, cursor = init_db(engine)
load_miri_tables(base)
base.metadata.create_all()
