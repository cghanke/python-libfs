#!/usr/bin/python3
"""
Actual test of EXIF
"""
from test.test_base import TestBase

class EXIFTest(TestBase):
    """
    Actual test using the id3-plugin
    """
    TYPE = "exif"
    LIBFS_SRC_DIR = "./test/data/exif"
    LIBFS_DB = "./test/data/testdb.exif"
    EXISTING_FILE = "%s/Jolla/Jolla/2017/4/21/10:52:2.jpeg" % TestBase.LIBFS_MNT
    EXISTING_DIR = "%s/Jolla/Jolla/2017/4/21" % TestBase.LIBFS_MNT
    NON_EXISTING_FILE = "%s/Jolla/Jolla/2017/4/21/11:52:2.jpeg" % TestBase.LIBFS_MNT
    NON_EXISTING_DIR = "%s/Jolla/Jolla/2017/4/22/" % TestBase.LIBFS_MNT

