#!/usr/bin/python3
"""
Actual test of ID3
"""
from test.test_base import TestBase

class ID3Test(TestBase):
    """
    Actual test using the id3-plugin
    """
    TYPE = "id3"
    LIBFS_SRC_DIR = "./test/data/id3"
    LIBFS_DB = "./test/data/testdb"
    EXISTING_FILE = "%s/A Cappella/Artist A/2000/Album A/1 -- Track A.mp3" % TestBase.LIBFS_MNT
    EXISTING_DIR = "%s/A Cappella/Artist A/2000/Album A" % TestBase.LIBFS_MNT
    NON_EXISTING_FILE = "%s/A Cappella/Artist A/2000/Album A/1 -- Track NON.mp3" % TestBase.LIBFS_MNT
    NON_EXISTING_DIR = "%s/A Cappella/Artist A/2000/Album NON" % TestBase.LIBFS_MNT
