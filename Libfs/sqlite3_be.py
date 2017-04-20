""""
DB-backend for sqlite3
XXX this is not thread-safe !?
"""

import logging
import sqlite3
import sys
from Libfs.misc import calltrace_logger

LOGGER = logging.getLogger(__name__)


class db_backend:
    """
    provides the interface to a sqlite3 database
    """

    ###
    # Pass on errors from the underlying DB
    ###

    IntegrityError = sqlite3.IntegrityError

    def __init__(self):
        """
        just declare some variable
        """
        self.connection = None
        self.cursor = None

    @calltrace_logger
    def open(self, user, host, passwd, db_path):
        """
        opens a connection and creates a cursor
        """
        try:
            self.connection = sqlite3.connect(db_path)
        except sqlite3.OperationalError:
            sys.stderr.write("unable to open database file %s\n" % db_path)
            sys.exit(1)
        self.cursor = self.connection.cursor()
        return

    @calltrace_logger
    def execute_statment(self, query_str, *args):
        """
        log and execute a statement
        """
        LOGGER.debug("Executing %s, %s", query_str, args)
        self.cursor.execute(query_str, args)
        return self.cursor.fetchall()

    @calltrace_logger
    def get_columns(self, table):
        """
        return list of a columns (or fields) of a table
        """
        self.cursor.execute("PRAGMA table_info('%s')" % table)
        return [tpl[1] for tpl in self.cursor.fetchall()]

    @calltrace_logger
    def commit(self):
        """
        return a transaction
        """
        self.connection.commit()

    def __repr__(self):
        """
        return a representation useful for debugging
        """
        # XXX to be implemented
        return "XXX"

