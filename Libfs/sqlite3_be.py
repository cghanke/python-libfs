""""
DB-backend for sqlite3
XXX this is not thread-safe !?
"""

import logging
import sqlite3
from Libfs.misc import calltrace_logger

logger = logging.getLogger(__name__)


class db_backend():
    def __init__(self):
        self.connection=None
        self.cursor=None

    @calltrace_logger
    def open(self,  user,  host,  passwd,   db_path):
        """
        opens a connection and creates a cursor
        """
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        return
    
    @calltrace_logger
    def execute_statment(self,  query_str,  *args):
        """
        log and execute a statement
        """
        logger.debug("Executing %s, %s",  query_str,  args)
        self.cursor.execute(query_str,  args)
        return self.cursor.fetchall()
    
    @calltrace_logger
    def get_columns(self,  table):
            self.cursor.execute("PRAGMA table_info('%s')" % table)
            return [tpl[1] for tpl in self.cursor.fetchall()]
            
    @calltrace_logger
    def commit(self):
        self.connection.commit()
    
    ###
    # Pass on errors from the underlying DB
    ###
    
    IntegrityError = sqlite3.IntegrityError
