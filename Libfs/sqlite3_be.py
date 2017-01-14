""""
DB-backend for sqlite3
XXX this is not thread-safe !?
"""

import logging
import sqlite3
from Libfs.misc import calltrace_logger

logger = logging.getLogger(__name__)

global connection,  cursor
connection=None
cursor=None

@calltrace_logger
def open(user,  host,  passwd,   db_path):
    """
    opens a connection and creates a cursor
    """
    global connection,  cursor
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    return

@calltrace_logger
def execute_statment(query_str,  *args):
    """
    log and execute a statement
    """
    logger.debug("Executing %s, %s",  query_str,  args)
    cursor.execute(query_str,  args)
    return cursor.fetchall()

@calltrace_logger
def get_columns(table):
        cursor.execute("PRAGMA table_info('%s')" % table)
        return [tpl[1] for tpl in cursor.fetchall()]
        
@calltrace_logger
def commit():
    connection.commit()

###
# Pass on errors from the underlying DB
###

IntegrityError = sqlite3.IntegrityError
