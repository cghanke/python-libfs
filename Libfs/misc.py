"""
collection of function useful for all other modules
"""
from collections import defaultdict
from functools import wraps
import inspect
import logging
import logging.config
import threading
from traceback import format_tb
import os
import re
import sys

# regex to see if a file has been marked as a duplicate
DUPLICATE_COUNTER_RX = re.compile(".* \(libfs:\d+\)$")

# dict to store the actual calltrace
# by thread-identifier
# calltrace[thread.ident] = indentation-level
CALLTRACE_STATE = defaultdict(lambda: 0)
LOGGER = logging.getLogger(__name__)
def calltrace_logger(func):
    """
    decorator to log from where a function or method is called
    and what it returns.
    writes it out as xml so that it can be comfortably viewed in a xml-editor
    """
    @wraps(func)
    def wrapped(*args, **kwargs):
        """
        actual logging wrapper
        """
        logger = logging.getLogger("calltrace")
        this_indent = "\t" * (CALLTRACE_STATE[threading.get_ident()])
        CALLTRACE_STATE[threading.get_ident()] += 1
        try:
            if isinstance(args[0], object):
                class_name = type(args[0]).__name__
            else:
                class_name = "NA"
        except:
            class_name = "WRAPPER-ERROR"

        logger.debug('%s <%s event="Entering" name="%s">',
                     this_indent, class_name, func.__name__)
        logger.debug('%s <args><![CDATA[%s]]></args>',
                     this_indent, (args,))
        logger.debug('%s <kwargs><![CDATA[%s]]></kwargs>',
                     this_indent, (kwargs,))
        try:
            result = func(*args, **kwargs)
        except Exception as excep:
            excep_type, excep_info, traceback = sys.exc_info()
            logger.debug('%s <Exception type="%s" info="%s"><![CDATA[',
                         this_indent, excep_type, excep_info)
            for line in format_tb(traceback):
                logger.debug('%s %s', this_indent, line)
            logger.debug('%s]]></Exception>', this_indent)
            logger.debug('%s </%s>', this_indent, class_name)
            CALLTRACE_STATE[threading.get_ident()] -= 1
            raise excep
        logger.debug('%s <result><![CDATA[%s]]></result>', this_indent, ("%s" % result))
        logger.debug('%s </%s>', this_indent, class_name)
        CALLTRACE_STATE[threading.get_ident()] -= 1
        return result
    return wrapped

@calltrace_logger
def canonicalize_vpath(vpath):
    """
    canonicalize path, so we don't need to care about it anymore
    in the inner modules
    """
    canon_path = os.path.normpath(vpath)
    while canon_path.startswith("/"):
        canon_path = canon_path[1:]
    LOGGER.debug("canonicalize_vpath: %s -> %s", vpath, canon_path)
    return canon_path

@calltrace_logger
def get_vpath_list(vpath):
    """
    return a list of the elements of the already canonicalized vpath
    """
    vpath_list = [v for v in vpath.split("/") if len(v) > 0]
    return vpath_list

@calltrace_logger
def filename_has_duplicate_counter(filename):
    if DUPLICATE_COUNTER_RX.match(filename):
        return True
    return False

def get_available_plugins():
    """
    scans the plugin_dir for .py files and add them
    to the list of available plugins
    The plugin-name on the command-line is the file-name of the plugin without the .py
    suffix
    """
    plugin_dir = "%s/plugins" % os.path.dirname(inspect.stack()[0][1])
    plugin_list = []
    for file_name in os.listdir(plugin_dir):
        module_name, ext = os.path.splitext(file_name) # Handles no-extension files, etc.
        if ext == '.py': # Important, ignore .pyc/other files.
            plugin_list.append(module_name)
    if len(plugin_list) == 0:
        raise RuntimeError("Cannot find any plugins in %s."\
                           "Please check your installation." % plugin_dir)
    return plugin_list
