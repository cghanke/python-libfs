
from collections import defaultdict
from functools import wraps
import inspect
import logging
import logging.config
import threading
import os

# dict to store the actual calltrace
# by thread-identifier
# calltrace[thread.ident] = indentation-level 
calltrace_state =  defaultdict(lambda : 0)
logger = logging.getLogger(__name__)

def calltrace_logger(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        calltrace_logger = logging.getLogger("calltrace")
        this_indent = "\t" * calltrace_state[threading.get_ident()]
        calltrace_state[threading.get_ident()] += 1
        try:
            if isinstance(args[0],  object):
                class_name =  type(args[0]).__name__
            else :
                class_name = ""
        except:
            class_name = ""

        calltrace_logger.debug("%s%s.%s:: Entering with args=%s and kwargs=%s" % (this_indent, class_name,  func.__name__, args, kwargs))
        result = func(*args, **kwargs)
        calltrace_logger.debug("%s%s.%s:: Leaving with result=%s" % (this_indent, class_name,  func.__name__, ("%s" % result)[:100]))
        calltrace_state[threading.get_ident()] -= 1
        return result
    return wrapped
    
@calltrace_logger
def canonicalize_vpath(vpath) :
    canon_path = os.path.normpath(vpath)
    while canon_path.startswith("/") :
        canon_path = canon_path[1:]
    logger.debug("canonicalize_vpath: %s -> %s", vpath, canon_path)
    return canon_path

@calltrace_logger
def get_vpath_list(vpath) :
    """
    return a list of the elements of the already canonicalized vpath
    """
    vpath_list = [ v for v in vpath.split("/") if len(v) > 0]
    return vpath_list



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
        raise RuntimeError("Cannot find any plugins in %s. Please check your installation." % plugin_dir)
    return plugin_list
