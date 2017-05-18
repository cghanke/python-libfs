"""
libfs plugin to deal with id3-tagged files (e.g. mp3)
The metadata itself is always a dict.
"""

from mutagenx.easyid3 import EasyID3
from mutagenx._constants import GENRES

from Libfs.misc import calltrace_logger
import logging
LOGGER = logging.getLogger(__name__)

IGNORE_KEYS = []

@calltrace_logger
def read_metadata(src_filename):
    """
    read the metadata from a sourcefile
    """
    metadata = {}
    audio = EasyID3(src_filename)
    for k in audio.keys():
        metadata[k] = audio[k][0]
    return metadata

@calltrace_logger
def write_metadata(src_filename, metadata):
    """
    write the metadata back to the file
    """
    audio = EasyID3(src_filename)
    for k in metadata.keys():
        audio[k] = metadata[k]
    audio.save()
    return

@calltrace_logger
def is_valid_metadata(key, value):
    """
    check if the given key/value pair makes sense
    e.g. Tracknumber must be an integer
    Ignore it for now
    """
    if key == "genre":
        return value in GENRES
    elif key == "tracknumber":
        try:
            if value == int(value):
                return True
        except:
            return False
    return True

@calltrace_logger
def get_default_view():
    """
    return the default view.
    default_dirtree, default_filename_generator
    """
    return {"dirtree" : ['genre', 'artist', 'date', 'album'],
            "fn_gen" : "%{tracknumber} -- %{title}.mp3"}

@calltrace_logger
def get_valid_keys():
    """
    return a list of all valid keys for this plugin
    """
    valid_keys = []
    # valid_keys may contain characters confusing the shell
    # could write an escape-mechnism for them (:* )
    for k in sorted(EasyID3.valid_keys.keys()):
        if ":" in k: continue
        if "*" in k: continue
        if " " in k: continue
        if k in IGNORE_KEYS: continue
        valid_keys.append(k)
    return valid_keys
