"""
libfs plugin to deal with exif-tagged files (e.g. jpg)
The metadata itself is always a dict.
"""

import os
import time

import piexif
#import geocoder

from Libfs.misc import calltrace_logger
import logging
LOGGER = logging.getLogger(__name__)

IGNORE_KEYS = []

VIRT_TIME_KEYS = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']
#VIRT_GEO_KEYS = ['latitude', 'longitude', 'state', 'state_long', 'country', 'country_long',
# 'city', 'street', 'street_long', 'housenumber']

@calltrace_logger
def read_metadata(src_filename):
    """
    read the metadata from a sourcefile
    """
    metadata = {}
    exif_dict = piexif.load(src_filename)
    for ifd in ("0th", "1st", "Image", "Exif", "GPS"):
        if not ifd in exif_dict: continue
        for tag in exif_dict[ifd]:
            # make everything simple a string
            if isinstance(exif_dict[ifd][tag], bytes):
                metadata[piexif.TAGS[ifd][tag]["name"]] = "%s" % (exif_dict[ifd][tag].decode(),)
            else:
                metadata[piexif.TAGS[ifd][tag]["name"]] = "%s" % (exif_dict[ifd][tag],)

    # get Datetime Tag
    LOGGER.debug("read_metadata: %s", metadata)


    # mangle metadata to include also virtual tags
    metadata["Year"] = 1970
    metadata["Month"] = 1
    metadata["Day"] = 1
    metadata["Hour"] = 0
    metadata["Minute"] = 0
    metadata["Second"] = 0

    # 
    # DateTime is called in exiftool ModifyDate
    # DateTimeOriginal should not be changed
    # So we expose the virtual entries only for DateTime, i.e. ModifyDate

    if "DateTime" in metadata.keys():
        time_t = time.strptime(metadata["DateTime"], '%Y:%m:%d %H:%M:%S')
        metadata["Year"] = time_t.tm_year
        metadata["Month"] = time_t.tm_mon
        metadata["Day"] = time_t.tm_mday
        metadata["Hour"] = time_t.tm_hour
        metadata["Minute"] = time_t.tm_min
        metadata["Second"] = time_t.tm_sec
    return metadata

@calltrace_logger
def write_metadata(src_filename, metadata):
    """
    write the metadata back to the file
    """

    # update exif_dict from metadata

    # load original data
    exif_dict = piexif.load(src_filename)
    LOGGER.debug("exif_dict: %s", exif_dict)
    LOGGER.debug("metadata: %s", metadata)
    # Get creation time of original file as default metadata
    fs_time = time.localtime(os.stat(src_filename).st_ctime)
    given_time_kvs = {'Year': fs_time.tm_year, 'Month': fs_time.tm_mon,
                      'Day': fs_time.tm_mday, 'Hour' : fs_time.tm_hour,
                      'Minute' : fs_time.tm_min, 'Second' : fs_time.tm_sec}


    # XXX does not work!
    for key, value in metadata.items():
        if key in VIRT_TIME_KEYS:
            given_time_kvs[key] = int(value)
    # put DateTime as 2017:04:21 10:52:02
    # Make sure this is a bytes-stream
    metadata["DateTime"] = "%d:%02d:%02d %02d:%02d:%02d" %  tuple([given_time_kvs[k] \
                       for k in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']])
    metadata["DateTime"] = bytes(metadata["DateTime"].encode())
    for ifd in ("0th", "1st", "Image", "Exif", "GPS", "Interop"):
        if not ifd in exif_dict.keys(): continue
        for tag in exif_dict[ifd]:
            LOGGER.debug("checking %s", piexif.TAGS[ifd][tag]["name"])
            if not piexif.TAGS[ifd][tag]["name"] in metadata.keys(): continue
            exif_dict[ifd][tag] = metadata[piexif.TAGS[ifd][tag]["name"]]
            LOGGER.debug("relacing %s with %s", piexif.TAGS[ifd][tag]["name"], metadata[piexif.TAGS[ifd][tag]["name"]])
    LOGGER.debug("exif_dict: %s", exif_dict)
    exif_bytes = piexif.dump(exif_dict)
    piexif.remove(src_filename)
    piexif.insert(exif_bytes, src_filename)
    return

@calltrace_logger
def is_valid_metadata(key, value):
    """
    check if the given key/value pair makes sense
    """
    if not key in get_valid_keys():
        return False
    if key == "Year":
        try:
            int(value)
        except ValueError:
            return False
    return True

@calltrace_logger
def get_default_view():
    """
    return the default view.
    default_dirtree, default_filename_generator
    """
    return {"dirtree" : ['Make', 'Model', 'Year', 'Month', 'Day'],
            "fn_gen" : "%{Hour}:%{Minute}:%{Second}.jpeg"}

@calltrace_logger
def get_valid_keys():
    """
    return a list of all valid keys for this plugin
    NOTE: Some keys exist more than once.
    Some keys do not exist and have to be assembled to be put back again.
    """

    valid_keys = [piexif.TAGS['Image'][tag]['name'] for tag in piexif.TAGS['Image']]
    valid_keys += [piexif.TAGS['Exif'][tag]['name'] for tag in piexif.TAGS['Exif']]
    #valid_keys += [piexif.TAGS['GPS'][tag]['name'] for tag in piexif.TAGS['GPS']]
    # Not existing in EXIF
    valid_keys += VIRT_TIME_KEYS
    #valid_keys += VIRT_GEO_KEYS
    # remove duplicates
    valid_keys = list(set(valid_keys))
    return valid_keys
