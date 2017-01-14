class NO_Plugin :
    """
    mimics a plugin, but fails on trying to
    write metadata to a file.
    """

    def __init__(self):
        return

    def write_metadata(self, src_filename, metadata):
        #raise RuntimeError("Metadataplugin not available.")
        return False

    def is_valid_metadata(self, key, value) :
        return False
