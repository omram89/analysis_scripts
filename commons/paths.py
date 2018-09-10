import os
import sys

"""
paths usage example

paths.THESIS
paths.TEST_DATA
paths.CLUSTER_DATA
paths.EV_TIME_BINNED
"""

def __get_thesis_base_path(path=None, resolve_full_path=True):
    """
    Finds path to the Thesis folder from given path.

    Args:
        path (optional): input path, defaults to path of this file
        resolve_full_path (optional): whether input path should be
            resolved to absolute path. Defaults to False.
    
    Returns:
        Path to the Thesis folder, None if Thesis folder is not found.
    """

    if path is None:
        path = __file__
    
    # Make slashes consistent
    path = path.replace('\\', os.sep)
    path = path.replace('/', os.sep)
    # Resolve full path and normalize
    if resolve_full_path:
        path = os.path.realpath(path) # realpath normalizes path (eg. deals with double slashes, . and ..)
    else:
        path = os.path.normpath(path) # if full path is not resolved, path still needs normalization
    
    # Return if Thesis is not in path at all
    if 'Thesis' not in path.split(os.sep):
        return None

    # Keep looking for parent directories until we find Thesis
    while True:
        path = os.path.dirname(path)
        if os.path.basename(path) == 'Thesis':
            return path

# Correct paths if running from thumb drive
# full_path = os.path.abspath(os.path.realpath(__file__))
# if full_path.startswith('/media') and 'B610-45CD' in full_path:
#     pos = full_path.find('B610-45CD')
#     new_base_path = full_path[:(pos + len('B610-45CD'))]

def __on_lab_machine():
    import platform
    return platform.node() == '0563427w6t172l'

THESIS = __get_thesis_base_path()
TEST_DATA = os.path.join(THESIS, 'Test_Data')
SOURCE_DATA = os.path.join(THESIS, 'Source Data and Work done')
CLUSTER_DATA = os.path.join(SOURCE_DATA, 'datashare3')
WORKLOAD_DATA = os.path.join(SOURCE_DATA, 'datashare4-shared')
EV_TIME_BINNED = os.path.join(THESIS, 'evTimeBinned')

CACHE = os.path.join(EV_TIME_BINNED, 'Cache')
os.makedirs(CACHE, exist_ok=True)

if __name__ == '__main__':
    print(dir())