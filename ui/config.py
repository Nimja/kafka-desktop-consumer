import os
import sys
from configparser import ConfigParser
import getopt




def get_config():
    """
    Load the config file.
    """
    # Parse parameters.
    config_file = 'config.ini'
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:")
    except:
        print("Pass -c config if you want to use a different ini file.")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ['-c']:
            config_file = arg

    if not os.path.exists(config_file):
        if os.path.exists(f"{config_file}.ini"):
            config_file += ".ini"
        else:
            print(f"'{config_file}' could not be found, example in config.sample.ini")
            sys.exit(3)

    config = ConfigParser()
    config.read(config_file)
    return config, _get_cache_path(config['main'].get('cache', 'cache'))

def _get_cache_path(path_name):
    base_path = os.path.dirname(os.path.abspath(__name__))
    cache_path = os.path.join(base_path, path_name)
    if not os.path.exists(cache_path):
        if os.access(base_path, os.W_OK):
            os.makedirs(cache_path)
        else:
            print(f"Cache path: '{path_name}' could not be found or created, please create.")
            sys.exit(4)

    if not os.access(cache_path, os.W_OK):
        print(f"Cache path: '{path_name}' exists but is not writable?")
        sys.exit(4)

    return cache_path