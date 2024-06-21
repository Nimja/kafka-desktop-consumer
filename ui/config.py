import os
import json
import sys
from configparser import ConfigParser
import getopt
from .dicthelper import deep_update, get_dict_by_path

DEFAULT_CONFIG = "config.ini"
STORED_VALS_FILE = "config.json"


def deep_update(base: dict, updated: dict):
    """
    Update the original dictionary with values of the updated, merging deeper.
    """
    for k, v in updated.items():
        if k not in base:
            base[k] = v
            continue
        cur = base[k]
        if not isinstance(cur, dict) or not isinstance(v, dict):
            base[k] = v
        else:
            deep_update(cur, v)


class Config:
    def __init__(self, config_file=None):
        config_file = self._get_config_filename(config_file)
        self.vals = self._get_config(config_file)
        self.cache_path = self._get_cache_path(self.vals['main'].get('cache', 'cache'))

        # Update with stored values.
        self._stored_vals = self._load_stored_vals()
        deep_update(self.vals, self._stored_vals)

    def get(self, key:str, default=None):
        """Get config value by key.
        """
        return self.vals.get(key, default)

    def get_path(self, path:str, default=None):
        """Get config value by key path.
        """
        location, key = get_dict_by_path(self.vals, path, {})
        return location.get(key, default)


    def _get_config_filename(self, config_file: str|None) -> str:
        # Parse parameters.
        config_file = config_file if config_file else DEFAULT_CONFIG
        try:
            opts, args = getopt.getopt(sys.argv[1:], "c:")
        except:
            print("Pass -c config if you want to use a different ini file.")
            sys.exit(2)
        for opt, arg in opts:
            if opt in ['-c']:
                config_file = arg

        # Verify the file exists, add .ini optionally.
        if not os.path.exists(config_file):
            if os.path.exists(f"{config_file}.ini"):
                config_file += ".ini"
            else:
                print(f"'{config_file}' could not be found, example in config.sample.ini")
                sys.exit(3)

        return config_file

    def _get_config(self, config_file: str) -> dict:
        """
        Load the config file.
        """
        config = ConfigParser()
        config.read(config_file)
        values = {s:dict(config.items(s)) for s in config.sections()}
        return values

    def _get_cache_path(self, path_name: str) -> str:
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

    def _get_stored_file_name(self) -> str:
        """_summary_

        Returns:
            _type_: _description_
        """
        return os.path.join(self.cache_path, STORED_VALS_FILE)

    def _load_stored_vals(self) -> dict:
        file_name = self._get_stored_file_name()
        if not os.path.exists(file_name) or not os.path.isfile(file_name):
            return {}

        # Write json file away.
        with open(file_name, 'r') as f:
            return json.loads(f.read())

    def save_value(self, path, value) -> None:
        """
        Save value with slash-based json path.

        To store value key in section main: "main/key"
        """

        location, key = get_dict_by_path(self._stored_vals, path)
        location[key] = value
        # Write as json file away.
        file_name = self._get_stored_file_name()
        with open(file_name, 'w') as f:
            f.write(json.dumps(self._stored_vals, indent=4))

config = Config()