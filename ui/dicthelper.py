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

def get_dict_by_path(base: dict, path: str, default=None):
    """
    Get a location in a deeper dictionary, by slash-divided path.

    Will return the dict holding the key and the last key itself.
    """
    parts = path.split("/")
    key = parts.pop()
    # Start at root.
    location = base
    # Traverse path.
    for part in parts:
        # If not in location, make it.
        if part not in location:
            cur_loc = {}
            location[part] = cur_loc
        else:
            cur_loc = location[part]

        # If it's not a dict, return None, as we can't continue.
        if not isinstance(cur_loc, dict):
            return default

        location = cur_loc

    return location, key
