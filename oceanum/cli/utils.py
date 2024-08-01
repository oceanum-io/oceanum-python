from pathlib import Path
from platformdirs import user_data_dir

USER_DATA_DIR = Path(user_data_dir('oceanum',  'Oceanum LTD.'))

def bytes_to_human(size):
    """Convert bytes to human-readable format."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size / 1024:.1f} KiB"
    elif size < 1024**3:
        return f"{size / 1024 ** 2:.1f} MiB"
    elif size < 1024**4:
        return f"{size / 1024 ** 3:.1f} GiB"
    else:
        return f"{size / 1024 ** 4:.1f} TiB"


def item_to_long(item, human_readable=False):
    """Convert item to long listing format."""
    size = item["size"]
    if human_readable:
        size = bytes_to_human(size)
    elif size == 0:
        size = ""
    modified = item["modified"] or ""
    return f"{size:>10}  {modified:>32}  {item['name']}", item["size"]

