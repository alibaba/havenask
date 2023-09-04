import os
import file_loader

def load(config_path, meta_info, entry_path = None):
    if not entry_path:
        entry_path = os.path.join(config_path, "navi.py")
    try:
        entry = file_loader.load_file(entry_path)
        return entry.config(config_path, meta_info)
    except Exception as e:
        raise Exception("load entry failed [%s], exception: %s" % (entry_path, str(e)))
