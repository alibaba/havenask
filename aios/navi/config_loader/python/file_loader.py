import sys
import os
import importlib

def load_file(file_path):
    dir_path, file_name = os.path.split(file_path)
    file_name = os.path.splitext(file_name)[0]
    sys.path.append(dir_path)
    module = importlib.import_module(file_name)
    sys.path.pop()
    return module
