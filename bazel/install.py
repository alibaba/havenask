import os
import shutil
import sys

def main(path):
    with open(path) as f:
        lines = f.read().splitlines()
    rules = [l.split(' -> ') for l in lines]

    dest_dir = os.path.dirname(path)
    srcs = {src: dst for src, dst in rules if src.startswith(dest_dir)}
    srcs[path] = path
    for root, _, files in os.walk(dest_dir):
        for name in files:
            file_path = os.path.join(root, name)
            if file_path not in srcs:
                print("delete file: %s" % file_path)
                os.remove(file_path)

    for src, dst in rules:
        if os.path.islink(src):
            # fs_util linked to file in cache in sandbox, copy to dst as file
            shutil.copyfile(src, dst)
        else:
            realpath = os.path.realpath(src)
            os.symlink(realpath, dst)
    return True

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit(1)
    ret = main(sys.argv[1])
    if not ret:
        sys.exit(1)
    else:
        sys.exit(0)
