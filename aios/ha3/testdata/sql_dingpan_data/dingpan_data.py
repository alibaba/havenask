# -*- coding: utf-8 -*-

import random

path_item = ["a", "b", "c", "d", "e", "f", "g"]


def gen_path(min_len, max_len):
    paths = []
    path_len = random.randint(min_len, max_len)
    for i in range(path_len):
        paths.append(path_item[random.randint(0, len(path_item)-1)])
    return "/" + "/".join(paths)

def get_multi_path(path):
    paths = [path]
    path_pair = path.rsplit("/", 1)
    while path_pair[0]:
        paths.append(path_pair[0])
        path_pair = path_pair[0].rsplit("/", 1)
    paths.reverse()
    return "".join(paths)

def gen_file_table(size):
    docs = []
    for i in range(size):
        title = "key" if i % 2 == 0 else str(i)
        path = gen_path(2, 7)
        join_path = get_multi_path(path)
        docs.append("cmd=add,fid=%s,title=t%s,path=%s,join_path=%s;" % (i, title, path, join_path))
    return docs

def gen_acl_table(size):
    docs = []
    for i in range(size):
        path = gen_path(1, 7)
        group = random.randint(0, 5)
        policy = random.randint(0, 10) / 2
        ignore = random.randint(0, 10) / 9
        docs.append("cmd=add,aid=%s,path=%s,group=%s,policy=%s,ignore=%s;" % (
            i, path, group, policy, ignore))
    return docs


if __name__ == "__main__":
    file_docs = gen_file_table(20000)
    with open("file_docs", "w") as fd:
        fd.write("".join(file_docs))

    acl_docs = gen_acl_table(2000)
    with open("acl_docs", "w") as fd:
        fd.write("".join(acl_docs))
