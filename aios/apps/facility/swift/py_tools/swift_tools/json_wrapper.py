# -*- coding: utf-8 -*-
import json


def write(obj):
    return json.dumps(obj)


def read(s):
    return json.loads(s)
