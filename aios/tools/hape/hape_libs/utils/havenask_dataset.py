#!/bin/env python
# *-* coding:utf-8 *-*

import os
import json

HERE = os.path.dirname(os.path.realpath(__file__))
import sys
sys.path = [HERE] + sys.path
from hape_libs.utils.havenask_schema import HavenaskSchema


class HavenaskRecord:
    def __init__(self, type, fields, raw_doc, raw_doc_dict):
        self.type = type
        self.fields = fields
        self.raw_doc = raw_doc
        self.raw_doc_dict = raw_doc_dict
        
    def get(self, key):
        return self.fields[key]
        
    def to_sql(self, table):
        keys, values = [], []
        for key, value in self.fields.items():
            keys.append(key)
            try:
                value = value.encode('utf-8')
            except:
                pass
            values.append(value)
        sql = "insert into {} ({}) values ({}) &&kvpair=databaseName:database".format(table, ",".join(keys), ",".join(values))
        return sql

class HavenaskDataSet:
    doc_sep = "\x1e"
    field_sep = "\x1f"
    def __init__(self, file_path, schema_path):
        self.kv_sep = "="
        self.records = []
        self.file_path = os.path.realpath(file_path)
        with open(schema_path) as f:
            self.schema =  HavenaskSchema(json.load(f))
        self.schema.parse()

    def to_sqls(self, table):
        sqls = []
        for record in self.records:
            sqls.append(record.to_sql(table))
        return sqls
    
    def parse(self):
        with open(self.file_path) as f:
            lines = f.readlines()
            
        fields = {}
        type = None
        raw_doc = ""
        raw_doc_dict = {}
        for line in lines:
            index = line.find(self.kv_sep)
            if index != -1:
                key, value = line[:index], line[index+1:]
                value = value[:value.find(HavenaskDataSet.field_sep)]
                raw_doc_dict[key] = value
                if key == "CMD":
                    type = value
                else:
                    if key not in self.schema.field_to_column_type:
                        print("{} is not in schema".format(key))
                        continue
                    else:
                        type = self.schema.field_to_column_type[key]
                        if type == "TEXT" or type == "STRING" or type=="RAW":
                            value = "'"+value+"'"
                        fields[key] = value
                raw_doc += key + "="+value + HavenaskDataSet.field_sep+"\n"
            else:
                record = HavenaskRecord(type, fields, raw_doc = raw_doc + HavenaskDataSet.doc_sep +"\n", raw_doc_dict=raw_doc_dict)
                self.records.append(record)
                fields = {}
                type = None
                raw_doc = ""
                raw_doc_dict = {}