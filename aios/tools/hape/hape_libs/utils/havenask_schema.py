import json
import copy

class HavenaskSchema:
    def __init__(self, raw_schema):
        self.raw_schema = raw_schema
        self.schema = copy.deepcopy(raw_schema)
        self.index_type_to_field = {}
        self.column_type_to_field = {}
        self.field_to_column_type = {}
        self.indexes = []
        self.fields = []
        self.columns = []
        self.shard_field = None


    def parse(self, enable_parse_types=True):
        # print(self.schema)
        self.indexes = self.schema["indexes"]
        self.columns = self.schema["columns"]

        self._parse_shard_field()
        if enable_parse_types:
            self._parse_types()
        # self._process_attribute()
        # self._process_dup_text_index()


    def find_column_by_name(self, col_name):
        for no, column in enumerate(self.columns):
            name = column["name"]
            if col_name == name:
                return no
        return -1


    def _parse_shard_field(self):
        for index_info in self.indexes:
            if index_info['index_type'] == 'PRIMARY_KEY64':
                for field_info in index_info['index_config']['index_fields']:
                    self.shard_field = field_info["field_name"]

        if self.shard_field == None:
            raise RuntimeError("No index of type [PRIMARY_KEY64] found in schema")

    def _parse_types(self):
        for column in self.columns:
            name, type = column["name"], column["type"]
            self.fields.append(name)
            self.field_to_column_type[name] = type
            if type not in self.column_type_to_field:
                self.column_type_to_field[type] = []
            self.column_type_to_field[type].append(name)

        for index_info in self.indexes:
            type = index_info["index_type"]
            for index_field in index_info['index_config']["index_fields"]:
                name = index_field["field_name"]
                if type not in self.index_type_to_field:
                    self.index_type_to_field[type] = set()
                self.index_type_to_field[type].add(name)


    def _process_attribute(self):
        ## ensure every field has attribute
        for field in self.fields:
            # import pdb
            # pdb.set_trace()
            if field not in self.index_type_to_field.get("ATTRIBUTE", []):
                self.indexes.append(
                    {
                        "name": field,
                        "index_type": "ATTRIBUTE",
                        "index_config": {
                            "index_fields": [
                                {
                                    "field_name": field
                                }
                            ]
                        }
                    }
                )

    def _process_dup_text_index(self):
        for field in self.fields:
            if field.startswith("DUP_"):
                raise RuntimeError("Cannot use 'DUP_' as prefix of column name")

        ## add dup for text field
        for field in self.column_type_to_field.get("TEXT", []):
            # import pdb
            # pdb.set_trace()
            no = self.find_column_by_name(field)
            ori_column = copy.deepcopy(self.columns[no])
            ori_column = {
                "name": field,
                "type": "STRING"
            }
            dup_column = copy.deepcopy(self.columns[no])
            dup_column["name"] = "DUP_" + field
            self.columns[no] = ori_column
            self.columns.append(dup_column)


            for index in self.indexes:
                if index['index_type'] != "TEXT" and index['index_type'] != "PACK":
                    continue
                for index_field in index['index_config']['index_fields']:
                    if index_field['field_name'] == field:
                        index_field['field_name'] = "DUP_" + field



if __name__ == "__main__":
    schema = {
    "columns": [
        {
            "name": "title",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "subject",
            "type": "TEXT",
            "analyzer": "simple_analyzer"
        },
        {
            "name": "id",
            "type": "UINT32"
        },
        {
            "name": "hits",
            "type": "UINT32"
        },
        {
            "name": "createtime",
            "type": "UINT64"
        }
    ],
    "indexes": [
        {
            "name": "id",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "id"
                    }
                ]
            },
            "index_type": "PRIMARY_KEY64"
        },
        {
            "name": "title",
            "index_config" : {
                "index_fields": [
                    {
                        "field_name": "title"
                    }
                ]
            },
            "index_type": "TEXT"
        },
        {
            "name": "default",
            "index_type": "PACK",
            "index_config": {
                "index_fields": [
                    {
                        "field_name": "title",
                        "boost": 100
                    },
                    {
                        "field_name": "subject",
                        "boost": 200
                    }
                ]
            }
        }
    ]
    }

    ha_schema = HavenaskSchema(schema)
    ha_schema.parse()
    print(json.dumps(ha_schema.schema, indent=4))
