import json
from google.protobuf.json_format import MessageToJson, Parse


class BaseBuilder(object):
    def __init__(self, msg):
        self.msg = msg

    def to_json(self):
        return json.loads(MessageToJson(self.msg))

    def from_json(self, content):
        Parse(content, self.msg)
        return self

    def build(self):
        return self.msg
