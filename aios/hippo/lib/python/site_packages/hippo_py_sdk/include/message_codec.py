#!/bin/env python
import json
from google.protobuf.descriptor import FieldDescriptor as FD


class ParseError(Exception): pass

class SimpleProtoField:
    def __init__(self, field_name, field_type, field_value, enum_descriptor = None):
        self.field_name = field_name
        self.field_type = field_type
        self.field_value = field_value
        self.enum_descriptor = enum_descriptor
        
    def name(self):
        return self.field_name
    
    def value(self):
        if self.field_type != FD.TYPE_ENUM or self.enum_descriptor == None:
            return _js2pb_type[self.field_type](self.field_value)
        else:
            #convert enum name to corresponding enum value
            enum_values = self.enum_descriptor.values_by_name
            if self.field_value in enum_values:
                return _js2pb_type[self.field_type](enum_values[self.field_value].number)
            else:
                raise ParseError("FATAL ERROR: enum value[%s] not found " % self.field_value)

    def json_value(self):
        if self.field_type != FD.TYPE_ENUM or self.enum_descriptor == None:
            return _js2pb_type[self.field_type](self.field_value)
        else:
            #convert value name to corresponding enum name
            enum_values = self.enum_descriptor.values_by_number
            if self.field_value in enum_values:
                return enum_values[self.field_value].name
            else:
                raise ParseError("FATAL ERROR: enum value[%s] not found " % self.field_value)
        

def json2pb(pb, js):
    ''' convert JSON string to google.protobuf.descriptor instance '''
    for field in pb.DESCRIPTOR.fields:
        if field.name not in js:
            if not field.is_extension:
                # print "WARN: miss an extension field:%s" % field.name
                continue
            else:
                raise ParseError("FATAL ERROR: required field %s is missing" % field.name)

        value = js[field.name]
        if field.label != FD.LABEL_REPEATED:
            if field.type in _js2pb_type:
                simple_field = SimpleProtoField(field.name, field.type, value, field.enum_type)
                setattr(pb, simple_field.name(), simple_field.value())
            elif field.type == FD.TYPE_MESSAGE:
                json2pb(getattr(pb, field.name, None), value)
            else:
                ParseError("FATAL ERROR: field %s.%s of type '%d' is not supported"
                           % (pb.__class__.__name__, field.name, field.type, ))
        else:
            pb_value = getattr(pb, field.name, None)
            for v in value:
                if field.type in _js2pb_type:
                    simple_field = SimpleProtoField(field.name, field.type, v, field.enum_type)
                    pb_value.append(simple_field.value())
                else:
                    json2pb(pb_value.add(), v)

def pb2json(js, pb):
    ''' convert google.protobuf.descriptor instance to JSON string '''
    # fields = pb.DESCRIPTOR.fields #all fields
    fields = pb.ListFields()        #only filled (including extensions)
    for field, value in fields:
        if field.label != FD.LABEL_REPEATED:
            if field.type in _js2pb_type:
                simple_field = SimpleProtoField(field.name, field.type, value, field.enum_type)
                js_value = simple_field.json_value()
            elif field.type == FD.TYPE_MESSAGE:
                js_value = {}
                pb2json(js_value, value)
            else:
                ParseError("FATAL ERROR: field %s.%s of type '%d' is not supported"
                           % (pb.__class__.__name__, field.name, field.type, ))
        else:
            js_value = []
            for v in value:
                if field.type in _js2pb_type:
                    simple_field = SimpleProtoField(field.name, field.type, v, field.enum_type)
                    js_value.append(simple_field.json_value())
                else:
                    sub_js = {}
                    pb2json(sub_js, v)
                    js_value.append(sub_js)
        js[field.name] = js_value

def pb2json_all(js, pb):
    ''' convert google.protobuf.descriptor instance to JSON string with all fields '''
    # fields = pb._fields
    fields = pb.DESCRIPTOR.fields #all fields
    for field in fields:
        value = getattr(pb, field.name, field.default_value)
        if field.label != FD.LABEL_REPEATED:
            if field.type in _js2pb_type:
                simple_field = SimpleProtoField(field.name, field.type, value, field.enum_type)
                js_value = simple_field.json_value()
            elif field.type == FD.TYPE_MESSAGE:
                js_value = {}
                pb2json_all(js_value, value)
            else:
                ParseError("FATAL ERROR: field %s.%s of type '%d' is not supported"
                           % (pb.__class__.__name__, field.name, field.type, ))
        else:
            js_value = []
            for v in value:
                if field.type in _js2pb_type:
                    simple_field = SimpleProtoField(field.name, field.type, v, field.enum_type)
                    js_value.append(simple_field.json_value())
                else:
                    sub_js = {}
                    pb2json_all(sub_js, v)
                    js_value.append(sub_js)
        js[field.name] = js_value

                
_js2pb_type = {
    FD.TYPE_DOUBLE: float,
    FD.TYPE_FLOAT: float,
    FD.TYPE_INT64: long,
    FD.TYPE_UINT64: long,
    FD.TYPE_INT32: int,
    FD.TYPE_FIXED64: float,
    FD.TYPE_FIXED32: float,
    FD.TYPE_BOOL: bool,
    FD.TYPE_STRING: unicode,
    # FD.TYPE_MESSAGE: json2pb | pb2js,     #handled specially
    FD.TYPE_BYTES: lambda x: x.decode('string_escape'),
    FD.TYPE_UINT32: int,
    FD.TYPE_ENUM: int,
    FD.TYPE_SFIXED32: float,
    FD.TYPE_SFIXED64: float,
    FD.TYPE_SINT32: int,
    FD.TYPE_SINT64: long,
    }
