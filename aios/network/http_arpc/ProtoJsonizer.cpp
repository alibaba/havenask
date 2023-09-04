/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ProtoJsonizer.h"

#include "Log.h"
#include "google/protobuf/descriptor.h"

using namespace std;
using namespace ::google::protobuf;
using namespace autil::legacy;
using namespace autil::legacy::json;

HTTP_ARPC_DECLARE_AND_SETUP_LOGGER(ProtoJsonizer);

namespace http_arpc {

const string ProtoJsonizer::ORIGINAL_STRING = "originalString";
const string ProtoJsonizer::HTTP_STATUS_CODE = "httpStatusCode";

ProtoJsonizer::ProtoJsonizer() {}

ProtoJsonizer::~ProtoJsonizer() {}

bool ProtoJsonizer::fromJsonString(const string &jsonStr, Message *message) {
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, jsonStr);
    } catch (const ExceptionBase &e) {
        HTTP_ARPC_LOG(DEBUG, "request is not json format, set request to original string");
        jsonMap[ORIGINAL_STRING] = Any(jsonStr);
    }

    try {
        fromJsonMap(jsonMap, message);
    } catch (const ExceptionBase &e) {
        HTTP_ARPC_LOG(ERROR, "%s", e.what());
        return false;
    }
    return true;
}

string ProtoJsonizer::toJsonString(const Message &message, bool outputDefault) {
    JsonMap jsonMap;
    toJsonMap(message, jsonMap, outputDefault);
    return ToJsonString(jsonMap);
}

string ProtoJsonizer::toJsonString(const Message &message, const string &responseField, bool outputDefault) {
    JsonMap jsonMap;
    toJsonMap(message, jsonMap, outputDefault);

    JsonMap::const_iterator it = jsonMap.find(responseField);
    if (it != jsonMap.end()) {
        if (IsJsonString(it->second)) {
            return AnyCast<string>(it->second);
        }
    }
    return ToJsonString(jsonMap);
}

bool ProtoJsonizer::fromJson(const string &jsonStr, Message *message) { return fromJsonString(jsonStr, message); }

string ProtoJsonizer::toJson(const Message &message) { return toJsonString(message, _responseField, false); }

int ProtoJsonizer::getStatusCode(const google::protobuf::Message &message) {
    const Descriptor *descriptor = message.GetDescriptor();
    const FieldDescriptor *fieldDesc = descriptor->FindFieldByName(HTTP_STATUS_CODE);
    if (fieldDesc != NULL && fieldDesc->cpp_type() == FieldDescriptor::CPPTYPE_INT32) {
        const Reflection *ref = message.GetReflection();
        return ref->GetInt32(message, fieldDesc);
    } else {
        return 200; // HTTP OK
    }
}

template <FieldDescriptor::CppType cppType>
struct Traits {};

#define declare_type_traits(cppType, Type)                                                                             \
    template <>                                                                                                        \
    struct Traits<cppType> {                                                                                           \
        typedef Type ValueType;                                                                                        \
        static ValueType getDefaultValue(const FieldDescriptor *desc) { return desc->default_value_##Type(); }         \
    }

declare_type_traits(FieldDescriptor::CPPTYPE_INT32, int32);
declare_type_traits(FieldDescriptor::CPPTYPE_UINT32, uint32);
declare_type_traits(FieldDescriptor::CPPTYPE_INT64, int64);
declare_type_traits(FieldDescriptor::CPPTYPE_UINT64, uint64);
declare_type_traits(FieldDescriptor::CPPTYPE_DOUBLE, double);
declare_type_traits(FieldDescriptor::CPPTYPE_FLOAT, float);
declare_type_traits(FieldDescriptor::CPPTYPE_STRING, string);
declare_type_traits(FieldDescriptor::CPPTYPE_BOOL, bool);

#undef declare_type_traits

template <FieldDescriptor::CppType cppType, typename T = typename Traits<cppType>::ValueType>
static T getDefaultValue(const FieldDescriptor *fieldDesc) {
    return Traits<cppType>::getDefaultValue(fieldDesc);
}

void ProtoJsonizer::toJsonMap(const Message &message, JsonMap &jsonMap, bool outputDefault) {
#define SET_ANY(cppType, funcSuffix)                                                                                   \
    case cppType: {                                                                                                    \
        if (fieldDesc->is_repeated()) {                                                                                \
            size_t count = ref->FieldSize(message, fieldDesc);                                                         \
            if (count > 0) {                                                                                           \
                JsonArray anyArray;                                                                                    \
                for (size_t i = 0; i < count; ++i) {                                                                   \
                    anyArray.push_back(ref->GetRepeated##funcSuffix(message, fieldDesc, i));                           \
                }                                                                                                      \
                jsonMap[name] = Any(anyArray);                                                                         \
            }                                                                                                          \
        } else {                                                                                                       \
            if (ref->HasField(message, fieldDesc)) {                                                                   \
                jsonMap[name] = Any(ref->Get##funcSuffix(message, fieldDesc));                                         \
            } else if (outputDefault) {                                                                                \
                jsonMap[name] = Any(getDefaultValue<cppType>(fieldDesc));                                              \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

    const Descriptor *descriptor = message.GetDescriptor();
    const Reflection *ref = message.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const FieldDescriptor *fieldDesc = descriptor->field(i);
#ifdef HTTP_ARPC_NO_CAMELCASE_NAME
        const string &name = fieldDesc->name();
#else
        const string &name = fieldDesc->camelcase_name();
#endif
        if (name == HTTP_STATUS_CODE) {
            continue;
        }
        switch (fieldDesc->cpp_type()) {
            SET_ANY(FieldDescriptor::CPPTYPE_INT32, Int32);
            SET_ANY(FieldDescriptor::CPPTYPE_UINT32, UInt32);
            SET_ANY(FieldDescriptor::CPPTYPE_INT64, Int64);
            SET_ANY(FieldDescriptor::CPPTYPE_UINT64, UInt64);
            SET_ANY(FieldDescriptor::CPPTYPE_DOUBLE, Double);
            SET_ANY(FieldDescriptor::CPPTYPE_FLOAT, Float);
            SET_ANY(FieldDescriptor::CPPTYPE_STRING, String);
            SET_ANY(FieldDescriptor::CPPTYPE_BOOL, Bool);
        case FieldDescriptor::CPPTYPE_ENUM: {
            if (fieldDesc->is_repeated()) {
                size_t count = ref->FieldSize(message, fieldDesc);
                if (count > 0) {
                    JsonArray anyArray;
                    for (size_t i = 0; i < count; ++i) {
                        anyArray.push_back(Any(ref->GetRepeatedEnum(message, fieldDesc, i)->name()));
                    }
                    jsonMap[name] = Any(anyArray);
                }
            } else {
                if (ref->HasField(message, fieldDesc)) {
                    jsonMap[name] = Any(ref->GetEnum(message, fieldDesc)->name());
                }
            }
            break;
        }
        case FieldDescriptor::CPPTYPE_MESSAGE: {
            if (fieldDesc->is_repeated()) {
                size_t count = ref->FieldSize(message, fieldDesc);
                if (count > 0) {
                    JsonArray anyArray;
                    for (size_t i = 0; i < count; ++i) {
                        JsonMap subJsonMap;
                        toJsonMap(ref->GetRepeatedMessage(message, fieldDesc, i), subJsonMap, outputDefault);
                        anyArray.push_back(Any(subJsonMap));
                    }
                    jsonMap[name] = Any(anyArray);
                }
            } else {
                if (ref->HasField(message, fieldDesc)) {
                    JsonMap subJsonMap;
                    toJsonMap(ref->GetMessage(message, fieldDesc), subJsonMap, outputDefault);
                    jsonMap[name] = Any(subJsonMap);
                }
            }
            break;
        }
        default: {
            break;
        }
        }
    }
#undef SET_ANY
}

void ProtoJsonizer::fromJsonMap(const JsonMap &jsonMap, Message *message) {
    const Descriptor *descriptor = message->GetDescriptor();
    for (map<string, Any>::const_iterator it = jsonMap.begin(); it != jsonMap.end(); ++it) {
        const FieldDescriptor *fieldDesc = descriptor->FindFieldByCamelcaseName(it->first);
        if (!fieldDesc) {
            continue;
        }
        if (fieldDesc->is_repeated()) {
            const JsonArray &array = AnyCast<JsonArray>(it->second);
            for (size_t i = 0; i < array.size(); ++i) {
                setOneValue(fieldDesc, array[i], message);
            }
        } else {
            setOneValue(fieldDesc, it->second, message);
        }
    }
}

void ProtoJsonizer::setOneValue(const FieldDescriptor *fieldDesc, const Any &any, Message *message) {
#define SET_VALUE(funcSuffix)                                                                                          \
    {                                                                                                                  \
        if (fieldDesc->is_repeated()) {                                                                                \
            ref->Add##funcSuffix(message, fieldDesc, value);                                                           \
        } else {                                                                                                       \
            ref->Set##funcSuffix(message, fieldDesc, value);                                                           \
        }                                                                                                              \
        break;                                                                                                         \
    }
#define CASE_AND_CONVERT(cpptype, type, funcSuffix)                                                                    \
    {                                                                                                                  \
    case cpptype:                                                                                                      \
        const type &value = parseType<type>(any);                                                                      \
        SET_VALUE(funcSuffix);                                                                                         \
        break;                                                                                                         \
    }

    const Reflection *ref = message->GetReflection();
    switch (fieldDesc->cpp_type()) {
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_INT32, int32_t, Int32);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_UINT32, uint32_t, UInt32);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_INT64, int64_t, Int64);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_UINT64, uint64_t, UInt64);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_DOUBLE, double, Double);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_FLOAT, float, Float);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_STRING, string, String);
        CASE_AND_CONVERT(FieldDescriptor::CPPTYPE_BOOL, bool, Bool);
    case FieldDescriptor::CPPTYPE_ENUM: {
        const EnumDescriptor *ed = fieldDesc->enum_type();
        string name = AnyCast<string>(any);
        const EnumValueDescriptor *value = ed->FindValueByName(name);
        if (!value) {
            HTTP_ARPC_LOG(ERROR, "unrecognized enum [%s]", name.c_str());
            break;
        }
        SET_VALUE(Enum);
        break;
    }
    case FieldDescriptor::CPPTYPE_MESSAGE: {
        Message *subMessage;
        if (fieldDesc->is_repeated()) {
            subMessage = ref->AddMessage(message, fieldDesc);
        } else {
            subMessage = ref->MutableMessage(message, fieldDesc);
        }
        fromJsonMap(AnyCast<JsonMap>(any), subMessage);
        break;
    }
    default: {
        break;
    }
    }
}

template <typename T>
T ProtoJsonizer::parseType(const Any &any) {
    return JsonNumberCast<T>(any);
}

void ProtoJsonizer::setResponseField(const string &responseField) { _responseField = responseField; }

} // namespace http_arpc
