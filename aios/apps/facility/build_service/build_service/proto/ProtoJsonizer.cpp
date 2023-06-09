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
#include "build_service/proto/ProtoJsonizer.h"

#include "google/protobuf/descriptor.h"

using namespace std;
using namespace ::google::protobuf;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, ProtoJsonizer);

bool ProtoJsonizer::fromJsonString(const string& jsonStr, Message* message)
{
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, jsonStr);
        doFromJsonString(jsonMap, message);
    } catch (const ExceptionBase& e) {
        string errorMsg = string(e.what());
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

string ProtoJsonizer::toJsonString(const Message& message)
{
    JsonMap jsonMap;
    doToJsonString(message, jsonMap);
    return ToJsonString(jsonMap);
}

void ProtoJsonizer::doToJsonString(const Message& message, JsonMap& jsonMap)
{
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
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

    const Descriptor* descriptor = message.GetDescriptor();
    const Reflection* ref = message.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const FieldDescriptor* fieldDesc = descriptor->field(i);
        const string& name = fieldDesc->name();
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
                        doToJsonString(ref->GetRepeatedMessage(message, fieldDesc, i), subJsonMap);
                        anyArray.push_back(Any(subJsonMap));
                    }
                    jsonMap[name] = Any(anyArray);
                }
            } else {
                if (ref->HasField(message, fieldDesc)) {
                    JsonMap subJsonMap;
                    doToJsonString(ref->GetMessage(message, fieldDesc), subJsonMap);
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

void ProtoJsonizer::doFromJsonString(const JsonMap& jsonMap, Message* message)
{
    const Descriptor* descriptor = message->GetDescriptor();
    for (map<string, Any>::const_iterator it = jsonMap.begin(); it != jsonMap.end(); ++it) {
        const FieldDescriptor* fieldDesc = descriptor->FindFieldByCamelcaseName(it->first);
        if (!fieldDesc) {
            throw ExceptionBase("Invalid field: [" + it->first + "]");
        }
        if (fieldDesc->is_repeated()) {
            const JsonArray& array = AnyCast<JsonArray>(it->second);
            for (size_t i = 0; i < array.size(); ++i) {
                setOneValue(fieldDesc, array[i], message);
            }
        } else {
            setOneValue(fieldDesc, it->second, message);
        }
    }
}

void ProtoJsonizer::setOneValue(const FieldDescriptor* fieldDesc, const Any& any, Message* message)
{
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
        const type& value = parseType<type>(any);                                                                      \
        SET_VALUE(funcSuffix);                                                                                         \
        break;                                                                                                         \
    }

    const Reflection* ref = message->GetReflection();
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
        const EnumDescriptor* ed = fieldDesc->enum_type();
        const EnumValueDescriptor* value = ed->FindValueByName(AnyCast<string>(any));
        SET_VALUE(Enum);
        break;
    }
    case FieldDescriptor::CPPTYPE_MESSAGE: {
        Message* subMessage;
        if (fieldDesc->is_repeated()) {
            subMessage = ref->AddMessage(message, fieldDesc);
        } else {
            subMessage = ref->MutableMessage(message, fieldDesc);
        }
        doFromJsonString(AnyCast<JsonMap>(any), subMessage);
        break;
    }
    default: {
        break;
    }
    }
}

template <typename T>
T ProtoJsonizer::parseType(const Any& any)
{
    if (IsJsonNumber(any)) {
        return JsonNumberCast<T>(any);
    } else {
        return AnyCast<T>(any);
    }
}

}} // namespace build_service::proto
