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
#include "suez/sdk/JsonNodeRef.h"

#include <assert.h>
#include <cstddef>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <stdint.h>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, JsonNodeRef);

namespace suez {

JsonNodeRef::JsonNodeRef(JsonMap &node) : _node(node) {}

JsonNodeRef::~JsonNodeRef() {}

bool JsonNodeRef::create(const std::string &nodePath, const Any &nodeValue, bool recursive) {
    return set(nodePath, nodeValue, "create", true, recursive);
}

bool JsonNodeRef::update(const std::string &nodePath, const Any &nodeValue) {
    return set(nodePath, nodeValue, "update", false, false);
}

bool JsonNodeRef::set(const std::string &nodePath,
                      const Any &nodeValue,
                      const std::string &operation,
                      bool allowNotExist,
                      bool createIfNotExist) {
    if (nodePath.empty()) {
        _lastError = operation + " with empty node path!";
        return false;
    }
    pair<JsonMap *, string> parseResult = getParent(nodePath, operation, createIfNotExist);
    if (!parseResult.first) {
        return false;
    }
    if (allowNotExist) {
        (*parseResult.first)[parseResult.second] = nodeValue; // override if exist
    } else {
        JsonMap::iterator it = parseResult.first->find(parseResult.second);
        if (it == parseResult.first->end()) {
            _lastError = operation + " node path node exist: " + nodePath;
            return false;
        }
        it->second = nodeValue;
    }
    return true;
}

JsonNodeRef::Any *JsonNodeRef::read(const std::string &nodePath) {
    if (nodePath.empty()) {
        _lastError = "read with empty node path!";
        return NULL;
    }
    pair<JsonMap *, string> parseResult = getParent(nodePath, "read");
    if (!parseResult.first) {
        return NULL;
    }

    JsonMap::iterator it = parseResult.first->find(parseResult.second);
    if (it == parseResult.first->end()) {
        _lastError = "read node[" + nodePath + "] not exist!";
        return NULL;
    }
    return &(it->second);
}

bool JsonNodeRef::del(const std::string &nodePath) {
    if (nodePath.empty()) {
        _lastError = "ERROR: del with empty node path!";
        return false;
    }
    pair<JsonMap *, string> parseResult = getParent(nodePath, "delete");
    if (!parseResult.first) {
        return false;
    }
    parseResult.first->erase(parseResult.second);
    return true;
}

pair<JsonNodeRef::JsonMap *, string>
JsonNodeRef::getParent(const string &nodePath, const string &operation, bool createIfNotExist) {
    vector<string> nodePathVec = split(nodePath);
    assert(nodePathVec.size() != 0);
    JsonMap *node = &_node;
    for (size_t i = 0; i < nodePathVec.size() - 1; ++i) {
        JsonMap::iterator it = node->find(nodePathVec[i]);
        if (it == node->end()) {
            if (!createIfNotExist) {
                _lastError = operation + " " + nodePath + " failed, path node[" + nodePathVec[i] + "] not exist!";
                return make_pair((JsonMap *)NULL, string(""));
            }
            (*node)[nodePathVec[i]] = JsonMap();
            node = AnyCast<JsonMap>(&(*node)[nodePathVec[i]]);
        } else {
            node = AnyCast<JsonMap>(&it->second);
            if (!node) {
                _lastError = operation + " " + nodePath + " failed, path node[" + nodePathVec[i] + "] is not a map!";
                return make_pair((JsonMap *)NULL, string(""));
            }
        }
    }
    return make_pair(node, *(nodePathVec.rbegin()));
}

bool JsonNodeRef::operator==(const JsonNodeRef &other) {
    return ToJsonString(_node) == ToJsonString(other._node) && _lastError == other._lastError;
}

vector<string> JsonNodeRef::split(const std::string &node) { return StringTokenizer::tokenize(StringView(node), '.'); }

JsonMap JsonNodeRef::copy(const JsonMap &from) {
    Any result = JsonMap();
    copyMap(from, result);
    return AnyCast<JsonMap>(result);
}

#define SWITCH_TYPE(src, dst)                                                                                          \
    if (false) {}                                                                                                      \
    SWITCH_ONE_TYPE(std::string, src, dst)                                                                             \
    SWITCH_ONE_TYPE(JsonNumber, src, dst)                                                                              \
    SWITCH_ONE_TYPE(uint8_t, src, dst)                                                                                 \
    SWITCH_ONE_TYPE(int8_t, src, dst)                                                                                  \
    SWITCH_ONE_TYPE(int16_t, src, dst)                                                                                 \
    SWITCH_ONE_TYPE(uint16_t, src, dst)                                                                                \
    SWITCH_ONE_TYPE(int32_t, src, dst)                                                                                 \
    SWITCH_ONE_TYPE(uint32_t, src, dst)                                                                                \
    SWITCH_ONE_TYPE(int64_t, src, dst)                                                                                 \
    SWITCH_ONE_TYPE(uint64_t, src, dst)                                                                                \
    SWITCH_ONE_TYPE(float, src, dst)                                                                                   \
    SWITCH_ONE_TYPE(double, src, dst)                                                                                  \
    SWITCH_ONE_TYPE(bool, src, dst)

#define SWITCH_ONE_TYPE(type, src, dst)                                                                                \
    else if (src.GetType() == typeid(type)) {                                                                          \
        dst = AnyCast<type>(src);                                                                                      \
    }

void JsonNodeRef::copyMap(const Any &from, Any &to) {
    auto fromJsonMap = AnyCast<JsonMap>(from);
    JsonMap toJsonMap;
    for (const auto &kv : fromJsonMap) {
        if (IsJsonArray(kv.second)) {
            copyArray(kv.second, toJsonMap[kv.first]);
        } else if (IsJsonMap(kv.second)) {
            copyMap(kv.second, toJsonMap[kv.first]);
        } else {
            SWITCH_TYPE(kv.second, toJsonMap[kv.first]);
        }
    }
    to = toJsonMap;
}

void JsonNodeRef::copyArray(const Any &from, Any &to) {
    auto fromJsonArray = AnyCast<JsonArray>(from);
    JsonArray toJsonArray;
    toJsonArray.reserve(fromJsonArray.size());
    for (const auto &v : fromJsonArray) {
        Any toValue;
        if (IsJsonArray(v)) {
            copyArray(v, toValue);
        } else if (IsJsonMap(v)) {
            copyMap(v, toValue);
        } else {
            SWITCH_TYPE(v, toValue);
        }
        toJsonArray.push_back(toValue);
    }
    to = toJsonArray;
}

const JsonNodeRef::JsonMap *getField(const JsonNodeRef::JsonMap *jsonMap, const std::string &fieldName) {
    if (!jsonMap) {
        FORMAT_THROW("to get field [%s], but jsonMap is NULL", fieldName.c_str());
    }
    auto fieldIter = jsonMap->find(fieldName);
    if (fieldIter == jsonMap->end()) {
        FORMAT_THROW("field [%s] not exist", fieldName.c_str());
    }
    auto content = autil::legacy::AnyCast<JsonNodeRef::JsonMap>(&fieldIter->second);
    if (!content) {
        FORMAT_THROW("field [%s] is not JsonMap", fieldName.c_str());
    }
    return content;
}

autil::legacy::json::JsonMap *getField(JsonNodeRef::JsonMap *jsonMap, const std::string &fieldName) {
    return const_cast<JsonNodeRef::JsonMap *>(getField(const_cast<const JsonNodeRef::JsonMap *>(jsonMap), fieldName));
}

void JsonNodeRef::castFromJsonMap(const JsonMap &jsonMap, ::google::protobuf::Message *message) {
    const ::google::protobuf::Descriptor *descriptor = message->GetDescriptor();
    for (map<string, Any>::const_iterator it = jsonMap.begin(); it != jsonMap.end(); ++it) {
        const ::google::protobuf::FieldDescriptor *fieldDesc = descriptor->FindFieldByCamelcaseName(it->first);
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

void JsonNodeRef::setOneValue(const ::google::protobuf::FieldDescriptor *fieldDesc,
                              const Any &any,
                              ::google::protobuf::Message *message) {
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

    const ::google::protobuf::Reflection *ref = message->GetReflection();
    switch (fieldDesc->cpp_type()) {
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_INT32, int32_t, Int32);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_UINT32, uint32_t, UInt32);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_INT64, int64_t, Int64);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_UINT64, uint64_t, UInt64);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE, double, Double);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_FLOAT, float, Float);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_STRING, string, String);
        CASE_AND_CONVERT(::google::protobuf::FieldDescriptor::CPPTYPE_BOOL, bool, Bool);
    case ::google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
        const ::google::protobuf::EnumDescriptor *ed = fieldDesc->enum_type();
        string name = AnyCast<string>(any);
        const ::google::protobuf::EnumValueDescriptor *value = ed->FindValueByName(name);
        if (!value) {
            AUTIL_LOG(ERROR, "unrecognized enum [%s]", name.c_str());
            break;
        }
        SET_VALUE(Enum);
        break;
    }
    case ::google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE: {
        ::google::protobuf::Message *subMessage;
        if (fieldDesc->is_repeated()) {
            subMessage = ref->AddMessage(message, fieldDesc);
        } else {
            subMessage = ref->MutableMessage(message, fieldDesc);
        }
        castFromJsonMap(AnyCast<JsonMap>(any), subMessage);
        break;
    }
    default: {
        break;
    }
    }
}

template <typename T>
struct dispatchParse {
    T parse(const autil::legacy::Any &any) {
        if (IsJsonNumber(any)) {
            return JsonNumberCast<T>(any);
        }
        return AnyNumberCast<T>(any);
    }
};

bool IsNumberType(const Any &number) {
    const std::type_info &type = number.GetType();
    return (type == typeid(uint8_t) || type == typeid(int8_t) || type == typeid(uint16_t) || type == typeid(int16_t) ||
            type == typeid(uint32_t) || type == typeid(int32_t) || type == typeid(uint64_t) ||
            type == typeid(int64_t) || type == typeid(float) || type == typeid(double));
}

template <>
struct dispatchParse<std::string> {
    std::string parse(const autil::legacy::Any &any) {
        if (IsNumberType(any)) {
            std::string numberStr;
            json::ToString(any, numberStr, true, "");
            return numberStr;
        }
        return AnyCast<std::string>(any);
    }
};

template <>
struct dispatchParse<bool> {
    bool parse(const autil::legacy::Any &any) { return AnyCast<bool>(any); }
};

template <typename T>
T JsonNodeRef::parseType(const Any &any) {
    dispatchParse<T> p;
    return p.parse(any);
}

} // namespace suez
