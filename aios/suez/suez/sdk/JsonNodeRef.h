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
#pragma once

#include <map>
#include <stdio.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"

namespace google {
namespace protobuf {
class FieldDescriptor;
class Message;
} // namespace protobuf
} // namespace google

namespace suez {

class JsonNodeRef {
public:
    typedef autil::legacy::Any Any;
    typedef autil::legacy::json::JsonMap JsonMap;

public:
    JsonNodeRef(JsonMap &target);
    ~JsonNodeRef();

public:
    bool create(const std::string &nodePath, const Any &nodeValue, bool recursive = false);
    bool update(const std::string &nodePath, const Any &nodeValue);
    Any *read(const std::string &nodePath);
    bool del(const std::string &nodePath);

public:
    static std::vector<std::string> split(const std::string &node);
    static JsonMap copy(const JsonMap &from);
    static void castFromJsonMap(const autil::legacy::json::JsonMap &jsonMap, ::google::protobuf::Message *message);

public:
    const JsonMap &get() const { return _node; }
    JsonMap &get() { return _node; }
    const std::string &getLastError() const { return _lastError; }

public:
    bool operator==(const JsonNodeRef &other);

private:
    std::pair<JsonMap *, std::string>
    getParent(const std::string &nodePath, const std::string &operation, bool createIfNotExist = false);
    bool set(const std::string &nodePath,
             const Any &nodeValue,
             const std::string &operation,
             bool allowNotExist,
             bool createIfNotExist = false);
    static void copyMap(const Any &from, Any &to);
    static void copyArray(const Any &from, Any &to);
    static void setOneValue(const ::google::protobuf::FieldDescriptor *fieldDesc,
                            const autil::legacy::Any &any,
                            ::google::protobuf::Message *message);
    template <typename T>
    static T parseType(const autil::legacy::Any &any);

private:
    JsonMap &_node;
    std::string _lastError;
};

#define FORMAT_THROW(format, ...)                                                                                      \
    do {                                                                                                               \
        char buf[1024];                                                                                                \
        snprintf(buf, sizeof(buf), format, __VA_ARGS__);                                                               \
        std::string msg(buf);                                                                                          \
        AUTIL_LEGACY_THROW(autil::legacy::ExceptionBase, msg);                                                         \
    } while (0)

const JsonNodeRef::JsonMap *getField(const JsonNodeRef::JsonMap *jsonMap, const std::string &fieldName);
JsonNodeRef::JsonMap *getField(JsonNodeRef::JsonMap *jsonMap, const std::string &fieldName);

template <typename T>
T getField(const JsonNodeRef::JsonMap *jsonMap, const std::string &fieldName, const T &defaultValue) {
    if (!jsonMap) {
        FORMAT_THROW("to get field [%s], but jsonMap is NULL", fieldName.c_str());
    }
    auto fieldIter = jsonMap->find(fieldName);
    if (fieldIter == jsonMap->end()) {
        return defaultValue;
    } else {
        T ret;
        FromJson(ret, fieldIter->second);
        return ret;
    }
}

} // namespace suez
