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
#include "build_service/admin/controlflow/JsonParamParser.h"

#include <assert.h>
#include <iosfwd>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/Eluna.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, JsonParamParser);

JsonParamParser::JsonParamParser() {}

JsonParamParser::~JsonParamParser() {}

bool JsonParamParser::fromJsonString(const string& str) { return parseFromJsonString(str.c_str()); }

string JsonParamParser::toJsonString() { return ToJsonString(_jsonMap, true); }

bool JsonParamParser::parseFromJsonString(const char* str)
{
    _jsonMap.clear();
    if (!str) {
        return true;
    }

    try {
        autil::legacy::FromJsonString(_jsonMap, str);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", str, e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", str);
        return false;
    }
    return true;
}

template <typename T>
bool JsonParamParser::getTypedValue(const std::string& key, T* value)
{
    if (!value) {
        return false;
    }
    auto iter = _jsonMap.find(key);
    if (iter == _jsonMap.end()) {
        return false;
    }
    try {
        FromJson(*value, iter->second);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "typeError on key[%s], catch exception: [%s]", key.c_str(), e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJson on key[%s] ", key.c_str());
        return false;
    }
    return true;
}

bool JsonParamParser::getStringValue(const std::string& key, std::string* value) { return getTypedValue(key, value); }

/////////////////////////////////

bool JsonParamWrapper::fromJsonString(const char* str) { return _parser.fromJsonString(str); }

const char* JsonParamWrapper::toJsonString()
{
    _str = _parser.toJsonString();
    return _str.c_str();
}

const char* JsonParamWrapper::getStringValue(const char* key)
{
    if (!_parser.getStringValue(key, &_str)) {
        return "";
    }
    return _str.c_str();
}

void JsonParamWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<JsonParamWrapper>(state, "JsonParamUtil", ELuna::constructor<JsonParamWrapper>);
    ELuna::registerMethod<JsonParamWrapper>(state, "fromJsonString", &JsonParamWrapper::fromJsonString);
    ELuna::registerMethod<JsonParamWrapper>(state, "toJsonString", &JsonParamWrapper::toJsonString);
    ELuna::registerMethod<JsonParamWrapper>(state, "getStringValue", &JsonParamWrapper::getStringValue);
}

}} // namespace build_service::admin
