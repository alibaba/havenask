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

#include <string>

extern "C" {
#include "lua.h"
}

#include "autil/legacy/json.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class JsonParamParser
{
public:
    JsonParamParser();
    ~JsonParamParser();

public:
    bool fromJsonString(const std::string& str);
    std::string toJsonString();
    template <typename T>
    bool getTypedValue(const std::string& key, T* value);
    bool getStringValue(const std::string& key, std::string* value);

private:
    bool parseFromJsonString(const char* str);

private:
    autil::legacy::json::JsonMap _jsonMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(JsonParamParser);

///////////////////////////////////////////////////////////////////////////
class JsonParamWrapper
{
public:
    JsonParamWrapper() {}

public:
    bool fromJsonString(const char* str);
    const char* toJsonString();
    const char* getStringValue(const char* key);

public:
    static void bindLua(lua_State* state);

private:
    JsonParamParser _parser;
    std::string _str;
};

}} // namespace build_service::admin
