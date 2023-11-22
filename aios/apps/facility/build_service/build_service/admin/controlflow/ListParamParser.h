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

#include <stddef.h>
#include <string>
#include <vector>

extern "C" {
#include "lua.h"
}

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ListParamParser
{
public:
    ListParamParser();
    ~ListParamParser();

public:
    bool fromJsonString(const std::string& str);
    std::string toJsonString();

    bool fromListString(const std::string& str, const std::string& delim = ";");
    std::string toListString(const std::string& delim = ";");

    void pushBack(const std::string& value);
    bool getValue(size_t idx, std::string& value);
    size_t getSize() const;

public:
    static bool parseFromJsonString(const char* str, std::vector<std::string>& list);
    static bool parseFromListString(const char* str, std::vector<std::string>& list, const std::string& delim = ";");
    static std::string toListString(const std::vector<std::string>& list, const std::string& delim = ";");

private:
    std::vector<std::string> _list;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ListParamParser);

///////////////////////////////////////////////////////////////////////////
class ListParamWrapper
{
public:
    ListParamWrapper() {}

public:
    bool fromJsonString(const char* str);
    const char* toJsonString();

    bool fromListString(const char* str, const char* delim);
    const char* toListString(const char* delim);

    void pushBack(const char* value);
    const char* getValue(int idx);
    int getSize();

public:
    static void bindLua(lua_State* state);

private:
    ListParamParser _parser;
    std::string _str;
};

}} // namespace build_service::admin
