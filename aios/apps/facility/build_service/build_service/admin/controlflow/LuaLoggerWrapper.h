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
#ifndef ISEARCH_BS_LUALOGGERWRAPPER_H
#define ISEARCH_BS_LUALOGGERWRAPPER_H

#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

////////////////////////////////////////////////////////////////////

class LuaLoggerWrapper
{
public:
    LuaLoggerWrapper() : _logCounter(0) {}

public:
    void setPrefix(const char* prefix);
    void setLocation(const char* hostId, const char* fileName, const char* lineStr);

    void log(const char* level, const char* msg);
    void interval_log(const char* level, const char* msg, uint32_t interval);

public:
    static void bindLua(lua_State* state);
    static std::string rewrite(const std::string& luaScript);

private:
    std::string _prefix;
    std::string _locationStr;
    uint32_t _logCounter;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin

#endif // ISEARCH_BS_LUALOGGERWRAPPER_H
