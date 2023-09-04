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
#include "build_service/admin/controlflow/LuaLoggerWrapper.h"

#include "autil/StringUtil.h"
#include "build_service/admin/controlflow/ControlDefine.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, LuaLoggerWrapper);

#define LOG_PREFIX _prefix.c_str()

void LuaLoggerWrapper::log(const char* level, const char* msg)
{
    string levelStr = level;

    if (levelStr == "ERROR") {
        BS_PREFIX_LOG(ERROR, "[%s] %s", _locationStr.c_str(), msg);
    } else if (levelStr == "INFO") {
        BS_PREFIX_LOG(INFO, "[%s] %s", _locationStr.c_str(), msg);
    } else if (levelStr == "WARN") {
        BS_PREFIX_LOG(WARN, "[%s] %s", _locationStr.c_str(), msg);
    } else if (levelStr == "FATAL") {
        BS_PREFIX_LOG(FATAL, "[%s] %s", _locationStr.c_str(), msg);
    } else if (levelStr == "DEBUG") {
        BS_PREFIX_LOG(DEBUG, "[%s] %s", _locationStr.c_str(), msg);
    } else {
        printf("[%s] [%s] %s\n", _prefix.c_str(), _locationStr.c_str(), msg);
    }
}

void LuaLoggerWrapper::interval_log(const char* level, const char* msg, uint32_t interval)
{
    if (_logCounter == 0) {
        log(level, msg);
        _logCounter = interval;
    }
    if (_logCounter != 0) {
        _logCounter--;
    }
}

void LuaLoggerWrapper::setPrefix(const char* prefix) { _prefix = prefix; }

void LuaLoggerWrapper::setLocation(const char* hostId, const char* fileName, const char* lineStr)
{
    _locationStr = string(hostId) + ":" + string(fileName) + ":" + string(lineStr);
}

void LuaLoggerWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<LuaLoggerWrapper>(state, "LuaLogger", ELuna::constructor<LuaLoggerWrapper>);
    ELuna::registerMethod<LuaLoggerWrapper>(state, "log", &LuaLoggerWrapper::log);
    ELuna::registerMethod<LuaLoggerWrapper>(state, "interval_log", &LuaLoggerWrapper::interval_log);
    ELuna::registerMethod<LuaLoggerWrapper>(state, "setPrefix", &LuaLoggerWrapper::setPrefix);
    ELuna::registerMethod<LuaLoggerWrapper>(state, "setLocation", &LuaLoggerWrapper::setLocation);
}

string LuaLoggerWrapper::rewrite(const string& luaScript)
{
    if (!enableLog(luaScript)) {
        return luaScript;
    }

    auto getLineNum = [](const string& str) {
        size_t pos = 0;
        size_t i = 0;
        while ((pos = str.find("\n", pos)) != string::npos) {
            pos++;
            i++;
        }
        return i;
    };

    string ret;
    string tmp = luaScript;
    size_t lineNum = 1;
    size_t pos = 0;
    while ((pos = tmp.find(CALL_LOG_PREFIX)) != string::npos) {
        string prefix = tmp.substr(0, pos);
        lineNum += getLineNum(prefix);
        tmp = tmp.substr(pos + CALL_LOG_PREFIX.size());
        ret += prefix;
        ret += REPLACE_LOG_PREFIX;
        ret += "\"" + StringUtil::toString(lineNum) + "\", ";
    }
    ret += tmp;

    StringUtil::replaceAll(ret, IMPORT_LOG_STR, LOG_SCRIPT_INTERFACE_STR);
    return ret;
}

#undef LOG_PREFIX

}} // namespace build_service::admin
