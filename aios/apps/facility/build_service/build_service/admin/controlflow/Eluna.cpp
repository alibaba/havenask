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
#include "build_service/admin/controlflow/Eluna.h"

using namespace std;

namespace ELuna {

alog::Logger* _logger = alog::Logger::getLogger("BS.controlflow.Eluna");

void traceStack(lua_State* L, int n)
{
    lua_Debug ar;
    if (lua_getstack(L, n, &ar)) {
        lua_getinfo(L, "Sln", &ar);

        if (ar.name) {
            BS_LOG(ERROR, "\tstack[%d] -> line %d : %s()[%s : line %d]\n", n, ar.currentline, ar.name, ar.short_src,
                   ar.linedefined);
        } else {
            BS_LOG(ERROR, "\tstack[%d] -> line %d : unknown[%s : line %d]\n", n, ar.currentline, ar.short_src,
                   ar.linedefined);
        }

        traceStack(L, n + 1);
    }
}

int error_log(lua_State* L)
{
    BS_LOG(ERROR, "error : %s\n", lua_tostring(L, -1));
    traceStack(L, 0);
    return 0;
}

bool doFile(lua_State* L, const char* fileName)
{
    lua_pushcclosure(L, error_log, 0);
    int stackTop = lua_gettop(L);

    int ret = luaL_loadfile(L, fileName);
    if (ret == 0) {
        if (lua_pcall(L, 0, 0, stackTop)) {
            lua_pop(L, 1);
        }
    } else {
        BS_LOG(ERROR, "dofile error: %s\n", lua_tostring(L, -1));
        lua_pop(L, 1);
    }

    lua_pop(L, 1);
    return ret == 0;
}

bool doString(lua_State* L, const char* str)
{
    lua_pushcclosure(L, error_log, 0);
    int stackTop = lua_gettop(L);

    int ret = luaL_loadstring(L, str);
    if (ret == 0) {
        if (lua_pcall(L, 0, 0, stackTop)) {
            lua_pop(L, 1);
        }
    } else {
        BS_LOG(ERROR, "doString error: %s\n", lua_tostring(L, -1));
        lua_pop(L, 1);
    }

    lua_pop(L, 1);
    return ret == 0;
}

} // namespace ELuna
