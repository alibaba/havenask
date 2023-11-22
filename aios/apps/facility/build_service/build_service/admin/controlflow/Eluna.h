#pragma once
/*////////////////////////////////////////////////////////////////////////////////////
  ELuna - Extended Luna

  ELuna is a simple and light library to bind C/C++ and Lua, which just
  depends on Lua library. It provides some simple API to bind cpp class,
  method, function or to bind lua function, table. You can include ELuna
  and Lua in your project to use.

  Mail: radiotail86@gmail.com

  The MIT License

  Copyright (C) 2012 rick

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
*/////////////////////////////////////////////////////////////////////////////////////

#ifndef _LUA_ELUNA_H_
#define _LUA_ELUNA_H_

#include <assert.h>
#include <map>
#include <string>
#include <vector>

#include "autil/legacy/exception.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

extern "C" {
#include "lauxlib.h"
#include "lua.h"
#include "lualib.h"
}

namespace ELuna {
struct LuaString;
struct LuaTable;
template <typename T>
struct ClassName;

void traceStack(lua_State* L, int n);
int error_log(lua_State* L);

bool doFile(lua_State* L, const char* fileName);
bool doString(lua_State* L, const char* str);

inline lua_State* openLua();
inline void closeLua(lua_State* L);
template <typename T>
inline T read2cpp(lua_State* L, int index);
template <typename T>
inline void push2lua(lua_State* L, T ret);

///////////////////////////////////////////////////////////////////////////////
// lua's string type
///////////////////////////////////////////////////////////////////////////////
struct LuaString {
    const char* str;
    size_t len;
};

///////////////////////////////////////////////////////////////////////////////
// this is a table reference count
///////////////////////////////////////////////////////////////////////////////
struct TableRefCount {
    TableRefCount(lua_State* L) : m_refCount(1) {}
    ~TableRefCount() {};

    inline void incRef() { ++m_refCount; }
    inline int decRef() { return --m_refCount; }

    int m_refCount;
};

///////////////////////////////////////////////////////////////////////////////
// lua's table type
///////////////////////////////////////////////////////////////////////////////
struct LuaTable {
    LuaTable() : m_refCount(NULL), m_tableAdd(NULL), m_luaState(NULL) {};

    LuaTable(lua_State* L)
    {
        lua_newtable(L);
        m_luaState = L;
        m_stackPos = lua_gettop(L);
        m_tableAdd = lua_topointer(L, -1);
        m_refCount = new TableRefCount(L);
    }

    LuaTable(LuaTable const& rhs)
    {
        m_refCount = rhs.m_refCount;
        m_luaState = rhs.m_luaState;
        m_tableAdd = rhs.m_tableAdd;
        m_stackPos = rhs.m_stackPos;

        if (m_refCount)
            m_refCount->incRef();
    }

    LuaTable& operator=(const LuaTable& rhs)
    {
        releaseRef();

        m_refCount = rhs.m_refCount;
        m_luaState = rhs.m_luaState;
        m_tableAdd = rhs.m_tableAdd;
        m_stackPos = rhs.m_stackPos;

        if (m_refCount)
            m_refCount->incRef();

        return *this;
    }

    // for reading table to cpps
    LuaTable(lua_State* L, int index)
    {
        if (index < 0) {
            index = lua_gettop(L) + index + 1;
        }

        if (!lua_istable(L, index)) {
            luaL_argerror(L, index, "table expected");
        }

        m_luaState = L;
        m_stackPos = index;
        m_tableAdd = lua_topointer(L, index);
        m_refCount = new TableRefCount(L);
    }

    // bind Lua Talbe
    LuaTable(lua_State* L, const char* name)
    {
        lua_getglobal(L, name);

        if (lua_istable(L, -1)) {
            m_luaState = L;
            m_stackPos = lua_gettop(L);
            m_tableAdd = lua_topointer(L, -1);
            m_refCount = new TableRefCount(L);
        } else {
            printf("%s is not a lua table!\n", name);
            assert(0);
        }
    }

    ~LuaTable()
    {
        releaseRef();

        m_luaState = NULL;
        m_tableAdd = NULL;
    }

    inline void releaseRef()
    {
        if (m_refCount) {
            if (!m_refCount->decRef()) {
                if (isValid())
                    lua_remove(m_luaState, m_stackPos);

                delete m_refCount;
            }
        }

        m_refCount = NULL;
    }

    inline bool isValid()
    {
        if (m_tableAdd) {
            if (m_tableAdd == lua_topointer(m_luaState, m_stackPos)) {
                return true;
            }
        }

        return false;
    }

    template <typename K, typename V>
    inline void set(K key, V value);

    template <typename K, typename V>
    inline V get(K key);
    template <typename K>
    inline LuaTable get(K key);

    template <typename K>
    inline bool has(K key);

    int m_stackPos = 0;
    TableRefCount* m_refCount = NULL;
    const void* m_tableAdd = NULL;
    lua_State* m_luaState = NULL;
};

template <typename K, typename V>
inline void LuaTable::set(K key, V value)
{
    if (isValid()) {
        push2lua(m_luaState, key);
        push2lua(m_luaState, value);
        lua_settable(m_luaState, m_stackPos);
    }
}

template <typename K, typename V>
inline V LuaTable::get(K key)
{
    if (isValid()) {
        push2lua(m_luaState, key);
        lua_gettable(m_luaState, m_stackPos);
    } else {
        lua_pushnil(m_luaState);
    }

    V result = read2cpp<V>(m_luaState, -1);
    lua_pop(m_luaState, 1);
    return result;
}

template <typename K>
inline LuaTable LuaTable::get(K key)
{
    if (isValid()) {
        push2lua(m_luaState, key);
        lua_gettable(m_luaState, m_stackPos);
    } else {
        lua_pushnil(m_luaState);
    }

    return LuaTable(m_luaState, -1);
}

template <typename K>
inline bool LuaTable::has(K key)
{
    if (isValid()) {
        push2lua(m_luaState, key);
        lua_gettable(m_luaState, m_stackPos);
    } else {
        lua_pushnil(m_luaState);
    }

    bool ret = !lua_isnil(m_luaState, -1);
    lua_pop(m_luaState, 1);
    return ret;
}

template <typename T>
struct UserData {
    UserData(const T* objPtr) : m_objPtr(const_cast<T*>(objPtr)) {};
    virtual ~UserData() {};
    T* m_objPtr;
};

template <typename T>
struct UserGCData : public UserData<T> {
    UserGCData(const T* objPtr) : UserData<T>(objPtr) {};
    ~UserGCData() { delete this->m_objPtr; };
};

template <typename T>
struct remove_const {
    typedef T type;
};

template <typename T>
struct remove_const<const T> {
    typedef T type;
};

template <typename T>
struct remove_const<const T*> {
    typedef T* type;
};

template <typename T>
struct remove_const<const T&> {
    typedef T& type;
};

///////////////////////////////////////////////////////////////////////////////
// read a value from lua to cpp
///////////////////////////////////////////////////////////////////////////////
template <typename T>
struct convert2CppType {
    inline static T convertType(lua_State* L, int index)
    {
        UserGCData<T>* ud = static_cast<UserGCData<T>*>(luaL_checkudata(L, index, ClassName<T>::getName()));
        return *(static_cast<T*>(ud->m_objPtr));
    }
};

template <typename T>
struct convert2CppType<T*> {
    inline static T* convertType(lua_State* L, int index)
    {
        UserData<T>* ud = static_cast<UserData<T>*>(luaL_checkudata(L, index, ClassName<T>::getName()));
        return static_cast<T*>(ud->m_objPtr);
    }
};

template <typename T>
struct convert2CppType<T&> {
    inline static T& convertType(lua_State* L, int index)
    {
        UserData<T>* ud = static_cast<UserData<T>*>(luaL_checkudata(L, index, ClassName<T>::getName()));
        return *(static_cast<T*>(ud->m_objPtr));
    }
};

template <typename T>
inline T read2cpp(lua_State* L, int index)
{
    return convert2CppType<typename remove_const<T>::type>::convertType(L, index);
};

template <>
inline void read2cpp(lua_State* L, int index) {};
template <>
inline bool read2cpp(lua_State* L, int index)
{
    return lua_toboolean(L, index) == 1;
};

#if !defined(LUA_VERSION_NUM) || LUA_VERSION_NUM < 503
template <>
inline char read2cpp(lua_State* L, int index)
{
    return (char)luaL_checknumber(L, index);
};
template <>
inline unsigned char read2cpp(lua_State* L, int index)
{
    return (unsigned char)luaL_checknumber(L, index);
};
template <>
inline short read2cpp(lua_State* L, int index)
{
    return (short)luaL_checknumber(L, index);
};
template <>
inline unsigned short read2cpp(lua_State* L, int index)
{
    return (unsigned short)luaL_checknumber(L, index);
};
template <>
inline int read2cpp(lua_State* L, int index)
{
    return (int)luaL_checknumber(L, index);
};
template <>
inline unsigned int read2cpp(lua_State* L, int index)
{
    return (unsigned int)luaL_checknumber(L, index);
};
template <>
inline long read2cpp(lua_State* L, int index)
{
    return (long)luaL_checknumber(L, index);
};
template <>
inline unsigned long read2cpp(lua_State* L, int index)
{
    return (unsigned long)luaL_checknumber(L, index);
};
template <>
inline long long read2cpp(lua_State* L, int index)
{
    return (long long)luaL_checknumber(L, index);
};
template <>
inline unsigned long long read2cpp(lua_State* L, int index)
{
    return (unsigned long long)luaL_checknumber(L, index);
};
#else
template <>
inline char read2cpp(lua_State* L, int index)
{
    return (char)luaL_checkinteger(L, index);
};
template <>
inline unsigned char read2cpp(lua_State* L, int index)
{
    return (unsigned char)luaL_checkinteger(L, index);
};
template <>
inline short read2cpp(lua_State* L, int index)
{
    return (short)luaL_checkinteger(L, index);
};
template <>
inline unsigned short read2cpp(lua_State* L, int index)
{
    return (unsigned short)luaL_checkinteger(L, index);
};
template <>
inline int read2cpp(lua_State* L, int index)
{
    return (int)luaL_checkinteger(L, index);
};
template <>
inline unsigned int read2cpp(lua_State* L, int index)
{
    return (unsigned int)luaL_checkinteger(L, index);
};
template <>
inline long read2cpp(lua_State* L, int index)
{
    return (long)luaL_checkinteger(L, index);
};
template <>
inline unsigned long read2cpp(lua_State* L, int index)
{
    return (unsigned long)luaL_checkinteger(L, index);
};
template <>
inline long long read2cpp(lua_State* L, int index)
{
    return (long long)luaL_checkinteger(L, index);
};
template <>
inline unsigned long long read2cpp(lua_State* L, int index)
{
    return (unsigned long long)luaL_checkinteger(L, index);
};
#endif

template <>
inline float read2cpp(lua_State* L, int index)
{
    return (float)luaL_checknumber(L, index);
};
template <>
inline double read2cpp(lua_State* L, int index)
{
    return (double)luaL_checknumber(L, index);
};
template <>
inline char* read2cpp(lua_State* L, int index)
{
    return (char*)luaL_checkstring(L, index);
};
template <>
inline const char* read2cpp(lua_State* L, int index)
{
    return (const char*)luaL_checkstring(L, index);
};
template <>
inline std::string read2cpp(lua_State* L, int index)
{
    std::string str(luaL_checkstring(L, index));
    return str;
};
template <>
inline LuaString read2cpp(lua_State* L, int index)
{
    LuaString ls;
    ls.str = (char*)luaL_checklstring(L, index, &ls.len);
    return ls;
};
template <>
inline LuaTable read2cpp(lua_State* L, int index)
{
    return LuaTable(L, index);
};

///////////////////////////////////////////////////////////////////////////////
// push a value from cpp to lua
///////////////////////////////////////////////////////////////////////////////
template <typename T>
struct convert2LuaType {
    inline static void convertType(lua_State* L, const T& ret)
    {
        UserGCData<T>* ud = static_cast<UserGCData<T>*>(lua_newuserdata(L, sizeof(UserGCData<T>)));
        new (ud) UserGCData<T>(new T(ret));

        luaL_getmetatable(L, ClassName<T>::getName());
        lua_setmetatable(L, -2);
    }
};

template <typename T>
struct convert2LuaType<T*> {
    inline static void convertType(lua_State* L, const T* ret)
    {
        if (!ret) {
            lua_pushnil(L);
            return;
        }

        UserData<T>* ud = static_cast<UserData<T>*>(lua_newuserdata(L, sizeof(UserData<T>)));
        new (ud) UserData<T>(ret);

        luaL_getmetatable(L, ClassName<T>::getName());
        lua_setmetatable(L, -2);
    }
};

template <typename T>
struct convert2LuaType<T&> {
    inline static void convertType(lua_State* L, const T& ret)
    {
        UserData<T>* ud = static_cast<UserData<T>*>(lua_newuserdata(L, sizeof(UserData<T>)));
        new (ud) UserData<T>(&ret);

        luaL_getmetatable(L, ClassName<T>::getName());
        lua_setmetatable(L, -2);
    }
};

template <typename T>
inline void push2lua(lua_State* L, T ret)
{
    convert2LuaType<typename remove_const<T>::type>::convertType(L, ret);
};

template <>
inline void push2lua(lua_State* L, bool ret)
{
    lua_pushboolean(L, ret);
};

#if !defined(LUA_VERSION_NUM) || LUA_VERSION_NUM < 503
template <>
inline void push2lua(lua_State* L, char ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned char ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, short ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned short ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, int ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned int ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, long ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned long ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, long long ret)
{
    lua_pushnumber(L, (lua_Number)ret);
};
template <>
inline void push2lua(lua_State* L, unsigned long long ret)
{
    lua_pushnumber(L, (lua_Number)ret);
};
#else
template <>
inline void push2lua(lua_State* L, char ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned char ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, short ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned short ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, int ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned int ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, long ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned long ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, long long ret)
{
    lua_pushinteger(L, ret);
};
template <>
inline void push2lua(lua_State* L, unsigned long long ret)
{
    lua_pushinteger(L, ret);
};
#endif

template <>
inline void push2lua(lua_State* L, float ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, double ret)
{
    lua_pushnumber(L, ret);
};
template <>
inline void push2lua(lua_State* L, char* ret)
{
    lua_pushstring(L, ret);
};
template <>
inline void push2lua(lua_State* L, const char* ret)
{
    lua_pushstring(L, ret);
};
template <>
inline void push2lua(lua_State* L, std::string ret)
{
    lua_pushstring(L, ret.c_str());
};
template <>
inline void push2lua(lua_State* L, LuaString ret)
{
    lua_pushlstring(L, ret.str, ret.len);
};
template <>
inline void push2lua(lua_State* L, LuaTable ret)
{
    if (ret.m_refCount)
        lua_pushvalue(L, ret.m_stackPos);
    else
        lua_pushnil(L);
};

///////////////////////////////////////////////////////////////////////////////
// define a proxy class for method and fuction
///////////////////////////////////////////////////////////////////////////////
// this is a base method class.
struct GenericMethod {
    virtual ~GenericMethod() {};
    GenericMethod(const char* name) : m_name(name) {};

    inline virtual int call(lua_State* L) { return 0; };

    const char* m_name;
};

// this is a base function class.
struct GenericFunction {
    virtual ~GenericFunction() {};
    GenericFunction(const char* name) : m_name(name) {};

    inline virtual int call(lua_State* L) { return 0; };

    const char* m_name;
};

///////////////////////////////////////////////////////////////////////////////
// bind cpp method
///////////////////////////////////////////////////////////////////////////////
#define ELUNA_METHODCLASSES_PARAM_LIST_0 typename T
#define ELUNA_METHODCLASSES_PARAM_LIST_1 ELUNA_METHODCLASSES_PARAM_LIST_0, typename P1
#define ELUNA_METHODCLASSES_PARAM_LIST_2 ELUNA_METHODCLASSES_PARAM_LIST_1, typename P2
#define ELUNA_METHODCLASSES_PARAM_LIST_3 ELUNA_METHODCLASSES_PARAM_LIST_2, typename P3
#define ELUNA_METHODCLASSES_PARAM_LIST_4 ELUNA_METHODCLASSES_PARAM_LIST_3, typename P4
#define ELUNA_METHODCLASSES_PARAM_LIST_5 ELUNA_METHODCLASSES_PARAM_LIST_4, typename P5
#define ELUNA_METHODCLASSES_PARAM_LIST_6 ELUNA_METHODCLASSES_PARAM_LIST_5, typename P6
#define ELUNA_METHODCLASSES_PARAM_LIST_7 ELUNA_METHODCLASSES_PARAM_LIST_6, typename P7
#define ELUNA_METHODCLASSES_PARAM_LIST_8 ELUNA_METHODCLASSES_PARAM_LIST_7, typename P8
#define ELUNA_METHODCLASSES_PARAM_LIST_9 ELUNA_METHODCLASSES_PARAM_LIST_8, typename P9
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_0 T
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_1 ELUNA_METHODCLASSES_SP_PARAM_LIST_0, P1
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_2 ELUNA_METHODCLASSES_SP_PARAM_LIST_1, P2
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_3 ELUNA_METHODCLASSES_SP_PARAM_LIST_2, P3
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_4 ELUNA_METHODCLASSES_SP_PARAM_LIST_3, P4
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_5 ELUNA_METHODCLASSES_SP_PARAM_LIST_4, P5
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_6 ELUNA_METHODCLASSES_SP_PARAM_LIST_5, P6
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_7 ELUNA_METHODCLASSES_SP_PARAM_LIST_6, P7
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_8 ELUNA_METHODCLASSES_SP_PARAM_LIST_7, P8
#define ELUNA_METHODCLASSES_SP_PARAM_LIST_9 ELUNA_METHODCLASSES_SP_PARAM_LIST_8, P9
#define ELUNA_PARAM_LIST_0
#define ELUNA_PARAM_LIST_1 ELUNA_PARAM_LIST_0 P1
#define ELUNA_PARAM_LIST_2 ELUNA_PARAM_LIST_1, P2
#define ELUNA_PARAM_LIST_3 ELUNA_PARAM_LIST_2, P3
#define ELUNA_PARAM_LIST_4 ELUNA_PARAM_LIST_3, P4
#define ELUNA_PARAM_LIST_5 ELUNA_PARAM_LIST_4, P5
#define ELUNA_PARAM_LIST_6 ELUNA_PARAM_LIST_5, P6
#define ELUNA_PARAM_LIST_7 ELUNA_PARAM_LIST_6, P7
#define ELUNA_PARAM_LIST_8 ELUNA_PARAM_LIST_7, P8
#define ELUNA_PARAM_LIST_9 ELUNA_PARAM_LIST_8, P9
#define ELUNA_READ_METHOD_PARAM_LIST_0
#define ELUNA_READ_METHOD_PARAM_LIST_1 ELUNA_READ_METHOD_PARAM_LIST_0 read2cpp<P1>(L, 2)
#define ELUNA_READ_METHOD_PARAM_LIST_2 ELUNA_READ_METHOD_PARAM_LIST_1, read2cpp<P2>(L, 3)
#define ELUNA_READ_METHOD_PARAM_LIST_3 ELUNA_READ_METHOD_PARAM_LIST_2, read2cpp<P3>(L, 4)
#define ELUNA_READ_METHOD_PARAM_LIST_4 ELUNA_READ_METHOD_PARAM_LIST_3, read2cpp<P4>(L, 5)
#define ELUNA_READ_METHOD_PARAM_LIST_5 ELUNA_READ_METHOD_PARAM_LIST_4, read2cpp<P5>(L, 6)
#define ELUNA_READ_METHOD_PARAM_LIST_6 ELUNA_READ_METHOD_PARAM_LIST_5, read2cpp<P6>(L, 7)
#define ELUNA_READ_METHOD_PARAM_LIST_7 ELUNA_READ_METHOD_PARAM_LIST_6, read2cpp<P7>(L, 8)
#define ELUNA_READ_METHOD_PARAM_LIST_8 ELUNA_READ_METHOD_PARAM_LIST_7, read2cpp<P8>(L, 9)
#define ELUNA_READ_METHOD_PARAM_LIST_9 ELUNA_READ_METHOD_PARAM_LIST_8, read2cpp<P9>(L, 10)

#define ELUNA_MAKE_METHODCLASSX(N)                                                                                     \
    template <typename RL, ELUNA_METHODCLASSES_PARAM_LIST_##N>                                                         \
    struct MethodClass##N : GenericMethod {                                                                            \
        typedef RL (T::*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                  \
        TFUNC m_func;                                                                                                  \
        MethodClass##N(const char* name, TFUNC func) : GenericMethod(name), m_func(func) {};                           \
        ~MethodClass##N() {};                                                                                          \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            T* obj = read2cpp<T*>(L, 1);                                                                               \
            push2lua(L, (obj->*m_func)(ELUNA_READ_METHOD_PARAM_LIST_##N));                                             \
            return 1;                                                                                                  \
        };                                                                                                             \
    };

#define ELUNA_MAKE_REF_RL_METHODCLASSX(N)                                                                              \
    template <typename RL, ELUNA_METHODCLASSES_PARAM_LIST_##N>                                                         \
    struct MethodClass##N<RL&, ELUNA_METHODCLASSES_SP_PARAM_LIST_##N> : GenericMethod {                                \
        typedef RL& (T::*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                 \
        TFUNC m_func;                                                                                                  \
        MethodClass##N(const char* name, TFUNC func) : GenericMethod(name), m_func(func) {};                           \
        ~MethodClass##N() {};                                                                                          \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            T* obj = read2cpp<T*>(L, 1);                                                                               \
            push2lua<RL&>(L, (obj->*m_func)(ELUNA_READ_METHOD_PARAM_LIST_##N));                                        \
            return 1;                                                                                                  \
        };                                                                                                             \
    };

#define ELUNA_MAKE_VOID_RL_METHODCLASSX(N)                                                                             \
    template <ELUNA_METHODCLASSES_PARAM_LIST_##N>                                                                      \
    struct MethodClass##N<void, ELUNA_METHODCLASSES_SP_PARAM_LIST_##N> : GenericMethod {                               \
        typedef void (T::*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                \
        TFUNC m_func;                                                                                                  \
        MethodClass##N(const char* name, TFUNC func) : GenericMethod(name), m_func(func) {};                           \
        ~MethodClass##N() {};                                                                                          \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            T* obj = read2cpp<T*>(L, 1);                                                                               \
            (obj->*m_func)(ELUNA_READ_METHOD_PARAM_LIST_##N);                                                          \
            return 0;                                                                                                  \
        };                                                                                                             \
    };

ELUNA_MAKE_METHODCLASSX(0)
ELUNA_MAKE_METHODCLASSX(1)
ELUNA_MAKE_METHODCLASSX(2)
ELUNA_MAKE_METHODCLASSX(3)
ELUNA_MAKE_METHODCLASSX(4)
ELUNA_MAKE_METHODCLASSX(5)
ELUNA_MAKE_METHODCLASSX(6)
ELUNA_MAKE_METHODCLASSX(7)
ELUNA_MAKE_METHODCLASSX(8)
ELUNA_MAKE_METHODCLASSX(9)

ELUNA_MAKE_REF_RL_METHODCLASSX(0)
ELUNA_MAKE_REF_RL_METHODCLASSX(1)
ELUNA_MAKE_REF_RL_METHODCLASSX(2)
ELUNA_MAKE_REF_RL_METHODCLASSX(3)
ELUNA_MAKE_REF_RL_METHODCLASSX(4)
ELUNA_MAKE_REF_RL_METHODCLASSX(5)
ELUNA_MAKE_REF_RL_METHODCLASSX(6)
ELUNA_MAKE_REF_RL_METHODCLASSX(7)
ELUNA_MAKE_REF_RL_METHODCLASSX(8)
ELUNA_MAKE_REF_RL_METHODCLASSX(9)

ELUNA_MAKE_VOID_RL_METHODCLASSX(0)
ELUNA_MAKE_VOID_RL_METHODCLASSX(1)
ELUNA_MAKE_VOID_RL_METHODCLASSX(2)
ELUNA_MAKE_VOID_RL_METHODCLASSX(3)
ELUNA_MAKE_VOID_RL_METHODCLASSX(4)
ELUNA_MAKE_VOID_RL_METHODCLASSX(5)
ELUNA_MAKE_VOID_RL_METHODCLASSX(6)
ELUNA_MAKE_VOID_RL_METHODCLASSX(7)
ELUNA_MAKE_VOID_RL_METHODCLASSX(8)
ELUNA_MAKE_VOID_RL_METHODCLASSX(9)

template <typename T>
struct ClassName {
    static inline void setName(const char* name) { m_name = name; }
    static inline const char* getName() { return m_name; }

    static const char* m_name;
};
template <typename T>
const char* ClassName<T>::m_name;

template <typename T>
inline int gc_obj(lua_State* L)
{
    // clean up
    UserData<T>* ud = static_cast<UserData<T>*>(luaL_checkudata(L, -1, ClassName<T>::getName()));
    ud->~UserData();
    return 0;
}

template <typename T>
inline void registerMetatable(lua_State* L, const char* name)
{
    luaL_newmetatable(L, name); // create a metatable in the registry

    lua_pushstring(L, "__index"); // push metamethods's name
    lua_pushvalue(L, -2);         // push matatable
    lua_settable(L, -3);          // metatable.__index = metatable

    lua_pushstring(L, "__newindex");
    lua_pushvalue(L, -2);
    lua_settable(L, -3); // metatable.__newindex = metatable

    lua_pushstring(L, "__gc");
    lua_pushcfunction(L, &gc_obj<T>);
    lua_rawset(L, -3); // metatable.__gc = Luna<T>::gc_obj

    lua_pop(L, 1);
}

template <typename T, typename F>
inline void registerClass(lua_State* L, const char* name, F constructor)
{
    ClassName<T>::setName(name);

    lua_pushcfunction(L, constructor);
    lua_setglobal(L, name);

    registerMetatable<T>(L, name);
}

template <typename T>
inline int inject(lua_State* L, T* objPtr)
{
    UserGCData<T>* ud = static_cast<UserGCData<T>*>(lua_newuserdata(L, sizeof(UserGCData<T>)));
    new (ud) UserGCData<T>(objPtr);

    luaL_getmetatable(L, ClassName<T>::getName());
    lua_setmetatable(L, -2); // self.metatable = uniqe_metatable

    return 1;
}

template <typename T>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T());
}

template <typename T, typename P1>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1)));
}

template <typename T, typename P1, typename P2>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2)));
}

template <typename T, typename P1, typename P2, typename P3>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4, typename P5>
inline int constructor(lua_State* L)
{
    return inject<T>(
        L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4), read2cpp<P5>(L, 5)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4),
                              read2cpp<P5>(L, 5), read2cpp<P6>(L, 6)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4),
                              read2cpp<P5>(L, 5), read2cpp<P6>(L, 6), read2cpp<P7>(L, 7)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7,
          typename P8>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4),
                              read2cpp<P5>(L, 5), read2cpp<P6>(L, 6), read2cpp<P7>(L, 7), read2cpp<P8>(L, 8)));
}

template <typename T, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7,
          typename P8, typename P9>
inline int constructor(lua_State* L)
{
    return inject<T>(L, new T(read2cpp<P1>(L, 1), read2cpp<P2>(L, 2), read2cpp<P3>(L, 3), read2cpp<P4>(L, 4),
                              read2cpp<P5>(L, 5), read2cpp<P6>(L, 6), read2cpp<P7>(L, 7), read2cpp<P8>(L, 8),
                              read2cpp<P9>(L, 9)));
}

inline int proxyMethodCall(lua_State* L)
{
    GenericMethod* pMethod = static_cast<GenericMethod*>(lua_touserdata(L, lua_upvalueindex(1)));
    return pMethod->call(L); // execute method
}

template <typename T, typename RL>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)())
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass0<RL, T>))) MethodClass0<RL, T>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass1<RL, T, P1>))) MethodClass1<RL, T, P1>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass2<RL, T, P1, P2>))) MethodClass2<RL, T, P1, P2>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass3<RL, T, P1, P2, P3>))) MethodClass3<RL, T, P1, P2, P3>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass4<RL, T, P1, P2, P3, P4>)))
            MethodClass4<RL, T, P1, P2, P3, P4>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4, typename P5>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4, P5))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass5<RL, T, P1, P2, P3, P4, P5>)))
            MethodClass5<RL, T, P1, P2, P3, P4, P5>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4, P5, P6))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass6<RL, T, P1, P2, P3, P4, P5, P6>)))
            MethodClass6<RL, T, P1, P2, P3, P4, P5, P6>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6,
          typename P7>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4, P5, P6, P7))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass7<RL, T, P1, P2, P3, P4, P5, P6, P7>)))
            MethodClass7<RL, T, P1, P2, P3, P4, P5, P6, P7>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6,
          typename P7, typename P8>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4, P5, P6, P7, P8))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass8<RL, T, P1, P2, P3, P4, P5, P6, P7, P8>)))
            MethodClass8<RL, T, P1, P2, P3, P4, P5, P6, P7, P8>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

template <typename T, typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6,
          typename P7, typename P8, typename P9>
inline void registerMethod(lua_State* L, const char* name, RL (T::*func)(P1, P2, P3, P4, P5, P6, P7, P8, P9))
{
    luaL_getmetatable(L, ClassName<T>::getName());

    if (lua_istable(L, -1)) {
        lua_pushstring(L, name);
        new (lua_newuserdata(L, sizeof(MethodClass9<RL, T, P1, P2, P3, P4, P5, P6, P7, P8, P9>)))
            MethodClass9<RL, T, P1, P2, P3, P4, P5, P6, P7, P8, P9>(name, func);
        lua_pushcclosure(L, &proxyMethodCall, 1);
        lua_rawset(L, -3);
    } else {
        printf("please register class %s\n", ClassName<T>::getName());
    }
    lua_pop(L, 1);
}

///////////////////////////////////////////////////////////////////////////////
// bind cpp function.
///////////////////////////////////////////////////////////////////////////////
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_0 typename RL
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_1 ELUNA_FUNCTIONCLASSX_PARAM_LIST_0, typename P1
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_2 ELUNA_FUNCTIONCLASSX_PARAM_LIST_1, typename P2
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_3 ELUNA_FUNCTIONCLASSX_PARAM_LIST_2, typename P3
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_4 ELUNA_FUNCTIONCLASSX_PARAM_LIST_3, typename P4
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_5 ELUNA_FUNCTIONCLASSX_PARAM_LIST_4, typename P5
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_6 ELUNA_FUNCTIONCLASSX_PARAM_LIST_5, typename P6
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_7 ELUNA_FUNCTIONCLASSX_PARAM_LIST_6, typename P7
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_8 ELUNA_FUNCTIONCLASSX_PARAM_LIST_7, typename P8
#define ELUNA_FUNCTIONCLASSX_PARAM_LIST_9 ELUNA_FUNCTIONCLASSX_PARAM_LIST_8, typename P9
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_0
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_1 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_0 typename P1
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_2 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_1, typename P2
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_3 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_2, typename P3
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_4 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_3, typename P4
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_5 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_4, typename P5
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_6 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_5, typename P6
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_7 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_6, typename P7
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_8 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_7, typename P8
#define ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_9 ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_8, typename P9
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_0 RL&
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_1 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_0, P1
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_2 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_1, P2
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_3 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_2, P3
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_4 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_3, P4
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_5 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_4, P5
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_6 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_5, P6
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_7 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_6, P7
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_8 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_7, P8
#define ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_9 ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_8, P9
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_0 void
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_1 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_0, P1
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_2 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_1, P2
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_3 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_2, P3
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_4 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_3, P4
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_5 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_4, P5
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_6 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_5, P6
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_7 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_6, P7
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_8 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_7, P8
#define ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_9 ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_8, P9
#define ELUNA_READ_FUNCTION_PARAM_LIST_0
#define ELUNA_READ_FUNCTION_PARAM_LIST_1 ELUNA_READ_FUNCTION_PARAM_LIST_0 read2cpp<P1>(L, 1)
#define ELUNA_READ_FUNCTION_PARAM_LIST_2 ELUNA_READ_FUNCTION_PARAM_LIST_1, read2cpp<P2>(L, 2)
#define ELUNA_READ_FUNCTION_PARAM_LIST_3 ELUNA_READ_FUNCTION_PARAM_LIST_2, read2cpp<P3>(L, 3)
#define ELUNA_READ_FUNCTION_PARAM_LIST_4 ELUNA_READ_FUNCTION_PARAM_LIST_3, read2cpp<P4>(L, 4)
#define ELUNA_READ_FUNCTION_PARAM_LIST_5 ELUNA_READ_FUNCTION_PARAM_LIST_4, read2cpp<P5>(L, 5)
#define ELUNA_READ_FUNCTION_PARAM_LIST_6 ELUNA_READ_FUNCTION_PARAM_LIST_5, read2cpp<P6>(L, 6)
#define ELUNA_READ_FUNCTION_PARAM_LIST_7 ELUNA_READ_FUNCTION_PARAM_LIST_6, read2cpp<P7>(L, 7)
#define ELUNA_READ_FUNCTION_PARAM_LIST_8 ELUNA_READ_FUNCTION_PARAM_LIST_7, read2cpp<P8>(L, 8)
#define ELUNA_READ_FUNCTION_PARAM_LIST_9 ELUNA_READ_FUNCTION_PARAM_LIST_8, read2cpp<P9>(L, 9)

#define ELUNA_MAKE_FUNCTIONCLASSX(N)                                                                                   \
    template <ELUNA_FUNCTIONCLASSX_PARAM_LIST_##N>                                                                     \
    struct FunctionClass##N : GenericFunction {                                                                        \
        typedef RL (*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                     \
        TFUNC m_func;                                                                                                  \
        FunctionClass##N(const char* name, TFUNC func) : GenericFunction(name), m_func(func) {};                       \
        ~FunctionClass##N() {};                                                                                        \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            push2lua(L, (*m_func)(ELUNA_READ_FUNCTION_PARAM_LIST_##N));                                                \
            return 1;                                                                                                  \
        };                                                                                                             \
    };

#define ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(N)                                                                            \
    template <ELUNA_FUNCTIONCLASSX_PARAM_LIST_##N>                                                                     \
    struct FunctionClass##N<ELUNA_FUNCTIONCLASSX_SP_REF_RL_PARAM_LIST_##N> : GenericFunction {                         \
        typedef RL& (*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                    \
        TFUNC m_func;                                                                                                  \
        FunctionClass##N(const char* name, TFUNC func) : GenericFunction(name), m_func(func) {};                       \
        ~FunctionClass##N() {};                                                                                        \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            push2lua<RL&>(L, (*m_func)(ELUNA_READ_FUNCTION_PARAM_LIST_##N));                                           \
            return 1;                                                                                                  \
        };                                                                                                             \
    };

#define ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(N)                                                                           \
    template <ELUNA_FUNCTIONCLASSX_VOID_RL_PARAM_LIST_##N>                                                             \
    struct FunctionClass##N<ELUNA_FUNCTIONCLASSX_SP_VOID_RL_PARAM_LIST_##N> : GenericFunction {                        \
        typedef void (*TFUNC)(ELUNA_PARAM_LIST_##N);                                                                   \
        TFUNC m_func;                                                                                                  \
        FunctionClass##N(const char* name, TFUNC func) : GenericFunction(name), m_func(func) {};                       \
        ~FunctionClass##N() {};                                                                                        \
        inline virtual int call(lua_State* L)                                                                          \
        {                                                                                                              \
            (*m_func)(ELUNA_READ_FUNCTION_PARAM_LIST_##N);                                                             \
            return 0;                                                                                                  \
        };                                                                                                             \
    };

ELUNA_MAKE_FUNCTIONCLASSX(0)
ELUNA_MAKE_FUNCTIONCLASSX(1)
ELUNA_MAKE_FUNCTIONCLASSX(2)
ELUNA_MAKE_FUNCTIONCLASSX(3)
ELUNA_MAKE_FUNCTIONCLASSX(4)
ELUNA_MAKE_FUNCTIONCLASSX(5)
ELUNA_MAKE_FUNCTIONCLASSX(6)
ELUNA_MAKE_FUNCTIONCLASSX(7)
ELUNA_MAKE_FUNCTIONCLASSX(8)
ELUNA_MAKE_FUNCTIONCLASSX(9)

ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(0)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(1)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(2)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(3)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(4)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(5)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(6)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(7)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(8)
ELUNA_MAKE_REF_RL_FUNCTIONCLASSX(9)

ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(0)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(1)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(2)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(3)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(4)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(5)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(6)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(7)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(8)
ELUNA_MAKE_VOID_RL_FUNCTIONCLASSX(9)

inline int proxyFunctionCall(lua_State* L)
{
    GenericFunction* pFunction =
        static_cast<GenericFunction*>(lua_touserdata(L, lua_upvalueindex(1))); // get functionClass pointer
    return pFunction->call(L);                                                 // execute function
}

template <typename RL>
inline void registerFunction(lua_State* L, const char* name, RL (*func)())
{
    new (lua_newuserdata(L, sizeof(FunctionClass0<RL>))) FunctionClass0<RL>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1))
{
    new (lua_newuserdata(L, sizeof(FunctionClass1<RL, P1>))) FunctionClass1<RL, P1>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2))
{
    new (lua_newuserdata(L, sizeof(FunctionClass2<RL, P1, P2>))) FunctionClass2<RL, P1, P2>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3))
{
    new (lua_newuserdata(L, sizeof(FunctionClass3<RL, P1, P2, P3>))) FunctionClass3<RL, P1, P2, P3>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4))
{
    new (lua_newuserdata(L, sizeof(FunctionClass4<RL, P1, P2, P3, P4>))) FunctionClass4<RL, P1, P2, P3, P4>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4, typename P5>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4, P5))
{
    new (lua_newuserdata(L, sizeof(FunctionClass5<RL, P1, P2, P3, P4, P5>)))
        FunctionClass5<RL, P1, P2, P3, P4, P5>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4, P5, P6))
{
    new (lua_newuserdata(L, sizeof(FunctionClass6<RL, P1, P2, P3, P4, P5, P6>)))
        FunctionClass6<RL, P1, P2, P3, P4, P5, P6>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4, P5, P6, P7))
{
    new (lua_newuserdata(L, sizeof(FunctionClass7<RL, P1, P2, P3, P4, P5, P6, P7>)))
        FunctionClass7<RL, P1, P2, P3, P4, P5, P6, P7>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7,
          typename P8>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4, P5, P6, P7, P8))
{
    new (lua_newuserdata(L, sizeof(FunctionClass8<RL, P1, P2, P3, P4, P5, P6, P7, P8>)))
        FunctionClass8<RL, P1, P2, P3, P4, P5, P6, P7, P8>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

template <typename RL, typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7,
          typename P8, typename P9>
inline void registerFunction(lua_State* L, const char* name, RL (*func)(P1, P2, P3, P4, P5, P6, P7, P8, P9))
{
    new (lua_newuserdata(L, sizeof(FunctionClass9<RL, P1, P2, P3, P4, P5, P6, P7, P8, P9>)))
        FunctionClass9<RL, P1, P2, P3, P4, P5, P6, P7, P8, P9>(name, func);
    lua_pushcclosure(L, proxyFunctionCall, 1);
    lua_setglobal(L, name);
}

///////////////////////////////////////////////////////////////////////////////
// bind lua function
///////////////////////////////////////////////////////////////////////////////
template <typename RL>
class LuaFunction
{
public:
    ~LuaFunction()
    {
        luaL_unref(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_luaState = NULL;
    }

    LuaFunction(lua_State* L, const char* funcName) : m_luaState(L)
    {
        lua_getglobal(L, funcName);

        if (lua_isfunction(L, -1)) {
            m_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        } else {
            printf("%s is not a lua function!\n", funcName);
            assert(0);
        }
    };

    RL operator()()
    {
        lua_pushcclosure(m_luaState, error_log, 0); // stack top +1
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_pcall = lua_pcall(m_luaState, 0, 1, stackTop); // stack top +1
        if (hasError()) {
            lua_settop(m_luaState, -3); // stack top -2, recover stack top
            return RL();
        }
        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3); // stack top -2, recover stack top
        return result;
    }

    template <typename P1>
    RL operator()(P1 p1)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        m_pcall = lua_pcall(m_luaState, 1, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }
        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2>
    RL operator()(P1 p1, P2 p2)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        m_pcall = lua_pcall(m_luaState, 2, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }
        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3>
    RL operator()(P1 p1, P2 p2, P3 p3)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        m_pcall = lua_pcall(m_luaState, 3, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }
        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        m_pcall = lua_pcall(m_luaState, 4, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }
        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        m_pcall = lua_pcall(m_luaState, 5, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }

        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        m_pcall = lua_pcall(m_luaState, 6, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }

        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        m_pcall = lua_pcall(m_luaState, 7, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }

        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        m_pcall = lua_pcall(m_luaState, 8, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }

        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8,
              typename P9>
    RL operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        push2lua(m_luaState, p9);
        m_pcall = lua_pcall(m_luaState, 9, 1, stackTop);
        if (hasError()) {
            lua_settop(m_luaState, -3);
            return RL();
        }

        RL result = read2cpp<RL>(m_luaState, -1);
        lua_settop(m_luaState, -3);
        return result;
    }

    bool hasError() { return m_pcall != 0; }

private:
    int m_ref = 0;
    int m_pcall = 0;
    // const char* m_name;
    lua_State* m_luaState = 0;
};

template <>
class LuaFunction<LuaTable>
{
public:
    ~LuaFunction()
    {
        luaL_unref(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_luaState = NULL;
    }

    LuaFunction(lua_State* L, const char* funcName) : m_luaState(L)
    {
        lua_getglobal(L, funcName);

        if (lua_isfunction(L, -1)) {
            m_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        } else {
            printf("%s is not a lua function!\n", funcName);
            assert(0);
        }
    };

    LuaTable operator()()
    {
        lua_pushcclosure(m_luaState, error_log, 0); // stack top +1
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_pcall = lua_pcall(m_luaState, 0, 1, stackTop); // stack top +1

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1>
    LuaTable operator()(P1 p1)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        m_pcall = lua_pcall(m_luaState, 1, 1, stackTop);

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2>
    LuaTable operator()(P1 p1, P2 p2)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        m_pcall = lua_pcall(m_luaState, 2, 1, stackTop);
        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3>
    LuaTable operator()(P1 p1, P2 p2, P3 p3)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        m_pcall = lua_pcall(m_luaState, 3, 1, stackTop);
        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        m_pcall = lua_pcall(m_luaState, 4, 1, stackTop);
        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        m_pcall = lua_pcall(m_luaState, 5, 1, stackTop);
        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        m_pcall = lua_pcall(m_luaState, 6, 1, stackTop);

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        m_pcall = lua_pcall(m_luaState, 7, 1, stackTop);

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        m_pcall = lua_pcall(m_luaState, 8, 1, stackTop);

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8,
              typename P9>
    LuaTable operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        push2lua(m_luaState, p9);
        m_pcall = lua_pcall(m_luaState, 9, 1, stackTop);

        lua_remove(m_luaState, -2);
        return LuaTable(m_luaState, -1);
    }

    bool hasError() { return m_pcall != 0; }

private:
    int m_ref = 0;
    int m_pcall = 0;
    // const char* m_name;
    lua_State* m_luaState = nullptr;
};

template <>
class LuaFunction<void>
{
public:
    LuaFunction(lua_State* L, const char* funcName) : m_luaState(L)
    {
        lua_getglobal(L, funcName);

        if (lua_isfunction(L, -1)) {
            m_ref = luaL_ref(L, LUA_REGISTRYINDEX);
        } else {
            printf("%s is not a lua function!\n", funcName);
            assert(0);
        }
    };

    ~LuaFunction()
    {
        luaL_unref(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_luaState = NULL;
    };

    void operator()()
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        m_pcall = lua_pcall(m_luaState, 0, 1, stackTop);
        lua_settop(m_luaState, -3);
    }

    template <typename P1>
    void operator()(P1 p1)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        m_pcall = lua_pcall(m_luaState, 1, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2>
    void operator()(P1 p1, P2 p2)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        m_pcall = lua_pcall(m_luaState, 2, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3>
    void operator()(P1 p1, P2 p2, P3 p3)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        m_pcall = lua_pcall(m_luaState, 3, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        m_pcall = lua_pcall(m_luaState, 4, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        m_pcall = lua_pcall(m_luaState, 5, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        m_pcall = lua_pcall(m_luaState, 6, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        m_pcall = lua_pcall(m_luaState, 7, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        m_pcall = lua_pcall(m_luaState, 8, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    template <typename P1, typename P2, typename P3, typename P4, typename P5, typename P6, typename P7, typename P8,
              typename P9>
    void operator()(P1 p1, P2 p2, P3 p3, P4 p4, P5 p5, P6 p6, P7 p7, P8 p8, P9 p9)
    {
        lua_pushcclosure(m_luaState, error_log, 0);
        int stackTop = lua_gettop(m_luaState);

        lua_rawgeti(m_luaState, LUA_REGISTRYINDEX, m_ref);
        push2lua(m_luaState, p1);
        push2lua(m_luaState, p2);
        push2lua(m_luaState, p3);
        push2lua(m_luaState, p4);
        push2lua(m_luaState, p5);
        push2lua(m_luaState, p6);
        push2lua(m_luaState, p7);
        push2lua(m_luaState, p8);
        push2lua(m_luaState, p9);
        m_pcall = lua_pcall(m_luaState, 9, 1, stackTop);

        lua_settop(m_luaState, -3);
    }

    bool hasError() { return m_pcall != 0; }

private:
    int m_ref = 0;
    int m_pcall = 0;
    // const char* m_name;
    lua_State* m_luaState = nullptr;
};

inline lua_State* openLua()
{
    lua_State* L = luaL_newstate();

    luaopen_base(L);
    luaL_openlibs(L);

    return L;
}

inline void closeLua(lua_State* L) { lua_close(L); }

} // namespace ELuna

#endif //_LUA_ELUNA_H_
