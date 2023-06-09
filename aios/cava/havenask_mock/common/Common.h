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

#include <stdint.h>
#include <sys/types.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace cava {

// #ifndef likely(x)
// #define likely(x)                   __builtin_expect(!!(x), 1)
// #endif
// #ifndef unlikely(x)
// #define unlikely(x)                 __builtin_expect(!!(x), 0)
// #endif

#define GENERATE_HAS_MEMBER(member)                                     \
template < class T >                                                    \
class HasMember_##member                                                \
{                                                                       \
private:                                                                \
    using Yes = char[2];                                                \
    using  No = char[1];                                                \
                                                                        \
    struct Fallback { int member; };                                    \
    struct Derived : T, Fallback { };                                   \
                                                                        \
    template < class U >                                                \
    static No& test ( decltype(U::member)* );                           \
    template < typename U >                                             \
    static Yes& test ( U* );                                            \
                                                                        \
public:                                                                 \
    static constexpr bool RESULT = sizeof(                              \
            test<Derived>(nullptr)) == sizeof(Yes);                     \
};                                                                      \
                                                                        \
template < class T >                                                    \
struct has_member_##member                                              \
: public std::integral_constant<bool, HasMember_##member<T>::RESULT>    \
{                                                                       \
};

#define DECLARE_ADD_GET_RESOURCE_FUNC(Func, T, _field)  \
public:                                                 \
    void add##Func(T* node) {                           \
        _field.push_back(node);                         \
    }                                                   \
    std::vector<T*> &get##Func##s() {                   \
        return _field;                                  \
    }                                                   \
    const std::vector<T*> &get##Func##s() const {       \
        return _field;                                  \
    }                                                   \
private:                                                \
    std::vector<T*> _field;

#define DECLARE_SET_GET_FUNC(Func, T, _field)           \
public:                                                 \
    T const &get##Func() const { return _field; }       \
    void set##Func(T const &val) { _field = val; }      \
private:                                                \
    T _field;

#define DECLARE_SET_GET_FUNC_OPT(Func, T, _field)       \
public:                                                 \
    T const &get##Func() const { return _field; }       \
    void set##Func(T val) { _field = std::move(val); }  \
private:                                                \
    T _field;

#define DECLARE_SET_GET_PTR_FUNC(Func, T, _field)       \
public:                                                 \
    T *get##Func() { return _field; }                   \
    void set##Func(T *val) {                            \
        _field = val;                                   \
    }                                                   \
private:                                                \
    T *_field = nullptr;

#define ALLOC_TYPE_VEC(Type, Func, _vec)                                \
public:                                                                 \
    std::vector<Type *> *Func() {                                       \
        std::vector<Type *> *val = new std::vector<Type *>();           \
        _vec.push_back(val);                                            \
        return val;                                                     \
    }                                                                   \
private:                                                                \
    std::vector<std::vector<Type *> *> _vec;


#define FOR_EACH_DELETE(_field)     \
    for (auto item: _field) {       \
        delete item;                \
    }                               \
    _field.clear();

#define FOR_EACH_ADD(vecPtr, addObj, Func)              \
    if (vecPtr != NULL) {                               \
        for (auto val : *vecPtr) {                      \
            addObj->add##Func(val);                     \
        }                                               \
    }

#define MERGE_VEC(objnew, obj1)                 \
    objnew->insert(objnew->end(),               \
                   obj1->begin(), obj1->end());

#define EXPORT_PLUGIN_CREATOR(classtype, classname)                  \
    extern "C" ::cava::classtype* cava##_##classtype##_##classname() \
    {                                                                \
        return new classname();                                      \
    }

extern const std::string PREFIX_AST_REWRITER;
extern const std::string PREFIX_TYPE_BUILDER;
extern const std::string PREFIX_SEMA_CHECKER;
extern const std::string PREFIX_NOT_EXISTS;

extern const std::string CAVA_PACKAGE_NAME;
extern const std::string LANG_PACKAGE_NAME;
extern const std::string CSTRING_CLASS_NAME;

extern const std::string CAVACTX_FORMAL_NAME;
extern const std::string THIS_FORMAL_NAME;
extern const std::string CAVACTX_CLASS_NAME;
extern const std::string NULL_CLASS_NAME;
extern const std::string UNSAFE_PACKAGE_NAME;
extern const std::string ANY_CLASS_NAME;
extern const std::string CAVAARRAY_CLASS_NAME;
extern const std::string CAVACTX_EXCEPTION_NAME;
constexpr uint CAVACTX_EXCEPTION_IDX = 0;
extern const std::string CAVACTX_STACK_DEPTH_NAME;
constexpr uint CAVACTX_STACK_DEPTH_IDX = 0;
extern const std::string CAVACTX_RUN_TIMES_NAME;
constexpr uint CAVACTX_RUN_TIMES_IDX = 0;

extern const std::string CAVA_ALLOC_NAME;
extern const std::string ARRAY_LENGTH_FIELD_NAME;
constexpr uint ARRAY_LENGTH_FIELD_IDX = 0;
extern const std::string ARRAY_DATA_FIELD_NAME;
constexpr uint ARRAY_DATA_FIELD_IDX = 0;

constexpr size_t HASH_CLASS_TMPL_INST_SALT = 0;

extern const std::string CAVA_CXA_ATEXIT;
extern const std::string CAVA_DSO_HANDLE;
extern const std::string CAVA_DSO_HANDLE_CTOR;

extern const std::string CAVA_CTX_APPEND_STACK_INFO;
extern const std::string CAVA_CTX_CHECK_FORCE_STOP;
constexpr int64_t CAVA_CTX_CHECK_FORCE_STOP_STEP = 0;
constexpr int64_t CAVA_CTX_STACK_DEPTH_LIMIT = 0;

extern const std::hash<std::string> StringHasher;
extern const std::hash<int> IntHasher;

template<typename T> struct is_vector : public std::false_type {};
template<typename T, typename A>
struct is_vector<std::vector<T, A>> : public std::true_type {};

template <class T>
inline void HashCombine(std::size_t& seed, const T& v)
{
    
}

typedef int8_t byte;

} // end namespace cava

typedef bool boolean;
typedef uint8_t ubyte;
typedef uint32_t uint;
typedef uint16_t ushort;
typedef uint64_t ulong;
#define CAVA_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
