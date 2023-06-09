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

#include <assert.h>
#include "autil/MultiValueType.h"
#include "autil/LongHashValue.h"
#include "autil/Hyperloglog.h"

namespace matchdoc {

enum BuiltinType : uint16_t {
    bt_unknown,
    bt_int8,
    bt_int16,
    bt_int32,
    bt_int64,
    bt_uint8,
    bt_uint16,
    bt_uint32,
    bt_uint64,
    bt_float,
    bt_double,
    bt_string, // for declare<autil::MultiChar> or std::string
    bt_bool,
    bt_cstring_deprecated, // deprecated, now isStdType is used to distinguish multichar from std::string
    bt_hash_128,
    bt_hllctx,
    bt_user_type1,
    bt_user_type2,
    bt_user_type3,
    bt_user_type4,
    bt_user_type5,
    bt_user_type6,
    bt_user_type7,
    bt_user_type8,
    bt_max
};

inline static int16_t getBuiltinTypeOrder(BuiltinType type) {
    static std::map<BuiltinType, int16_t> typeSizeMap = {
        {bt_int8, 1},
        {bt_uint8, 2},
        {bt_int16, 3},
        {bt_uint16, 4},
        {bt_int32, 5},
        {bt_uint32, 6},
        {bt_int64, 7},
        {bt_uint64, 8},
        {bt_float, 9},
        {bt_double, 10}
    };
    auto iter =  typeSizeMap.find(type);
    if (iter != typeSizeMap.end()) {
        return iter->second;
    } else {
        return 0;
    }
}

inline static BuiltinType builtinTypeFromString(std::string type) {
    BuiltinType builtinType = bt_unknown;
    if (strcasecmp("int8", type.c_str()) == 0) {
        builtinType = bt_int8;
    } else if (strcasecmp("int16", type.c_str()) == 0) {
        builtinType = bt_int16;
    } else if (strcasecmp("int32", type.c_str()) == 0) {
        builtinType = bt_int32;
    } else if (strcasecmp("int64", type.c_str()) == 0) {
        builtinType = bt_int64;
    } else if (strcasecmp("uint8", type.c_str()) == 0) {
        builtinType = bt_uint8;
    } else if (strcasecmp("uint16", type.c_str()) == 0) {
        builtinType = bt_uint16;
    } else if (strcasecmp("uint32", type.c_str()) == 0) {
        builtinType = bt_uint32;
    } else if (strcasecmp("uint64", type.c_str()) == 0) {
        builtinType = bt_uint64;
    } else if (strcasecmp("float", type.c_str()) == 0) {
        builtinType = bt_float;
    } else if (strcasecmp("double", type.c_str()) == 0) {
        builtinType = bt_double;
    } else if (strcasecmp("string", type.c_str()) == 0) {
        builtinType = bt_string;
    }
    return builtinType;
}

inline static std::string builtinTypeToString(BuiltinType vt) {
    switch (vt) {
        case matchdoc::bt_int8:
            return "int8";
        case matchdoc::bt_uint8:
            return "uint8";
        case matchdoc::bt_int16:
            return "int16";
        case matchdoc::bt_uint16:
            return "uint16";
        case matchdoc::bt_int32:
            return "int32";
        case matchdoc::bt_uint32:
            return "uint32";
        case matchdoc::bt_int64:
            return "int64";
        case matchdoc::bt_uint64:
            return "uint64";
        case matchdoc::bt_float:
            return "float";
        case matchdoc::bt_double:
            return "double";
        case matchdoc::bt_string:
            return "string";
        default:
            return "unknown";
    }
}

template <BuiltinType bt, bool multi = false>
struct BuiltinType2CppType {
    struct UnknownType {};
    typedef UnknownType CppType;
};
#define BuiltinType2CppTypeTraits(builtinType, cppType) \
    template <>                                         \
    struct BuiltinType2CppType<builtinType, false> {    \
        typedef cppType CppType;                        \
    }
BuiltinType2CppTypeTraits(bt_int8, int8_t);
BuiltinType2CppTypeTraits(bt_int16, int16_t);
BuiltinType2CppTypeTraits(bt_int32, int32_t);
BuiltinType2CppTypeTraits(bt_int64, int64_t);
BuiltinType2CppTypeTraits(bt_uint8, uint8_t);
BuiltinType2CppTypeTraits(bt_uint16, uint16_t);
BuiltinType2CppTypeTraits(bt_uint32, uint32_t);
BuiltinType2CppTypeTraits(bt_uint64, uint64_t);
BuiltinType2CppTypeTraits(bt_bool, bool);
BuiltinType2CppTypeTraits(bt_float, float);
BuiltinType2CppTypeTraits(bt_double, double);
BuiltinType2CppTypeTraits(bt_string, autil::MultiChar);
BuiltinType2CppTypeTraits(bt_hash_128, autil::uint128_t);
BuiltinType2CppTypeTraits(bt_hllctx, autil::HllCtx);


#define BuiltinType2CppTypeTraitsMulti(builtinType, cppType)    \
    template <>                                                 \
    struct BuiltinType2CppType<builtinType, true> {             \
        typedef autil::MultiValueType<cppType> CppType;         \
    }

BuiltinType2CppTypeTraitsMulti(bt_int8, int8_t);
BuiltinType2CppTypeTraitsMulti(bt_int16, int16_t);
BuiltinType2CppTypeTraitsMulti(bt_int32, int32_t);
BuiltinType2CppTypeTraitsMulti(bt_int64, int64_t);
BuiltinType2CppTypeTraitsMulti(bt_uint8, uint8_t);
BuiltinType2CppTypeTraitsMulti(bt_uint16, uint16_t);
BuiltinType2CppTypeTraitsMulti(bt_uint32, uint32_t);
BuiltinType2CppTypeTraitsMulti(bt_uint64, uint64_t);
BuiltinType2CppTypeTraitsMulti(bt_bool, bool);
BuiltinType2CppTypeTraitsMulti(bt_float, float);
BuiltinType2CppTypeTraitsMulti(bt_double, double);
BuiltinType2CppTypeTraitsMulti(bt_string, autil::MultiChar);
BuiltinType2CppTypeTraitsMulti(bt_hash_128, autil::uint128_t);

template <BuiltinType bt, bool isMulti>
struct MatchDocBuiltinType2CppType {
    struct UnknownType {};
    typedef UnknownType CppType;
};
#define MatchDocBuiltinType2CppTypeTraits(builtinType, isMulti, cppType) \
    template <>                                                         \
    struct MatchDocBuiltinType2CppType<builtinType, isMulti> {          \
        typedef cppType CppType;                                        \
    }
MatchDocBuiltinType2CppTypeTraits(bt_int8, false, int8_t);
MatchDocBuiltinType2CppTypeTraits(bt_int16, false, int16_t);
MatchDocBuiltinType2CppTypeTraits(bt_int32, false, int32_t);
MatchDocBuiltinType2CppTypeTraits(bt_int64, false, int64_t);
MatchDocBuiltinType2CppTypeTraits(bt_uint8, false, uint8_t);
MatchDocBuiltinType2CppTypeTraits(bt_uint16, false, uint16_t);
MatchDocBuiltinType2CppTypeTraits(bt_uint32, false, uint32_t);
MatchDocBuiltinType2CppTypeTraits(bt_uint64, false, uint64_t);
MatchDocBuiltinType2CppTypeTraits(bt_bool, false, bool);
MatchDocBuiltinType2CppTypeTraits(bt_float, false, float);
MatchDocBuiltinType2CppTypeTraits(bt_double, false, double);
MatchDocBuiltinType2CppTypeTraits(bt_string, false, autil::MultiChar);
MatchDocBuiltinType2CppTypeTraits(bt_hash_128, false, autil::uint128_t);
MatchDocBuiltinType2CppTypeTraits(bt_hllctx, false, autil::HllCtx);

MatchDocBuiltinType2CppTypeTraits(bt_int8, true, autil::MultiInt8);
MatchDocBuiltinType2CppTypeTraits(bt_int16, true, autil::MultiInt16);
MatchDocBuiltinType2CppTypeTraits(bt_int32, true, autil::MultiInt32);
MatchDocBuiltinType2CppTypeTraits(bt_int64, true, autil::MultiInt64);
MatchDocBuiltinType2CppTypeTraits(bt_uint8, true, autil::MultiUInt8);
MatchDocBuiltinType2CppTypeTraits(bt_uint16, true, autil::MultiUInt16);
MatchDocBuiltinType2CppTypeTraits(bt_uint32, true, autil::MultiUInt32);
MatchDocBuiltinType2CppTypeTraits(bt_uint64, true, autil::MultiUInt64);
MatchDocBuiltinType2CppTypeTraits(bt_float, true, autil::MultiFloat);
MatchDocBuiltinType2CppTypeTraits(bt_double, true, autil::MultiDouble);
MatchDocBuiltinType2CppTypeTraits(bt_string, true, autil::MultiString);

struct ValueType {
    uint32_t _isBuiltinType : 1;
    uint32_t _isMulti : 1;
    uint32_t _buildInType : 8;
    uint32_t _reserved : 19;
    uint32_t _needNotConstruct : 1;
    uint32_t _ha3ReservedFlag : 1;
    uint32_t _isStdType : 1;
    ValueType() {
        *(uint32_t *)this = 0;
    }
    uint32_t toInt() const {
        return *(uint32_t *)this;
    }
    bool isBuiltInType() const {
        return _isBuiltinType == 1;
    }
    void setBuiltin() {
        _isBuiltinType = 1;
    }
    bool isMultiValue() const {
        return _isMulti == 1;
    }
    void setMultiValue(bool isMulti) {
        _isMulti = isMulti ? 1 : 0;
    }
    BuiltinType getBuiltinType() const {
        assert(isBuiltInType());
        if (_buildInType >= bt_max) {
            return bt_unknown;
        }
        return (BuiltinType)_buildInType;
    }
    void setBuiltinType(BuiltinType bt) {
        _buildInType = bt;
    }
    void setStdType(bool isStdType) {
        _isStdType = isStdType;
    }
    bool isStdType() const {
        return _isStdType == 1;
    }
    uint32_t getType() const {
        return *(uint32_t *)this;
    }
    void setType(uint32_t i) {
        *(uint32_t *)this = i;
    }
    void setNeedConstruct(bool needConstruct) {
        _needNotConstruct = needConstruct ? 0 : 1;
    }
    bool needConstruct() const {
        return _needNotConstruct != 1;
    }
    uint32_t getTypeIgnoreConstruct() const {
        ValueType res = *this;
        res._needNotConstruct = 0;
        return res.getType();
    }
    bool operator==(const ValueType &other) const {
        return *(uint32_t *)this == *(uint32_t *)(&other);
    }
};

template <typename T>
struct ValueTypeHelper {
    static ValueType getValueType() {
        return ValueType();
    }
};

#define BUILTIN_TYPE_HELPER(T, isMulti, isStdType, bt)  \
    template <>                                         \
    struct ValueTypeHelper<T> {                         \
        static inline ValueType getValueType() {        \
            ValueType vt;                               \
            vt.setBuiltin();                            \
            vt.setMultiValue(isMulti);                  \
            vt.setStdType(isStdType);                   \
            vt.setBuiltinType(bt);                      \
            return vt;                                  \
        }                                               \
    }

BUILTIN_TYPE_HELPER(int8_t, false, false, bt_int8);
BUILTIN_TYPE_HELPER(int16_t, false, false, bt_int16);
BUILTIN_TYPE_HELPER(int32_t, false, false, bt_int32);
BUILTIN_TYPE_HELPER(int64_t, false, false, bt_int64);
BUILTIN_TYPE_HELPER(uint8_t, false, false, bt_uint8);
BUILTIN_TYPE_HELPER(uint16_t, false, false, bt_uint16);
BUILTIN_TYPE_HELPER(uint32_t, false, false, bt_uint32);
BUILTIN_TYPE_HELPER(uint64_t, false, false, bt_uint64);
BUILTIN_TYPE_HELPER(autil::uint128_t, false, false, bt_hash_128);
BUILTIN_TYPE_HELPER(float, false, false, bt_float);
BUILTIN_TYPE_HELPER(double, false, false, bt_double);
BUILTIN_TYPE_HELPER(bool, false, false, bt_bool);
BUILTIN_TYPE_HELPER(autil::MultiChar, false, false, bt_string);
BUILTIN_TYPE_HELPER(std::string, false, true, bt_string);
BUILTIN_TYPE_HELPER(autil::HllCtx, false, false, bt_hllctx);

BUILTIN_TYPE_HELPER(autil::MultiInt8, true, false, bt_int8);
BUILTIN_TYPE_HELPER(autil::MultiInt16, true, false, bt_int16);
BUILTIN_TYPE_HELPER(autil::MultiInt32, true, false, bt_int32);
BUILTIN_TYPE_HELPER(autil::MultiInt64, true, false, bt_int64);
BUILTIN_TYPE_HELPER(autil::MultiUInt8, true, false, bt_uint8);
BUILTIN_TYPE_HELPER(autil::MultiUInt16, true, false, bt_uint16);
BUILTIN_TYPE_HELPER(autil::MultiUInt32, true, false, bt_uint32);
BUILTIN_TYPE_HELPER(autil::MultiUInt64, true, false, bt_uint64);
BUILTIN_TYPE_HELPER(autil::MultiFloat, true, false, bt_float);
BUILTIN_TYPE_HELPER(autil::MultiDouble, true, false, bt_double);
BUILTIN_TYPE_HELPER(autil::MultiString, true, false, bt_string);

BUILTIN_TYPE_HELPER(std::vector<int8_t>, true, true, bt_int8);
BUILTIN_TYPE_HELPER(std::vector<int16_t>, true, true, bt_int16);
BUILTIN_TYPE_HELPER(std::vector<int32_t>, true, true, bt_int32);
BUILTIN_TYPE_HELPER(std::vector<int64_t>, true, true, bt_int64);
BUILTIN_TYPE_HELPER(std::vector<uint8_t>, true, true, bt_uint8);
BUILTIN_TYPE_HELPER(std::vector<uint16_t>, true, true, bt_uint16);
BUILTIN_TYPE_HELPER(std::vector<uint32_t>, true, true, bt_uint32);
BUILTIN_TYPE_HELPER(std::vector<uint64_t>, true, true, bt_uint64);
BUILTIN_TYPE_HELPER(std::vector<autil::uint128_t>, true, true, bt_hash_128);
BUILTIN_TYPE_HELPER(std::vector<float>, true, true, bt_float);
BUILTIN_TYPE_HELPER(std::vector<double>, true, true, bt_double);
BUILTIN_TYPE_HELPER(std::vector<bool>, true, true, bt_bool);
BUILTIN_TYPE_HELPER(std::vector<std::string>, true, true, bt_string);

}

#define INTERGER_BUILTIN_TYPE_MACRO_HELPER(MY_MACRO)             \
    MY_MACRO(matchdoc::bt_int8);                                 \
    MY_MACRO(matchdoc::bt_int16);                                \
    MY_MACRO(matchdoc::bt_int32);                                \
    MY_MACRO(matchdoc::bt_int64);                                \
    MY_MACRO(matchdoc::bt_uint8);                                \
    MY_MACRO(matchdoc::bt_uint16);                               \
    MY_MACRO(matchdoc::bt_uint32);                               \
    MY_MACRO(matchdoc::bt_uint64);

#define NUMBER_BUILTIN_TYPE_MACRO_HELPER(MY_MACRO)               \
    INTERGER_BUILTIN_TYPE_MACRO_HELPER(MY_MACRO)                 \
    MY_MACRO(matchdoc::bt_float);                                \
    MY_MACRO(matchdoc::bt_double);

#define BUILTIN_TYPE_MACRO_HELPER(MY_MACRO)                      \
    NUMBER_BUILTIN_TYPE_MACRO_HELPER(MY_MACRO)                   \
    MY_MACRO(matchdoc::bt_string);

#define _CASE_HELPER(buildinType, valueType, MY_MACRO)                  \
    case buildinType:                                                   \
    if (valueType.isMultiValue()) {                                     \
        typedef matchdoc::BuiltinType2CppType<buildinType, true>::CppType Type; \
        MY_MACRO(Type);                                                 \
    } else {                                                            \
        typedef matchdoc::BuiltinType2CppType<buildinType, false>::CppType Type; \
        MY_MACRO(Type);                                                 \
    }                                                                   \
    break;

#define SWITCH_BUILTIN_TYPE_MACRO_HELPER(valueType, MY_MACRO, ON_DEFAULT) \
    do {                                                                \
        switch (valueType.getBuiltinType()) {                           \
            _CASE_HELPER(matchdoc::bt_int8, valueType, MY_MACRO);       \
            _CASE_HELPER(matchdoc::bt_uint8, valueType, MY_MACRO);      \
            _CASE_HELPER(matchdoc::bt_int16, valueType, MY_MACRO);      \
            _CASE_HELPER(matchdoc::bt_uint16, valueType, MY_MACRO);     \
            _CASE_HELPER(matchdoc::bt_int32, valueType, MY_MACRO);      \
            _CASE_HELPER(matchdoc::bt_uint32, valueType, MY_MACRO);     \
            _CASE_HELPER(matchdoc::bt_int64, valueType, MY_MACRO);      \
            _CASE_HELPER(matchdoc::bt_uint64, valueType, MY_MACRO);     \
            _CASE_HELPER(matchdoc::bt_float, valueType, MY_MACRO);      \
            _CASE_HELPER(matchdoc::bt_double, valueType, MY_MACRO);     \
        case matchdoc::bt_string:                                       \
            if (valueType.isMultiValue()) {                             \
                typedef matchdoc::BuiltinType2CppType<matchdoc::bt_string, true>::CppType Type; \
                MY_MACRO(Type);                                         \
            } else if (!valueType.isStdType()) {                        \
                typedef matchdoc::BuiltinType2CppType<matchdoc::bt_string, false>::CppType Type; \
                MY_MACRO(Type);                                         \
            } else {                                                    \
                MY_MACRO(std::string);                                  \
            }                                                           \
            break;                                                      \
        default:                                                        \
            ON_DEFAULT(valueType);                                      \
            break;                                                      \
        }                                                               \
    } while(0)
