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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/MultiValueType.h"
#include "cava/common/Common.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"

namespace ha3 {

constexpr size_t SPECIAL_EMPTY_MCHAR_ADDR = -1;

enum CAVA_FIELD_TYPE {
    ct_boolean,
    ct_byte,
    ct_short,
    ct_int,
    ct_long,
    ct_ubyte,
    ct_ushort,
    ct_uint,
    ct_ulong,
    ct_float,
    ct_double,
    ct_MChar,
    ct_MInt8,
    ct_MInt16,
    ct_MInt32,
    ct_MInt64,
    ct_MUInt8,
    ct_MUInt16,
    ct_MUInt32,
    ct_MUInt64,
    ct_MFloat,
    ct_MDouble,
    ct_MString,
    ct_UnkownType
};

class CavaFieldTypeHelper {
public:
    static CAVA_FIELD_TYPE getType(const std::string &name) {
        auto it = _name2Type.find(name);
        if (it == _name2Type.end()) {
            return CAVA_FIELD_TYPE::ct_UnkownType;
        }
        return it->second;
    }
    static std::pair<matchdoc::BuiltinType, bool> getTypePair(const std::string &name) {
        auto it = _name2TypePair.find(name);
        if (it == _name2TypePair.end()) {
            return std::pair<matchdoc::BuiltinType, bool>(matchdoc::bt_unknown, false);
        }
        return it->second;
    }

private:
    static std::map<std::string, CAVA_FIELD_TYPE> _name2Type;
    static std::map<std::string, std::pair<matchdoc::BuiltinType, bool>> _name2TypePair;
};

template <class T>
struct CppType2CavaType {
    typedef T CavaType;
};

//注意这里不说ha3::MIntX 而是ha3::MIntX *
#define CppType2CavaTypeTraits(cppType, cavaType)                                                                      \
    template <>                                                                                                        \
    struct CppType2CavaType<cppType> {                                                                                 \
        typedef cavaType *CavaType;                                                                                    \
    }

CppType2CavaTypeTraits(autil::MultiChar, ha3::MChar);
CppType2CavaTypeTraits(autil::MultiInt8, ha3::MInt8);
CppType2CavaTypeTraits(autil::MultiInt16, ha3::MInt16);
CppType2CavaTypeTraits(autil::MultiInt32, ha3::MInt32);
CppType2CavaTypeTraits(autil::MultiInt64, ha3::MInt64);
CppType2CavaTypeTraits(autil::MultiUInt8, ha3::MUInt8);
CppType2CavaTypeTraits(autil::MultiUInt16, ha3::MUInt16);
CppType2CavaTypeTraits(autil::MultiUInt32, ha3::MUInt32);
CppType2CavaTypeTraits(autil::MultiUInt64, ha3::MUInt64);
CppType2CavaTypeTraits(autil::MultiFloat, ha3::MFloat);
CppType2CavaTypeTraits(autil::MultiDouble, ha3::MDouble);
CppType2CavaTypeTraits(autil::MultiString, ha3::MString);

template <class T>
inline std::string CppType2CavaTypeName() {
    return "unknown";
};
#define CppType2CavaTypeNameTraits(cppType, cavaTypeName)                                                              \
    template <>                                                                                                        \
    inline std::string CppType2CavaTypeName<cppType>() {                                                               \
        return #cavaTypeName;                                                                                          \
    }

CppType2CavaTypeNameTraits(bool, boolean);
CppType2CavaTypeNameTraits(int8_t, byte);
CppType2CavaTypeNameTraits(int16_t, short);
CppType2CavaTypeNameTraits(int32_t, int);
CppType2CavaTypeNameTraits(int64_t, long);
CppType2CavaTypeNameTraits(uint8_t, ubyte);
CppType2CavaTypeNameTraits(uint16_t, ushort);
CppType2CavaTypeNameTraits(uint32_t, uint);
CppType2CavaTypeNameTraits(uint64_t, ulong);
CppType2CavaTypeNameTraits(float, float);
CppType2CavaTypeNameTraits(double, double);
CppType2CavaTypeNameTraits(autil::MultiChar, MChar);
CppType2CavaTypeNameTraits(autil::MultiInt8, MInt8);
CppType2CavaTypeNameTraits(autil::MultiInt16, MInt16);
CppType2CavaTypeNameTraits(autil::MultiInt32, MInt32);
CppType2CavaTypeNameTraits(autil::MultiInt64, MInt64);
CppType2CavaTypeNameTraits(autil::MultiUInt8, MUInt8);
CppType2CavaTypeNameTraits(autil::MultiUInt16, MUInt16);
CppType2CavaTypeNameTraits(autil::MultiUInt32, MUInt32);
CppType2CavaTypeNameTraits(autil::MultiUInt64, MUInt64);
CppType2CavaTypeNameTraits(autil::MultiFloat, MFloat);
CppType2CavaTypeNameTraits(autil::MultiDouble, MDouble);
CppType2CavaTypeNameTraits(autil::MultiString, MString);

//////////////////////////////////////////////////////////
template <class CavaT, class CppT>
inline bool TransCavaValue2CppValue(CavaT const &src, CppT &dest) = delete;

#define TransCavaValue2SingleValue(cppType, cavaType)                                                                  \
    template <>                                                                                                        \
    inline bool TransCavaValue2CppValue<cavaType, cppType>(cavaType const &src, cppType &dest) {                       \
        dest = src;                                                                                                    \
        return true;                                                                                                   \
    }

#define TransCavaValue2MultiValue(cppType, cavaType)                                                                   \
    template <>                                                                                                        \
    inline bool TransCavaValue2CppValue<cavaType, cppType>(cavaType const &src, cppType &dest) {                       \
        if (src == NULL) {                                                                                             \
            return false;                                                                                              \
        }                                                                                                              \
        dest.init(src->getMultiValueBuffer());                                                                         \
        return true;                                                                                                   \
    }

template <>
inline bool TransCavaValue2CppValue<autil::MultiValueType<bool>, autil::MultiValueType<bool>>(
    autil::MultiValueType<bool> const &src, autil::MultiValueType<bool> &dest) {
    return false;
}

template <>
inline bool TransCavaValue2CppValue<MChar *, autil::MultiChar>(MChar *const &src, autil::MultiChar &dest) {
    if (src == nullptr) {
        return false;
    }
    if (src == (MChar *)SPECIAL_EMPTY_MCHAR_ADDR) {
        dest.init(nullptr);
    } else {
        dest.init((char *)src);
    }
    return true;
}

template <>
inline bool TransCavaValue2CppValue<MString *, autil::MultiString>(MString *const &src, autil::MultiString &dest) {
    if (src == nullptr) {
        return false;
    }
    dest.init((char *)(src->getBaseAddress()));
    return true;
}

TransCavaValue2SingleValue(bool, boolean);
TransCavaValue2SingleValue(int8_t, cava::byte);
TransCavaValue2SingleValue(int16_t, short);
TransCavaValue2SingleValue(int32_t, int);
TransCavaValue2SingleValue(int64_t, long);
TransCavaValue2SingleValue(uint8_t, ubyte);
TransCavaValue2SingleValue(uint16_t, ushort);
TransCavaValue2SingleValue(uint32_t, uint);
TransCavaValue2SingleValue(uint64_t, ulong);
TransCavaValue2SingleValue(float, float);
TransCavaValue2SingleValue(double, double);
TransCavaValue2MultiValue(autil::MultiInt8, MInt8 *);
TransCavaValue2MultiValue(autil::MultiInt16, MInt16 *);
TransCavaValue2MultiValue(autil::MultiInt32, MInt32 *);
TransCavaValue2MultiValue(autil::MultiInt64, MInt64 *);
TransCavaValue2MultiValue(autil::MultiUInt8, MUInt8 *);
TransCavaValue2MultiValue(autil::MultiUInt16, MUInt16 *);
TransCavaValue2MultiValue(autil::MultiUInt32, MUInt32 *);
TransCavaValue2MultiValue(autil::MultiUInt64, MUInt64 *);
TransCavaValue2MultiValue(autil::MultiFloat, MFloat *);
TransCavaValue2MultiValue(autil::MultiDouble, MDouble *);

} // namespace ha3

#undef CppType2CavaTypeTraits
#undef CppType2CavaTypeNameTraits
#undef TransCavaValue2SingleValue
#undef TransCavaValue2MultiValue
