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

#include <cstdint>
#include <ext/alloc_traits.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueType.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {

template <AttrType at>
struct AttrType2VariableTypeTraits {
    static const VariableType VARIABLE_TYPE = vt_unknown;
};

template <VariableType at>
struct VariableType2AttrTypeTraits {
    static const AttrType ATTR_TYPE = AT_UNKNOWN;
};

#define AT2VT_AND_VT2AT_HELPER(x, y)                                                                                   \
    template <>                                                                                                        \
    struct AttrType2VariableTypeTraits<x> {                                                                            \
        static const VariableType VARIABLE_TYPE = y;                                                                   \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct VariableType2AttrTypeTraits<y> {                                                                            \
        static const AttrType ATTR_TYPE = x;                                                                           \
    }

AT2VT_AND_VT2AT_HELPER(AT_INT8, vt_int8);
AT2VT_AND_VT2AT_HELPER(AT_INT16, vt_int16);
AT2VT_AND_VT2AT_HELPER(AT_INT32, vt_int32);
AT2VT_AND_VT2AT_HELPER(AT_INT64, vt_int64);

AT2VT_AND_VT2AT_HELPER(AT_UINT8, vt_uint8);
AT2VT_AND_VT2AT_HELPER(AT_UINT16, vt_uint16);
AT2VT_AND_VT2AT_HELPER(AT_UINT32, vt_uint32);
AT2VT_AND_VT2AT_HELPER(AT_UINT64, vt_uint64);

AT2VT_AND_VT2AT_HELPER(AT_FLOAT, vt_float);
AT2VT_AND_VT2AT_HELPER(AT_DOUBLE, vt_double);
AT2VT_AND_VT2AT_HELPER(AT_STRING, vt_string);
AT2VT_AND_VT2AT_HELPER(AT_HASH_128, vt_hash_128);
#undef AT2VT_AND_VT2AT_HELPER

inline VariableType attrType2VariableType(AttrType at) {
#define ATTR_TYPE_2_VARIABLE_TYPE_HELPER(at)                                                                           \
    case at: {                                                                                                         \
        return AttrType2VariableTypeTraits<at>::VARIABLE_TYPE;                                                         \
    } break
    switch (at) {
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_INT8);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_UINT8);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_INT16);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_UINT16);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_INT32);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_UINT32);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_INT64);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_UINT64);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_FLOAT);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_DOUBLE);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_STRING);
        ATTR_TYPE_2_VARIABLE_TYPE_HELPER(AT_HASH_128);
    default:
        break;
    }
#undef ATTR_TYPE_2_VARIABLE_TYPE_HELPER
    return vt_unknown;
}

////////////////////////////////////////////////////
template <>
struct AttrType2VariableTypeTraits<AT_HASH_64> {
    static const VariableType VARIABLE_TYPE = vt_uint64;
};
///////////////////////////////////////////////////

////////////////////////////// declare VariableType2FieldTypeTraits  //////////////////////
template <VariableType at>
struct VariableType2FieldTypeTraits {
    static const FieldType FIELD_TYPE = ft_unknown;
};

#define AT2FT_HELPER(x, y)                                                                                             \
    template <>                                                                                                        \
    struct VariableType2FieldTypeTraits<x> {                                                                           \
        static const FieldType FIELD_TYPE = y;                                                                         \
    }
AT2FT_HELPER(vt_int8, ft_int8);
AT2FT_HELPER(vt_int16, ft_int16);
AT2FT_HELPER(vt_int32, ft_int32);
AT2FT_HELPER(vt_int64, ft_int64);

AT2FT_HELPER(vt_uint8, ft_uint8);
AT2FT_HELPER(vt_uint16, ft_uint16);
AT2FT_HELPER(vt_uint32, ft_uint32);
AT2FT_HELPER(vt_uint64, ft_uint64);

AT2FT_HELPER(vt_float, ft_float);
AT2FT_HELPER(vt_double, ft_double);
AT2FT_HELPER(vt_string, ft_string);
AT2FT_HELPER(vt_hash_128, ft_hash_128);
#undef AT2FT_HELPER

////////////////////////////// declare VariableTypeTraits //////////////////////
template <VariableType vt, bool isMulti>
struct VariableTypeTraits {
    struct UnknownType {};
    typedef UnknownType AttrItemType;
    typedef UnknownType AttrExprType;
    static inline AttrItemType convert(const AttrExprType &x) { return x; }
};

template <typename T>
struct TypeHaInfoTraits {
    static const VariableType VARIABLE_TYPE = vt_unknown;
    static const bool IS_MULTI = false;
};

template <bool>
struct BoolTypeTraits {};

template <typename Type>
inline Type convertType2AttrItemType(const Type &x) {
    return x;
}

template <typename Type>
inline std::vector<Type> convertType2AttrItemType(const autil::MultiValueType<Type> &x) {
    return std::vector<Type>(x.data(), x.data() + x.size());
}

inline std::string convertType2AttrItemType(const autil::MultiChar &x) { return std::string(x.data(), x.size()); }

inline std::vector<std::string> convertType2AttrItemType(const autil::MultiString &x) {
    std::vector<std::string> multiStr;
    multiStr.resize(x.size());
    for (size_t i = 0; i < x.size(); ++i) {
        multiStr[i] = convertType2AttrItemType(x[i]);
    }
    return multiStr;
}

template <typename T>
struct Type2AttrItemType {
    typedef VariableTypeTraits<TypeHaInfoTraits<T>::VARIABLE_TYPE, TypeHaInfoTraits<T>::IS_MULTI> VTT;
    typedef typename VTT::AttrItemType AttrItemType;
    static inline AttrItemType convert(const T &x) { return convertType2AttrItemType(x); }
};

template <bool>
struct SupportTraits {};

typedef SupportTraits<true> Support;
typedef SupportTraits<false> Unsupport;

template <bool>
struct MultiTraits {};
///////////////// declare macro, transform VariableType, AttrItem, AttrExpr ////////////
////////////////////////////////////////////////////////////////////////////////////////

#define SINGLE_VARIABLE_TYPE_TRAITS(vt, type)                                                                          \
    template <>                                                                                                        \
    struct VariableTypeTraits<vt, false> {                                                                             \
        typedef type AttrExprType;                                                                                     \
        typedef type AttrItemType;                                                                                     \
        static inline const AttrItemType &convert(const AttrExprType &x) { return x; }                                 \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct TypeHaInfoTraits<type> {                                                                                    \
        static const VariableType VARIABLE_TYPE = vt;                                                                  \
        static const bool IS_MULTI = false;                                                                            \
    };

SINGLE_VARIABLE_TYPE_TRAITS(vt_int8, int8_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_int16, int16_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_int32, int32_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_int64, int64_t);

SINGLE_VARIABLE_TYPE_TRAITS(vt_uint8, uint8_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_uint16, uint16_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_uint32, uint32_t);
SINGLE_VARIABLE_TYPE_TRAITS(vt_uint64, uint64_t);

SINGLE_VARIABLE_TYPE_TRAITS(vt_float, float);
SINGLE_VARIABLE_TYPE_TRAITS(vt_double, double);
SINGLE_VARIABLE_TYPE_TRAITS(vt_hash_128, primarykey_t);

SINGLE_VARIABLE_TYPE_TRAITS(vt_bool, bool);
////////////////// macro will be different when stardard type is string ////////////////
template <>
struct VariableTypeTraits<vt_string, false> {
    typedef autil::MultiChar AttrExprType;
    typedef std::string AttrItemType;
    static inline AttrItemType convert(const AttrExprType &x) { return convertType2AttrItemType(x); }
};

template <>
struct TypeHaInfoTraits<std::string> {
    static const VariableType VARIABLE_TYPE = vt_string;
    static const bool IS_MULTI = false;
};

template <>
struct TypeHaInfoTraits<autil::MultiChar> : public TypeHaInfoTraits<std::string> {};

///////////////////////////////////////////////////////////////////////////////////////

//////////// multi value attributes /////////////////////////////////////////////////
#define MULTI_VARIABLE_TYPE_TRAITS(vt, type)                                                                           \
    template <>                                                                                                        \
    struct VariableTypeTraits<vt, true> {                                                                              \
        typedef autil::MultiValueType<type> AttrExprType;                                                              \
        typedef std::vector<type> AttrItemType;                                                                        \
        static inline AttrItemType convert(const AttrExprType &x) { return convertType2AttrItemType(x); }              \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct TypeHaInfoTraits<VariableTypeTraits<vt, true>::AttrItemType> {                                              \
        static const VariableType VARIABLE_TYPE = vt;                                                                  \
        static const bool IS_MULTI = true;                                                                             \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct TypeHaInfoTraits<VariableTypeTraits<vt, true>::AttrExprType>                                                \
        : public TypeHaInfoTraits<VariableTypeTraits<vt, true>::AttrItemType> {}

MULTI_VARIABLE_TYPE_TRAITS(vt_int8, int8_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_int16, int16_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_int32, int32_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_int64, int64_t);

MULTI_VARIABLE_TYPE_TRAITS(vt_uint8, uint8_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_uint16, uint16_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_uint32, uint32_t);
MULTI_VARIABLE_TYPE_TRAITS(vt_uint64, uint64_t);

MULTI_VARIABLE_TYPE_TRAITS(vt_float, float);
MULTI_VARIABLE_TYPE_TRAITS(vt_double, double);
MULTI_VARIABLE_TYPE_TRAITS(vt_bool, bool);
MULTI_VARIABLE_TYPE_TRAITS(vt_hash_128, primarykey_t);

#undef MULTI_VARIABLE_TYPE_TRAITS

/////////////////////////////////the same as single value, this is multi-string ///////////////
template <>
struct VariableTypeTraits<vt_string, true> {
    typedef autil::MultiString AttrExprType;
    typedef std::vector<std::string> AttrItemType;
    static inline AttrItemType convert(const AttrExprType &x) { return convertType2AttrItemType(x); }
};
template <>
struct TypeHaInfoTraits<VariableTypeTraits<vt_string, true>::AttrItemType> {
    static const VariableType VARIABLE_TYPE = vt_string;
    static const bool IS_MULTI = true;
};

template <>
struct TypeHaInfoTraits<VariableTypeTraits<vt_string, true>::AttrExprType>
    : public TypeHaInfoTraits<VariableTypeTraits<vt_string, true>::AttrItemType> {};

/////////////////////////////////////////////////////////////////////////////////////////
#define INTEGER_VARIABLE_TYPE_MACRO_HELPER(MY_MACRO)                                                                   \
    MY_MACRO(vt_int8);                                                                                                 \
    MY_MACRO(vt_int16);                                                                                                \
    MY_MACRO(vt_int32);                                                                                                \
    MY_MACRO(vt_int64);                                                                                                \
    MY_MACRO(vt_uint8);                                                                                                \
    MY_MACRO(vt_uint16);                                                                                               \
    MY_MACRO(vt_uint32);                                                                                               \
    MY_MACRO(vt_uint64)

#define NUMERIC_VARIABLE_TYPE_MACRO_HELPER(MY_MACRO)                                                                   \
    MY_MACRO(vt_int8);                                                                                                 \
    MY_MACRO(vt_int16);                                                                                                \
    MY_MACRO(vt_int32);                                                                                                \
    MY_MACRO(vt_int64);                                                                                                \
    MY_MACRO(vt_uint8);                                                                                                \
    MY_MACRO(vt_uint16);                                                                                               \
    MY_MACRO(vt_uint32);                                                                                               \
    MY_MACRO(vt_uint64);                                                                                               \
    MY_MACRO(vt_float);                                                                                                \
    MY_MACRO(vt_double)

#define NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(MY_MACRO)                                                         \
    MY_MACRO(vt_bool);                                                                                                 \
    NUMERIC_VARIABLE_TYPE_MACRO_HELPER(MY_MACRO)

#define NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_STRING(MY_MACRO)                                                       \
    MY_MACRO(vt_string);                                                                                               \
    NUMERIC_VARIABLE_TYPE_MACRO_HELPER(MY_MACRO)

#define NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(MY_MACRO)                                              \
    MY_MACRO(vt_string);                                                                                               \
    NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(MY_MACRO)

/////////////////////////////////////////////////////////////////////////////////////////

inline std::string vt2TypeName(VariableType vt) {
    switch (vt) {
    case vt_int8:
        return "int8_t";
    case vt_int16:
        return "int16_t";
    case vt_int32:
        return "int32_t";
    case vt_int64:
        return "int64_t";
    case vt_uint8:
        return "uint8_t";
    case vt_uint16:
        return "uint16_t";
    case vt_uint32:
        return "uint32_t";
    case vt_uint64:
        return "uint64_t";
    case vt_float:
        return "float";
    case vt_double:
        return "double";
    case vt_string:
        return "string";
    default:
        return "NonBasicType";
    }
}

} // namespace turing
} // namespace suez
