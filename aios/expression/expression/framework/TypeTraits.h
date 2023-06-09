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
#ifndef ISEARCH_EXPRESSION_ATTRTYPETRAITS_H
#define ISEARCH_EXPRESSION_ATTRTYPETRAITS_H

#include "expression/common.h"
#include "expression/framework/TypeInfo.h"
#include "autil/MultiValueType.h"
#include "autil/LongHashValue.h"

namespace expression {



template <typename T>
struct AttrValueType2ExprValueType {
    static const ExprValueType evt = EVT_UNKNOWN;
    static const bool isMulti = false;
};

template <ExprValueType evt, bool isMulti>
struct ExprValueType2AttrValueType {
    struct Unknown {};
    typedef Unknown AttrValueType;
};

#define ExprValueTypeTraits(attrValueType, exprValueType, isMulti_)     \
    template <>                                                         \
    struct AttrValueType2ExprValueType<attrValueType> {                 \
        static const ExprValueType evt = exprValueType;                 \
        static const bool isMulti = isMulti_;                           \
    };                                                                  \
    template <>                                                         \
    struct ExprValueType2AttrValueType<exprValueType, isMulti_> {       \
        typedef attrValueType AttrValueType;                            \
    }

ExprValueTypeTraits(int8_t, EVT_INT8, false);
ExprValueTypeTraits(int16_t, EVT_INT16, false);
ExprValueTypeTraits(int32_t, EVT_INT32, false);
ExprValueTypeTraits(int64_t, EVT_INT64, false);
ExprValueTypeTraits(uint8_t, EVT_UINT8, false);
ExprValueTypeTraits(uint16_t, EVT_UINT16, false);
ExprValueTypeTraits(uint32_t, EVT_UINT32, false);
ExprValueTypeTraits(uint64_t, EVT_UINT64, false);
ExprValueTypeTraits(float, EVT_FLOAT, false);
ExprValueTypeTraits(double, EVT_DOUBLE, false);
ExprValueTypeTraits(bool, EVT_BOOL, false);
ExprValueTypeTraits(void, EVT_VOID, false);
ExprValueTypeTraits(autil::MultiChar, EVT_STRING, false);

ExprValueTypeTraits(autil::MultiInt8, EVT_INT8, true);
ExprValueTypeTraits(autil::MultiInt16, EVT_INT16, true);
ExprValueTypeTraits(autil::MultiInt32, EVT_INT32, true);
ExprValueTypeTraits(autil::MultiInt64, EVT_INT64, true);
ExprValueTypeTraits(autil::MultiUInt8, EVT_UINT8, true);
ExprValueTypeTraits(autil::MultiUInt16, EVT_UINT16, true);
ExprValueTypeTraits(autil::MultiUInt32, EVT_UINT32, true);
ExprValueTypeTraits(autil::MultiUInt64, EVT_UINT64, true);
ExprValueTypeTraits(autil::MultiValueType<bool>, EVT_BOOL, true);
ExprValueTypeTraits(autil::MultiFloat, EVT_FLOAT, true);
ExprValueTypeTraits(autil::MultiDouble, EVT_DOUBLE, true);
ExprValueTypeTraits(autil::MultiString, EVT_STRING, true);

template <typename T>
struct BasicTypeTraits {
    typedef T BasicType;
};

#define MULTI_VALUE_TYPE_TO_BASIC_TYPE(MultiType)  \
    template <>                                    \
    struct BasicTypeTraits<MultiType>              \
    {                                              \
        typedef typename MultiType::type BasicType;\
    }

MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiInt8);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiInt16);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiInt32);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiInt64);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiUInt8);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiUInt16);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiUInt32);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiUInt64);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiFloat);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiDouble);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiString);
MULTI_VALUE_TYPE_TO_BASIC_TYPE(autil::MultiChar);



//////////////////////////////////////////////////////////////////////////

}
#endif //ISEARCH_EXPRESSION_ATTRTYPETRAITS_H
