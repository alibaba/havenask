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
#include "suez/turing/expression/cava/impl/AttributeExpression.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/MultiValueType.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

class CavaCtx;
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

using namespace std;

namespace unsafe {

#define GET_SINGLE_VALUE_WITH_TYPE(name, type)                                                                         \
    type AttributeExpression::get##name(CavaCtx *ctx, ha3::MatchDoc *doc) {                                            \
        auto attributeExpr = (suez::turing::AttributeExpressionTyped<type> *)this;                                     \
        return attributeExpr->evaluateAndReturn(*(matchdoc::MatchDoc *)doc);                                           \
    }

#define GET_MULTI_VALUE_WITH_TYPE(name, type)                                                                          \
    ha3::name *AttributeExpression::get##name(CavaCtx *ctx, ha3::MatchDoc *doc, ha3::name *result) {                   \
        auto attributeExpr = (suez::turing::AttributeExpressionTyped<type> *)this;                                     \
        auto mvalue = attributeExpr->evaluateAndReturn(*(matchdoc::MatchDoc *)doc);                                    \
        if (!result) {                                                                                                 \
            result = (ha3::name *)suez::turing::SuezCavaAllocUtil::alloc(ctx, sizeof(ha3::name));                      \
            if (unlikely(result == NULL)) {                                                                            \
                return NULL;                                                                                           \
            }                                                                                                          \
        }                                                                                                              \
        result->reset(&mvalue);                                                                                        \
        return result;                                                                                                 \
    }

GET_SINGLE_VALUE_WITH_TYPE(Boolean, bool);
GET_SINGLE_VALUE_WITH_TYPE(Int8, int8_t);
GET_SINGLE_VALUE_WITH_TYPE(Int16, int16_t);
GET_SINGLE_VALUE_WITH_TYPE(Int32, int32_t);
GET_SINGLE_VALUE_WITH_TYPE(Int64, int64_t);
GET_SINGLE_VALUE_WITH_TYPE(UInt8, uint8_t);
GET_SINGLE_VALUE_WITH_TYPE(UInt16, uint16_t);
GET_SINGLE_VALUE_WITH_TYPE(UInt32, uint32_t);
GET_SINGLE_VALUE_WITH_TYPE(UInt64, uint64_t);
GET_SINGLE_VALUE_WITH_TYPE(Float, float);
GET_SINGLE_VALUE_WITH_TYPE(Double, double);

ha3::MChar *AttributeExpression::getMChar(CavaCtx *ctx, ha3::MatchDoc *doc) {
    auto attributeExpr = (suez::turing::AttributeExpressionTyped<autil::MultiChar> *)this;
    const auto &multiChar = attributeExpr->evaluateAndReturn(*(matchdoc::MatchDoc *)doc);
    assert(multiChar.isEmptyData() || multiChar.hasEncodedCount());
    return (ha3::MChar *)multiChar.getBaseAddress();
}

GET_MULTI_VALUE_WITH_TYPE(MInt8, autil::MultiInt8);
GET_MULTI_VALUE_WITH_TYPE(MInt16, autil::MultiInt16);
GET_MULTI_VALUE_WITH_TYPE(MInt32, autil::MultiInt32);
GET_MULTI_VALUE_WITH_TYPE(MInt64, autil::MultiInt64);
GET_MULTI_VALUE_WITH_TYPE(MUInt8, autil::MultiUInt8);
GET_MULTI_VALUE_WITH_TYPE(MUInt16, autil::MultiUInt16);
GET_MULTI_VALUE_WITH_TYPE(MUInt32, autil::MultiUInt32);
GET_MULTI_VALUE_WITH_TYPE(MUInt64, autil::MultiUInt64);
GET_MULTI_VALUE_WITH_TYPE(MFloat, autil::MultiFloat);
GET_MULTI_VALUE_WITH_TYPE(MDouble, autil::MultiDouble);
GET_MULTI_VALUE_WITH_TYPE(MString, autil::MultiString);

} // namespace unsafe
