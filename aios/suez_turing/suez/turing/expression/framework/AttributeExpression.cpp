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
#include "suez/turing/expression/framework/AttributeExpression.h"

#include <functional>
#include <ostream>

#include "autil/MultiValueType.h"

using namespace std;
namespace suez {
namespace turing {

// "abc" -> abc
// "a\"bc" -> a"bc
// "a\\bc" -> a\bc

template <>
bool AttributeExpression::getConstValue<string>(string &value) const {
    if (!(ET_ARGUMENT == getExpressionType() && vt_string == getType())) {
        return false;
    }
    const string &oriStr = getOriginalString();
    value.clear();
    value.reserve(oriStr.size());
    for (size_t i = 1; i + 1 < oriStr.size(); ++i) {
        if (oriStr[i] == '\\') {
            ++i;
        }
        value += oriStr[i];
    }

    return true;
}

#define SUPPORTED_EVALUATE_HASH(T)                                                                                     \
    template <>                                                                                                        \
    uint64_t AttributeExpressionTyped<T>::evaluateHash(matchdoc::MatchDoc matchDoc) {                                  \
        _value = evaluateAndReturn(matchDoc);                                                                          \
        return std::hash<T>()(_value);                                                                                 \
    }

SUPPORTED_EVALUATE_HASH(bool)
SUPPORTED_EVALUATE_HASH(int8_t)
SUPPORTED_EVALUATE_HASH(int16_t)
SUPPORTED_EVALUATE_HASH(int32_t)
SUPPORTED_EVALUATE_HASH(int64_t)
SUPPORTED_EVALUATE_HASH(uint8_t)
SUPPORTED_EVALUATE_HASH(uint16_t)
SUPPORTED_EVALUATE_HASH(uint32_t)
SUPPORTED_EVALUATE_HASH(uint64_t)
SUPPORTED_EVALUATE_HASH(float)
SUPPORTED_EVALUATE_HASH(double)
SUPPORTED_EVALUATE_HASH(autil::MultiChar)

#undef SUPPORTED_EVALUATE_HASH

std::ostream &operator<<(std::ostream &os, const AttributeExpression &expr) {
    return os << "{" << expr.getOriginalString() << "}";
}

std::ostream &operator<<(std::ostream &os, const AttributeExpression *expr) {
    if (expr != nullptr) {
        return os << *expr;
    } else {
        return os << "nullptr";
    }
}

} // namespace turing
} // namespace suez
