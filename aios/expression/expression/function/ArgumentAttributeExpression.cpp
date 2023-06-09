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
#include "expression/function/ArgumentAttributeExpression.h"
#include "expression/util/SyntaxStringConvertor.h"

using namespace std;

namespace expression {

AUTIL_LOG_SETUP(expression, ArgumentAttributeExpression);

ArgumentAttributeExpression::ArgumentAttributeExpression(
        ConstExprType constType,
        const string &exprStr)
    : AttributeExpression(ET_ARGUMENT, EVT_UNKNOWN, false, exprStr)
    , _constValueType(constType)
{
    assert(UNKNOWN != _constValueType);
    if (_constValueType == INTEGER_VALUE) {
        _exprValueType = EVT_INT64;
    } else if (_constValueType == FLOAT_VALUE) {
        _exprValueType = EVT_DOUBLE;
    } else {
        _exprValueType = EVT_STRING;
    }
}

ArgumentAttributeExpression::~ArgumentAttributeExpression() {
}

bool ArgumentAttributeExpression::allocate(
        matchdoc::MatchDocAllocator *allocator,
        const string &groupName, uint8_t serializeLevel)
{
    return true;
}

void ArgumentAttributeExpression::evaluate(const matchdoc::MatchDoc &matchDoc) {
}

void ArgumentAttributeExpression::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
}

// "abc" -> abc
// "a\"bc" -> a"bc
// "a\\bc" -> a\bc
template<>
bool ArgumentAttributeExpression::getConstValue<string>(string &value) const
{
    if (_constValueType != STRING_VALUE) {
        return false;
    }

    value = SyntaxStringConvertor::decodeEscapeString(getExprString());
    return true;
}

}
