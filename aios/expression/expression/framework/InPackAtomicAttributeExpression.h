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
#ifndef ISEARCH_EXPRESSION_INPACKATOMICATTRIBUTEEXPRESSION_H
#define ISEARCH_EXPRESSION_INPACKATOMICATTRIBUTEEXPRESSION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/TypeTraits.h"

namespace expression {

template <typename T>
class InPackAtomicAttributeExpression : public AttributeExpressionTyped<T>
{
public:
    InPackAtomicAttributeExpression(const std::string &attrName);
    ~InPackAtomicAttributeExpression() {}
private:
    InPackAtomicAttributeExpression(const InPackAtomicAttributeExpression &);
    InPackAtomicAttributeExpression& operator=(const InPackAtomicAttributeExpression &);
public:
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc) {}
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {}
    /* override */ bool needEvaluate() const { return false; }

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////////////////
template <typename T>
InPackAtomicAttributeExpression<T>::InPackAtomicAttributeExpression(
        const std::string &attrName)
    : AttributeExpressionTyped<T>(ET_INPACK,
                                  AttrValueType2ExprValueType<T>::evt,
                                  AttrValueType2ExprValueType<T>::isMulti,
                                  attrName)
{}

}

#endif //ISEARCH_EXPRESSION_INPACKATOMICATTRIBUTEEXPRESSION_H
