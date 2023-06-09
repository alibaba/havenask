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
#ifndef ISEARCH_EXPRESSION_ARGUMENTATTRIBUTEEXPRESSION_H
#define ISEARCH_EXPRESSION_ARGUMENTATTRIBUTEEXPRESSION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "autil/StringUtil.h"
#include <type_traits>

namespace expression {

class ArgumentAttributeExpression : public AttributeExpression
{
public:
    ArgumentAttributeExpression(ConstExprType constType,
                                const std::string &exprStr);
    ~ArgumentAttributeExpression();
public:
    bool allocate(matchdoc::MatchDocAllocator *slabAllocator,
                                 const std::string &groupName, uint8_t serializeLevel = SL_PROXY) override;
    void evaluate(const matchdoc::MatchDoc &matchDoc) override;
    void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    void reset()  override{ /* do nothing */ }
    matchdoc::ReferenceBase* getReferenceBase() const  override{
        return NULL;
    }
    bool needEvaluate() const  override{ return false; }

    template<typename T>
    bool getConstValue(T &value) const;
private:
    ConstExprType _constValueType;
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
bool ArgumentAttributeExpression::getConstValue(T &value) const
{
    static_assert(std::is_arithmetic<T>::value, "not_a_arithmetic_type");
    if (_constValueType != INTEGER_VALUE
        && _constValueType != FLOAT_VALUE)
    {
        return false;
    }
    if (_constValueType == FLOAT_VALUE && !std::is_floating_point<T>::value)
    {
        return false;
    }
    return autil::StringUtil::numberFromString<T>(getExprString(), value);
}

template<> bool ArgumentAttributeExpression::getConstValue<std::string>(std::string &value) const;

}

#endif //ISEARCH_ARGUMENTATTRIBUTEEXPRESSION_H
