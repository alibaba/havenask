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
#ifndef ISEARCH_EXPRESSION_FUNCTIONPROVIDER_H
#define ISEARCH_EXPRESSION_FUNCTIONPROVIDER_H

#include "expression/common.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/function/FunctionResource.h"
#include "expression/common/SessionResourceProvider.h"

namespace expression {

class FunctionProvider : public SessionResourceProvider
{
public:
    FunctionProvider(const FunctionResource &resource);
    ~FunctionProvider();
private:
    FunctionProvider(const FunctionProvider &);
    FunctionProvider& operator=(const FunctionProvider &);
public:
    matchdoc::ReferenceBase* requireAttribute(
            const std::string &name,
            SerializeLevel sl = SL_PROXY);
    bool getExpressionsByType(
            ExpressionType type, std::vector<AttributeExpression*>& exprVec) const
    {
        if (!_attrExprCreator)
        {
            return false;
        }
        _attrExprCreator->getExpressionsByType(type, exprVec);
        return true;
    }
    
protected:
    AttributeExpressionCreator *_attrExprCreator;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_FUNCTIONPROVIDER_H
