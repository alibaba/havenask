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
#ifndef ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONPOOL_H
#define ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONPOOL_H

#include "expression/common.h"
#include <tr1/unordered_map>
#include <tr1/unordered_set>

namespace expression {

class AttributeExpression;

class AttributeExpressionPool
{
public:
    AttributeExpressionPool();
    ~AttributeExpressionPool();
private:
    AttributeExpressionPool(const AttributeExpressionPool &);
    AttributeExpressionPool& operator=(const AttributeExpressionPool &);
public:
    inline AttributeExpression* tryGetAttributeExpression(
            const std::string &exprStr) const
    {
        std::tr1::unordered_map<std::string, AttributeExpression *>::const_iterator it =
            _exprMap.find(exprStr);
        if (it != _exprMap.end()) {
            return it->second;
        }
        return NULL;
    }

    inline void addPair(const std::string &exprStr,
                        AttributeExpression *attrExpr)
    {
        _exprMap[exprStr] = attrExpr;
        addNeedDeleteExpr(attrExpr);
    }

    inline AttributeExpression* tryGetAttributeExpression(
            expressionid_t exprId) const
    {
        if ((size_t)exprId >= _exprVec.size())
        {
            return NULL;
        }
        return _exprVec[exprId];
    }

    size_t getAttributeExpressionCount() const
    { return _exprVec.size(); }

    void addNeedDeleteExpr(AttributeExpression *attrExpr);
    
    const std::vector<AttributeExpression*>& getAllExpressions() const {
        return _exprVec;
    }
    
private:
    std::vector<AttributeExpression*> _exprVec;
    std::tr1::unordered_set<AttributeExpression *> _exprSet;
    std::tr1::unordered_map<std::string, AttributeExpression *> _exprMap;
private:
    AUTIL_LOG_DECLARE();
};

}
#endif //ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONPOOL_H
