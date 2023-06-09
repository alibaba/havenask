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
#ifndef ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSION_H
#define ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSION_H

#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "expression/common.h"
#include "expression/framework/TypeInfo.h"
#include "expression/framework/BottomUpEvaluator.h"
#include "expression/framework/AttributeExpressionPool.h"

namespace expression {

class AttributeExpression
{
public:
    AttributeExpression(
            ExpressionType et = ET_UNKNOWN,
            ExprValueType evt = EVT_UNKNOWN,
            bool isMulti = false,
            const std::string &exprStr = "")
        : _et(et)
        , _exprValueType(evt)
        , _isMulti(isMulti)
        , _exprStr(exprStr)
        , _exprPool(NULL)
        , _isSubExpression(false)
    {}
    virtual ~AttributeExpression() {}
private:
    AttributeExpression(const AttributeExpression &);
    AttributeExpression& operator=(const AttributeExpression &);

public:
    void init(const std::vector<expressionid_t>& depExprIds,
              AttributeExpressionPool* exprPool)
    {
        _dependentExprIds.assign(depExprIds.begin(), depExprIds.end());
        _exprPool = exprPool;
    }

    virtual bool allocate(matchdoc::MatchDocAllocator *allocator,
                          const std::string &groupName = matchdoc::DEFAULT_GROUP,
                          uint8_t serializeLevel = SL_PROXY) = 0;
    virtual void evaluate(const matchdoc::MatchDoc &matchDoc) = 0;
    virtual void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) = 0;
    virtual void reset() = 0;
    virtual matchdoc::ReferenceBase* getReferenceBase() const = 0;
    virtual bool needEvaluate() const { return true; }
    virtual void setIsSubExpression(bool isSub) { _isSubExpression = isSub; }
    virtual bool check() const {
        return true;
    }

public:
    // for test
    void bottomUpEvaluate(matchdoc::MatchDoc matchDoc, 
                          matchdoc::MatchDocAllocator *allocator = NULL)
    {
        BottomUpEvaluator evaluator;
        std::vector<AttributeExpression*> depExprs;
        getDependentAttributeExpressions(depExprs);
        matchdoc::SubDocAccessor* subDocAccessor = NULL;
        if (_isSubExpression && allocator) {
            subDocAccessor = allocator->getSubDocAccessor();
        }
        evaluator.init(depExprs, subDocAccessor);
        evaluator.evaluate(matchDoc);
    }

    const std::vector<expressionid_t>& getDependentExpressionIds() const
    { return _dependentExprIds; }

    void getDependentAttributeExpressions(
            std::vector<AttributeExpression*> &depExprs)
    { 
        assert(_exprPool);
        for (size_t i = 0; i < _dependentExprIds.size(); i++)
        {
            depExprs.push_back(
                    _exprPool->tryGetAttributeExpression(_dependentExprIds[i]));
        }
    }

    expressionid_t getAttributeExpressionId() const
    { 
        if (_dependentExprIds.empty())
        {
            return INVALID_EXPRESSION_ID;
        }
        return *_dependentExprIds.rbegin();
    }

    const std::string& getExprString() const { return _exprStr; }
    ExpressionType getType() const { return _et; }
    ExprValueType getExprValueType() const { return _exprValueType; }
    bool isMulti() const { return _isMulti; }
    bool isSubExpression() const { return _isSubExpression; }

protected:
    ExpressionType _et;
    ExprValueType _exprValueType;
    bool _isMulti;
    std::string _exprStr;
    std::vector<expressionid_t> _dependentExprIds;
    AttributeExpressionPool* _exprPool;
    bool _isSubExpression;
};

typedef std::vector<AttributeExpression*> AttributeExpressionVec;

}

#endif //ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSION_H
