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
#ifndef ISEARCH_EXPRESSION_FUNCTIONEXPRESSIONCREATOR_H
#define ISEARCH_EXPRESSION_FUNCTIONEXPRESSIONCREATOR_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "expression/function/FunctionInterfaceManager.h"
#include "expression/function/FunctionResource.h"
#include "expression/common/ErrorDefine.h"
#include "autil/mem_pool/Pool.h"

DECLARE_RESOURCE_READER

namespace expression {

class AttributeExpressionCreator;

class FunctionExpressionCreator
{
public:
    FunctionExpressionCreator(autil::mem_pool::Pool *pool,
                              FunctionInterfaceManager *funcManager,
                              AttributeExpressionCreator *owner,
                              bool useSub);
    ~FunctionExpressionCreator();
private:
    FunctionExpressionCreator(const FunctionExpressionCreator &);
    FunctionExpressionCreator& operator=(const FunctionExpressionCreator &);
public:
    AttributeExpression* createFunctionExpression(
            const std::string &exprName,
            const std::string &funcName, 
            const AttributeExpressionVec &funcSubExprVec,
            AttributeExpressionVec &dependentExprVec);

    bool beginRequest(const FunctionResource &resource);

    void endRequest();

    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }

    void reset();

    bool needCollectDependentExpression() const {
        return !_dependentInfoVec.empty();
    }
    
    void addDependentExpression(AttributeExpression* expr)  {
        if (needCollectDependentExpression())  {
            _dependentInfoVec.rbegin()->second.push_back(expr);
        }
    }
    
private:
    typedef std::pair<std::string, FunctionInterface*> FunctionPair;
    typedef std::pair<std::string, AttributeExpressionVec> FunctionDependentInfo;
    
private:
    bool deduceFunctionTypeInfo(const std::string &funcName,
                                const AttributeExpressionVec &funcSubExprVec,
                                ExprValueType &evt, bool &isMulti, bool &isSub, 
                                FuncBatchEvaluateMode &batchEvalMode);
    template<typename T>
    AttributeExpression* createAttrExpression(
            const std::string &exprStr, FunctionInterface *func,
            FuncBatchEvaluateMode batchEvalMode) const;

    bool pushDependentExprLayer(const std::string& funcName,
                                const AttributeExpressionVec& exprs)
    {
        std::vector<FunctionDependentInfo>::const_iterator it =
            _dependentInfoVec.begin();
        for (; it != _dependentInfoVec.end(); ++it) {
            if (funcName == it->first) {
                AUTIL_LOG(ERROR, "loop dependency detected in function [%s]",
                        funcName.c_str());
                return false;
            }
        }
        _dependentInfoVec.push_back(make_pair(funcName, exprs));
        return true;
    }

    void popDependentExprLayer(AttributeExpressionVec& exprs) {
        if (needCollectDependentExpression()) {
            exprs.assign(_dependentInfoVec.rbegin()->second.begin(),
                         _dependentInfoVec.rbegin()->second.end());
            _dependentInfoVec.pop_back();
        }
    }

    AttributeExpression* GetTypeDeduceArgumentExpression(
            const AttributeExpressionVec& exprs, uint32_t typeDeduceArgIdx);

private:
    autil::mem_pool::Pool *_pool;
    FunctionInterfaceManager *_funcManager;
    AttributeExpressionCreator *_owner;
    std::vector<FunctionPair> _functions;
    bool _useSub;
    ErrorCode _errorCode;
    std::string _errorMsg;
    std::vector<FunctionDependentInfo> _dependentInfoVec;
    
private:
    AUTIL_LOG_DECLARE();
};

}
#endif //ISEARCH_EXPRESSION_FUNCTIONEXPRESSIONCREATOR_H
