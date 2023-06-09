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
#include "expression/framework/FunctionExpressionCreator.h"
#include "expression/framework/FunctionExpression.h"
#include "expression/function/FunctionProvider.h"
#include "resource_reader/ResourceReader.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
using namespace resource_reader;

namespace expression {
AUTIL_LOG_SETUP(expression, FunctionExpressionCreator);

FunctionExpressionCreator::FunctionExpressionCreator(
        autil::mem_pool::Pool *pool,
        FunctionInterfaceManager *funcManager,
        AttributeExpressionCreator *owner,
        bool useSub)
    : _pool(pool)
    , _funcManager(funcManager)
    , _owner(owner)
    , _useSub(useSub)
    , _errorCode(ERROR_NONE)
{ 
}

FunctionExpressionCreator::~FunctionExpressionCreator() { 
    _functions.clear();
    _owner = NULL;
    _funcManager = NULL;
}

void FunctionExpressionCreator::reset()
{
    _functions.clear();
}

AttributeExpression* FunctionExpressionCreator::createFunctionExpression(
        const string &exprStr, const string &funcName,
        const AttributeExpressionVec &funcSubExprVec,
        AttributeExpressionVec &dependentExprs)
{
    ExprValueType evt = EVT_UNKNOWN;
    bool isMulti = false;
    bool isSub = false;
    FuncBatchEvaluateMode batchEvalMode = FUNC_BATCH_DEFAULT_MODE;
    if (!deduceFunctionTypeInfo(funcName, funcSubExprVec, evt, 
                                isMulti, isSub, batchEvalMode)) 
    {
        return NULL;
    }

    if (!pushDependentExprLayer(funcName, funcSubExprVec)) {
        return NULL;
    }
    
    FunctionInterface* func = _funcManager->createFunctionInterface(
            funcName, funcSubExprVec, _owner);
    if (func == NULL) {
        AUTIL_LOG(WARN, "create function[%s] failed.", funcName.c_str());
        _errorCode = ERROR_CREATE_FUNCTION_FAIL;
        _errorMsg = "create function[" + funcName + "] failed.";
        popDependentExprLayer(dependentExprs);
        return NULL;
    }
    AttributeExpression *attrExpr = NULL;

#define CREATE_TYPED_FUNCTION_EXPRESSION(evt_type)                      \
    {                                                                   \
    case evt_type:                                                      \
        if (isMulti) {                                                  \
            typedef ExprValueType2AttrValueType<evt_type, true>::AttrValueType T;  \
            attrExpr = createAttrExpression<T>(exprStr, func, batchEvalMode); \
            break;                                                      \
        } else {                                                        \
            typedef ExprValueType2AttrValueType<evt_type, false>::AttrValueType T; \
            attrExpr = createAttrExpression<T>(exprStr, func, batchEvalMode); \
            break;                                                      \
        }                                                               \
    }

    switch (evt) {
        NUMERIC_EXPR_VALUE_TYPE_MACRO_WITH_STRING_HELPER(CREATE_TYPED_FUNCTION_EXPRESSION);
    case EVT_BOOL:
        if (isMulti) {
            AUTIL_LOG(WARN, "mutli bool is not supported");
        } else {
            typedef ExprValueType2AttrValueType<EVT_BOOL, false>::AttrValueType T;
            attrExpr = createAttrExpression<T>(exprStr, func, batchEvalMode);
        }
        break;
    case EVT_VOID:
        attrExpr = createAttrExpression<void>(exprStr, func, batchEvalMode);
        break;
    default:
        AUTIL_LOG(WARN, "unknown function type[%d].", (int32_t)evt);
    }
#undef CREATE_TYPED_FUNCTION_EXPRESSION

    if (!attrExpr) {
        AUTIL_LOG(WARN, "create function expression failed, funcName: [%s]", funcName.c_str());
        func->destroy();
    } else {
        attrExpr->setIsSubExpression(isSub);
        _functions.push_back(make_pair(funcName, func));
    }

    popDependentExprLayer(dependentExprs);
    return attrExpr;
}

bool FunctionExpressionCreator::deduceFunctionTypeInfo(
        const string &funcName, const AttributeExpressionVec &funcSubExprVec,
        ExprValueType &evt, bool &isMulti, bool &isSub, 
        FuncBatchEvaluateMode &batchEvalMode)
{
    assert(_funcManager);
    FunctionProtoInfo funcProto;
    if (!_funcManager->getFunctionProtoInfo(funcName, funcProto)) {
        _errorCode = ERROR_FUNCTION_NOT_DEFINED;
        _errorMsg = "can not find protoinfo for function [" + funcName + "]";
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }
    if (funcProto.argCount != FUNCTION_UNLIMITED_ARGUMENT_COUNT
        && funcProto.argCount != funcSubExprVec.size())
    {
        _errorCode = ERROR_FUNCTION_ARGCOUNT;
        _errorMsg = "Invalid argument count for :" + funcName
                    + ",expected count:" + StringUtil::toString(funcProto.argCount) 
                    + ",actual count:"  + StringUtil::toString(funcSubExprVec.size());
        AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
        return false;
    }

    isMulti = funcProto.isMulti;
    evt = funcProto.resultType;
    batchEvalMode = funcProto.batchEvaluateMode;
    if (funcProto.funcType == FT_TEMPLATE || funcProto.funcType == FT_TEMPLATE_RESULT_TYPE) {
        if (funcSubExprVec.size() == 0) {
            AUTIL_LOG(WARN, "template function[%s] has no argument", funcName.c_str());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            _errorMsg = "template function[" + funcName + "] has no argument";
            return false;
        }

        uint32_t typeDeduceArgIdx = funcProto.typeDeduceArgmentIdx;
        AttributeExpression *typeDeduceExpr = GetTypeDeduceArgumentExpression(
                funcSubExprVec, typeDeduceArgIdx);
        if (!typeDeduceExpr)
        {
            AUTIL_LOG(WARN, "template function[%s] deduce type failed, deduceIdx[%u], argCount[%lu]!",
                      funcName.c_str(), typeDeduceArgIdx, funcSubExprVec.size());
            _errorCode = ERROR_FUNCTION_ARGCOUNT;
            _errorMsg = "template function[" + funcName + "] deduce type failed! "
                        + "deduceIdx[" + StringUtil::toString(typeDeduceArgIdx) + "], "
                        + "argCount[" + StringUtil::toString(funcSubExprVec.size()) + "]!";
            return false;
        }
        
        if (funcProto.funcType == FT_TEMPLATE) {
            evt = typeDeduceExpr->getExprValueType();
            isMulti = typeDeduceExpr->isMulti();
        } else {
            evt = typeDeduceExpr->getExprValueType();
        }
    }
    if (evt == EVT_UNKNOWN) {
        return false;
    }
    FuncActionScopeType scopeType = funcProto.scopeType;
    if (scopeType == FUNC_ACTION_SCOPE_ADAPTER) {
        scopeType = FUNC_ACTION_SCOPE_MAIN_DOC;
        for (size_t i = 0 ; i < funcSubExprVec.size(); i++) {
            if(funcSubExprVec[i]->isSubExpression()) {
                scopeType = FUNC_ACTION_SCOPE_SUB_DOC;
                break;
            }
        }
    }
    if (scopeType == FUNC_ACTION_SCOPE_SUB_DOC) {
        if (!_useSub) {
            _errorCode = ERROR_SUB_DOC_NOT_SUPPORT;
            _errorMsg = "use sub scope function[" + funcName + "] "
                        "when expression creator not use sub";
            AUTIL_LOG(WARN, "%s", _errorMsg.c_str());
            return false;
        }
        isSub = true;
    }
    else {
        isSub = false;
    }
    return true;
}

template<typename T>
AttributeExpression* FunctionExpressionCreator::createAttrExpression(
        const string &exprStr, FunctionInterface* func,
        FuncBatchEvaluateMode batchEvalMode) const
{
    FunctionInterfaceTyped<T> *typedFunc = 
        dynamic_cast<FunctionInterfaceTyped<T>*>(func);
    if (!typedFunc) {
        AUTIL_LOG(WARN, "unexpected return type.");
        return NULL;
    }
    return POOL_NEW_CLASS(_pool, FunctionExpression<T>, 
                          exprStr, typedFunc, batchEvalMode);
}

bool FunctionExpressionCreator::beginRequest(const FunctionResource &resource) {
    if (_functions.size() == 0) {
        return true;
    }

    FunctionProvider provider(resource);
    for (size_t i = 0; i < _functions.size(); i++) {
        provider.setSubScope(_functions[i].second->isSubScope());
        if (!_functions[i].second->beginRequest(&provider)) {
            AUTIL_LOG(WARN, "function[%s] begin request failed",
                      _functions[i].first.c_str());
            return false;
        }
        provider.unsetSubScope();
    }
    return true;
}

void FunctionExpressionCreator::endRequest() {
    for (size_t i = 0; i < _functions.size(); i++) {
        _functions[i].second->endRequest();
    }
}

AttributeExpression* FunctionExpressionCreator::GetTypeDeduceArgumentExpression(
        const AttributeExpressionVec& exprs, uint32_t typeDeduceArgIdx)
{
    if (exprs.empty())
    {
        return NULL;
    }

    // deduce by last argment
    if (typeDeduceArgIdx == TYPE_DEDUCE_BY_LAST_ARGUMENT)
    {
        return *exprs.rbegin();
    }

    if ((size_t)typeDeduceArgIdx >= exprs.size())
    {
        return NULL;
    }
    return exprs[typeDeduceArgIdx];
}


}
