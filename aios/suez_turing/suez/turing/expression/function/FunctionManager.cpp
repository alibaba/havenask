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
#include "suez/turing/expression/function/FunctionManager.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/CavaFunctionExpression.h"
#include "suez/turing/expression/framework/FunctionExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FunctionManager);

FunctionManager::FunctionManager(const FunctionInterfaceCreator *funcCreator,
                                 FunctionProvider *provider,
                                 autil::mem_pool::Pool *pool,
                                 const suez::turing::CavaPluginManager *cavaPluginManager)
    : _funcCreator(funcCreator), _provider(provider), _pool(pool), _cavaPluginManager(cavaPluginManager) {}

FunctionManager::~FunctionManager() {}

AttributeExpression *FunctionManager::createCavaFuncExpression(const std::string &funcName,
                                                               const FunctionSubExprVec &funcSubExprVec,
                                                               VariableType vt,
                                                               bool isMulti,
                                                               bool isSubExpr,
                                                               bool funcIsDeterministic) {
    if (!_cavaPluginManager) {
        return NULL;
    }
    std::string processFuncName = funcName + CAVA_INNER_METHOD_SEP + CAVA_INNER_METHOD_NAME; //"_inner_process"
    std::string processFuncNameIgnoreConstArg = processFuncName;

    for (auto it = funcSubExprVec.begin(); it != funcSubExprVec.end(); ++it) {

        VariableType type = (*it)->getType();
        bool isMulti = (*it)->isMultiValue();

        std::string cavaTypeName;

#define TYPE_CASE_HELPER(type)                                                                                         \
    case type:                                                                                                         \
        if (isMulti) {                                                                                                 \
            typedef matchdoc::MatchDocBuiltinType2CppType<type, true>::CppType cppType;                                \
            cavaTypeName = ha3::CppType2CavaTypeName<cppType>();                                                       \
        } else {                                                                                                       \
            typedef matchdoc::MatchDocBuiltinType2CppType<type, false>::CppType cppType;                               \
            cavaTypeName = ha3::CppType2CavaTypeName<cppType>();                                                       \
        }                                                                                                              \
        break;
        switch (type) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(TYPE_CASE_HELPER);
        default:
            AUTIL_LOG(ERROR, "impossible to reach this branch");
            return NULL;
        }

        processFuncName += CAVA_INNER_METHOD_SEP + cavaTypeName;
        if (suez::turing::ET_ARGUMENT != (*it)->getExpressionType()) {
            processFuncNameIgnoreConstArg += CAVA_INNER_METHOD_SEP + cavaTypeName;
        }
    }

    if (_provider) {
        _provider->setSubScope(isSubExpr);
    }

    bool prefetchConst = false;
    AttributeExpression *attrExpr = NULL;
    CavaFunctionModuleInfo *moduleInfo =
        dynamic_cast<CavaFunctionModuleInfo *>(_cavaPluginManager->getCavaModuleInfo(processFuncName).get());
    if (moduleInfo == NULL) {
        moduleInfo = dynamic_cast<CavaFunctionModuleInfo *>(
            _cavaPluginManager->getCavaModuleInfo(processFuncNameIgnoreConstArg).get());
        if (moduleInfo == NULL) {
            AUTIL_LOG(ERROR,
                      "Neither module [%s] nor module [%s] is a function module!!",
                      processFuncName.c_str(),
                      processFuncNameIgnoreConstArg.c_str());
            return NULL;
        }
        prefetchConst = true;
    }
    funcIsDeterministic &= moduleInfo->functionProto->isDeterministic;
    if (vt != moduleInfo->functionProto->resultType || isMulti != moduleInfo->functionProto->isMulti) {
        AUTIL_LOG(ERROR, "module [%s] return type is error !!", processFuncName.c_str());
        return NULL;
    }
    attrExpr = doCreateCavaFuncExpression(funcName, funcSubExprVec, vt, isMulti, moduleInfo, prefetchConst);
    if (!attrExpr) {
        return nullptr;
    }
    attrExpr->andIsDeterministic(funcIsDeterministic);
    if (_provider) {
        _provider->unsetSubScope();
    }
    return attrExpr;
}

AttributeExpression *FunctionManager::doCreateCavaFuncExpression(const std::string &funcName,
                                                                 const FunctionSubExprVec &funcSubExprVec,
                                                                 VariableType vt,
                                                                 bool isMulti,
                                                                 CavaFunctionModuleInfo *moduleInfo,
                                                                 bool prefetchConst) {
#define CREATE_TYPED_FUNCTION_EXPRESSION(vt_type)                                                                      \
    {                                                                                                                  \
    case vt_type:                                                                                                      \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                 \
            CavaFunctionExpression<T> *funcExpr =                                                                      \
                POOL_NEW_CLASS(_pool, CavaFunctionExpression<T>, _cavaPluginManager);                                  \
            if (!funcExpr->init(moduleInfo, _provider, funcSubExprVec, prefetchConst)) {                               \
                AUTIL_LOG(WARN,                                                                                        \
                          "init cava function expression failed"                                                       \
                          ", funcName: [%s]",                                                                          \
                          funcName.c_str());                                                                           \
                POOL_DELETE_CLASS(funcExpr);                                                                           \
                return NULL;                                                                                           \
            }                                                                                                          \
            return funcExpr;                                                                                           \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                \
            CavaFunctionExpression<T> *funcExpr =                                                                      \
                POOL_NEW_CLASS(_pool, CavaFunctionExpression<T>, _cavaPluginManager);                                  \
            if (!funcExpr->init(moduleInfo, _provider, funcSubExprVec, prefetchConst)) {                               \
                AUTIL_LOG(WARN,                                                                                        \
                          "init cava function expression failed"                                                       \
                          ", funcName: [%s]",                                                                          \
                          funcName.c_str());                                                                           \
                POOL_DELETE_CLASS(funcExpr);                                                                           \
                return NULL;                                                                                           \
            }                                                                                                          \
            return funcExpr;                                                                                           \
        }                                                                                                              \
    }

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(CREATE_TYPED_FUNCTION_EXPRESSION);
    default:
        AUTIL_LOG(WARN, "unknown cava function type[%d].", vt);
    }
#undef CREATE_TYPED_FUNCTION_EXPRESSION
    return NULL;
}

AttributeExpression *FunctionManager::createFuncExpression(
    const string &funcName, const FunctionSubExprVec &funcSubExprVec, VariableType vt, bool isMulti, bool isSubExpr) {
    assert(_funcCreator);
    bool funcIsDeterministic = true;
    for (auto *funcSubExpr : funcSubExprVec) {
        funcIsDeterministic &= funcSubExpr->isDeterministic();
    }
    FunctionInterface *func = _funcCreator->createFuncExpression(funcName, funcSubExprVec);
    if (func == NULL) {
        AUTIL_LOG(DEBUG, "not find function [%s] in plugins, try cava function", funcName.c_str());
        AttributeExpression *exprssion =
            createCavaFuncExpression(funcName, funcSubExprVec, vt, isMulti, isSubExpr, funcIsDeterministic);
        if (exprssion == NULL) {
            AUTIL_LOG(WARN,
                      "not find function both in cava and plugins, "
                      "create function[%s] failed.",
                      funcName.c_str());
        }
        return exprssion;
    }

    const FuncInfoMap &funcInfoMap = _funcCreator->getFuncInfoMap();
    auto iter = funcInfoMap.find(funcName);
    if (iter != funcInfoMap.end()) {
        funcIsDeterministic &= iter->second.isDeterministic;
    } else {
        AUTIL_LOG(ERROR, "unexpected, can not find func[%s]'s function info.", funcName.c_str());
        return nullptr;
    }
    //  for test, maybe NULL
    if (_provider) {
        _provider->setSubScope(isSubExpr);
    }
    if (!func->beginRequest(_provider)) {
        func->destroy();
        AUTIL_LOG(WARN, "Function interface beginRequest failed.");
        return NULL;
    }
    if (_provider) {
        _provider->unsetSubScope();
    }

    AttributeExpression *attrExpr = NULL;

#define CREATE_TYPED_FUNCTION_EXPRESSION(vt_type)                                                                      \
    {                                                                                                                  \
    case vt_type:                                                                                                      \
        if (isMulti) {                                                                                                 \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;                                                 \
            attrExpr = createAttrExpression<T>(func);                                                                  \
            break;                                                                                                     \
        } else {                                                                                                       \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T;                                                \
            attrExpr = createAttrExpression<T>(func);                                                                  \
            break;                                                                                                     \
        }                                                                                                              \
    }

    switch (vt) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL_AND_STRING(CREATE_TYPED_FUNCTION_EXPRESSION);
    default:
        AUTIL_LOG(WARN, "unknown function type[%d].", vt);
    }
#undef CREATE_TYPED_FUNCTION_EXPRESSION

    if (!attrExpr) {
        AUTIL_LOG(WARN, "create function expression failed, funcName: [%s]", funcName.c_str());
        func->destroy();
    } else {
        attrExpr->andIsDeterministic(funcIsDeterministic);
    }
    return attrExpr;
}

template <typename T>
AttributeExpression *FunctionManager::createAttrExpression(FunctionInterface *func) const {
    FunctionInterfaceTyped<T> *typedFunc = dynamic_cast<FunctionInterfaceTyped<T> *>(func);
    if (!typedFunc) {
        AUTIL_LOG(WARN, "unexpected return type.");
        return NULL;
    }
    return POOL_NEW_CLASS(_pool, FunctionExpression<T>, typedFunc);
}

} // namespace turing
} // namespace suez
