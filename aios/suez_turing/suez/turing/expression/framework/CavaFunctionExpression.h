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
#pragma once

#include "cava/common/ErrorDef.h"
#include "cava/runtime/CavaCtx.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/CavaFunctionModuleInfo.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/cava/impl/ExpressionVector.h"
#include "suez/turing/expression/cava/impl/FunctionProvider.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/ArgumentAttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/syntax/FuncSyntaxExpr.h"

namespace suez {
namespace turing {

template <typename T>
class CavaFunctionExpression : public AttributeExpressionTyped<T> {
public:
    typedef typename ha3::CppType2CavaType<T>::CavaType CavaType;
    typedef CavaType (*ProcessProtoType)(void *, CavaCtx *, ::ha3::MatchDoc *, ::unsafe::ExpressionVector *);

    CavaFunctionExpression(const CavaPluginManager *cavaPluginManager);
    ~CavaFunctionExpression();

private:
    CavaFunctionExpression(const CavaFunctionExpression &);
    CavaFunctionExpression &operator=(const CavaFunctionExpression &);

public:
    bool init(CavaFunctionModuleInfo *moduleInfo,
              suez::turing::FunctionProvider *provider,
              const FunctionSubExprVec &subExprVec,
              bool prefetchConst);
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    ExpressionType getExpressionType() const override { return ET_FUNCTION; }
    void setEvaluated() override {
        AttributeExpressionTyped<T>::setEvaluated();
        for (auto *subExpr : _subAttrExprVec) {
            subExpr->setEvaluated();
        }
    }

protected:
    inline bool callCavaEvaluate(matchdoc::MatchDoc matchDoc, T &value);

protected:
    const CavaPluginManager *_cavaPluginManager = nullptr;
    suez::turing::Tracer *_tracer = nullptr;
    matchdoc::Reference<suez::turing::Tracer> *_traceRefer = nullptr;
    bool _initSuccessFlag;
    FunctionSubExprVec _subAttrExprVec;
    std::vector<std::string> _argsStrVec;
    CavaFunctionModuleInfo *_moduleInfo = nullptr;
    ha3::FunctionProvider *_provider = nullptr;
    suez::turing::FunctionProvider *_funProvider = nullptr;
    CavaCtx *_cavaCtx = nullptr;
    bool _hasCavaException = false;

    ProcessProtoType _processFunc;
    void *_functionObj = nullptr;
    unsafe::ExpressionVector *_expressionVector = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////

template <typename T>
CavaFunctionExpression<T>::CavaFunctionExpression(const CavaPluginManager *cavaPluginManager)
    : _cavaPluginManager(cavaPluginManager)
    , _tracer(NULL)
    , _traceRefer(NULL)
    , _initSuccessFlag(true)
    , _moduleInfo(NULL)
    , _provider(NULL)
    , _funProvider(NULL)
    , _cavaCtx(NULL)
    , _functionObj(NULL)
    , _expressionVector(NULL) {}

template <typename T>
CavaFunctionExpression<T>::~CavaFunctionExpression() {
    DELETE_AND_SET_NULL(_provider);
    DELETE_AND_SET_NULL(_expressionVector);
    DELETE_AND_SET_NULL(_cavaCtx);
}

template <typename T>
bool CavaFunctionExpression<T>::init(CavaFunctionModuleInfo *moduleInfo,
                                     suez::turing::FunctionProvider *provider,
                                     const FunctionSubExprVec &subExprVec,
                                     bool prefetchConst) {
    _cavaPluginManager->reportFunctionPlugin();
    if (unlikely(!provider)) {
        REQUEST_TRACE(ERROR, "function provider is null");
        _initSuccessFlag = false;
        return true;
    }
    _funProvider = provider;
    _tracer = provider->getRequestTracer();
    _traceRefer = provider->getTracerRefer();

    if (moduleInfo == NULL || moduleInfo->initFunc == NULL || moduleInfo->processAddress == 0 ||
        moduleInfo->createFunc == NULL) {
        REQUEST_TRACE(ERROR, "function module error");
        _initSuccessFlag = false;
        return true;
    }

    SuezCavaAllocator *cavaAllocator = provider->getCavaAllocator();
    if (unlikely(!cavaAllocator)) {
        REQUEST_TRACE(ERROR, "cava allocator is null");
        _initSuccessFlag = false;
        return true;
    }
    _moduleInfo = moduleInfo;
    _cavaCtx = new CavaCtx(cavaAllocator);

    _functionObj = _moduleInfo->createFunc(_cavaCtx);
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        REQUEST_TRACE(ERROR, "function create exception!, proto name : [%s]", _moduleInfo->protoName.c_str());
        _cavaCtx->reset();
        _initSuccessFlag = false;
        _hasCavaException = true;
        _cavaPluginManager->reportFunctionException();
        return true;
    }
    if (_functionObj == NULL) {
        REQUEST_TRACE(ERROR, "create function object failed!, proto name : [%s]", _moduleInfo->protoName.c_str());
        _cavaCtx->reset();
        _initSuccessFlag = false;
        return true;
    }

    for (size_t i = 0; i < subExprVec.size(); i++) {
        if (subExprVec[i]->getExpressionType() == suez::turing::ET_ARGUMENT && prefetchConst) {
            auto argExpr = subExprVec[i];
            VariableType type = argExpr->getType();
            std::string args;
            bool ret = true;
            switch (type) {
            case vt_int64:
            case vt_double:
            case vt_int32:
            case vt_int16:
            case vt_int8:
            case vt_uint64:
            case vt_uint32:
            case vt_uint16:
            case vt_uint8:
            case vt_float: {
                args = argExpr->getOriginalString();
            } break;
            case vt_string: {
                ret = argExpr->getConstValue<std::string>(args);
            } break;
            default: {
                ret = false;
            } break;
            }

            if (!ret) {
                REQUEST_TRACE(WARN, "error argument :%s", argExpr->getOriginalString().c_str());
                _initSuccessFlag = false;
                return true;
            }

            _argsStrVec.emplace_back(args);
        } else {
            _subAttrExprVec.push_back(subExprVec[i]);
        }
    }

    _provider = new ha3::FunctionProvider(provider, _moduleInfo->params, _argsStrVec);
    bool ret = _moduleInfo->initFunc(_functionObj, _cavaCtx, _provider);
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        REQUEST_TRACE(ERROR,
                      "function [%s] init exception\n:%s",
                      _moduleInfo->protoName.c_str(),
                      _cavaCtx->getStackInfo().c_str());
        _cavaCtx->reset();
        _initSuccessFlag = false;
        _hasCavaException = true;
        _cavaPluginManager->reportFunctionException();
        return true;
    }
    if (unlikely(!ret)) {
        REQUEST_TRACE(ERROR, "function init failed!, proto name : [%s]", _moduleInfo->protoName.c_str());
        _initSuccessFlag = false;
        return true;
    }

    _processFunc = (ProcessProtoType)_moduleInfo->processAddress;
    _expressionVector = new unsafe::ExpressionVector(&_subAttrExprVec);
    return true;
}

template <typename T>
bool CavaFunctionExpression<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    T value = CavaFunctionExpression<T>::evaluateAndReturn(matchDoc);
    this->setValue(value);
    return true;
}

template <typename T>
inline bool CavaFunctionExpression<T>::callCavaEvaluate(matchdoc::MatchDoc matchDoc, T &value) {
    if (!_initSuccessFlag || _hasCavaException) {
        return false;
    }
    _funProvider->prepareTracer(matchDoc);
    auto ret = _processFunc(_functionObj, _cavaCtx, (ha3::MatchDoc *)&matchDoc, _expressionVector);
    if (unlikely(_cavaCtx->exception != cava::ExceptionCode::EXC_NONE)) {
        REQUEST_TRACE(ERROR,
                      "call [%s]'s process exception\n%s, with matchDoc[docid=%d, index=%d]",
                      _moduleInfo->protoName.c_str(),
                      _cavaCtx->getStackInfo().c_str(),
                      matchDoc.getDocId(),
                      matchDoc.getIndex());
        _cavaCtx->reset();
        _hasCavaException = true;
        _cavaPluginManager->reportFunctionException();
        return false;
    }
    if (!ha3::TransCavaValue2CppValue<CavaType, T>(ret, value)) {
        return false;
    }
    return true;
}

template <typename T>
inline T CavaFunctionExpression<T>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    T value = T();

    if (!this->tryFetchValue(matchDoc, value)) {
        callCavaEvaluate(matchDoc, value);
        this->storeValue(matchDoc, value);
    }
    return value;
}

template <typename T>
bool CavaFunctionExpression<T>::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
    auto reference = this->getReference();
    if (!reference) {
        return false;
    }
    if (this->isEvaluated()) {
        return true;
    }
    for (uint32_t i = 0; i < matchDocCount; ++i) {
        matchdoc::MatchDoc matchDoc = matchDocs[i];
        T value = T();
        callCavaEvaluate(matchDoc, value);
        this->storeValue(matchDoc, value);
    }
    return true;
}

} // namespace turing
} // namespace suez
