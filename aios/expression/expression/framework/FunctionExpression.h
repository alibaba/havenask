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
#ifndef ISEARCH_EXPRESSION_FUNCTIONEXPRESSION_H
#define ISEARCH_EXPRESSION_FUNCTIONEXPRESSION_H

#include "autil/Log.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/function/FunctionInterface.h"

namespace expression {

template<typename T>
class FunctionExpression : public AttributeExpressionTyped<T>
{
public:
    FunctionExpression(const std::string &exprStr, 
                       FunctionInterfaceTyped<T> *function,
                       FuncBatchEvaluateMode batchEvalMode);
    ~FunctionExpression();
private:
    FunctionExpression(const FunctionExpression &);
    FunctionExpression& operator=(const FunctionExpression &);
public:
    /* override */ bool allocate(matchdoc::MatchDocAllocator *allocator,
                                 const std::string &groupName = matchdoc::DEFAULT_GROUP,
                                 uint8_t serializeLevel = SL_PROXY);
    
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc);
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount);
    /* override */ void setIsSubExpression(bool isSub);
    /* override */ void reset() { 
        AttributeExpressionTyped<T>::reset();
        if(_function) {
            _function->setReference(NULL);
        }
    }

private:
    FunctionInterfaceTyped<T> *_function;
    FuncBatchEvaluateMode _batchEvalMode;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////

template<typename T>
inline FunctionExpression<T>::FunctionExpression(
        const std::string &exprStr, FunctionInterfaceTyped<T> *function,
        FuncBatchEvaluateMode batchEvalMode)
    : AttributeExpressionTyped<T>(
            ET_FUNCTION,
            AttrValueType2ExprValueType<T>::evt,
            AttrValueType2ExprValueType<T>::isMulti,
            exprStr)
    , _function(function)
    , _batchEvalMode(batchEvalMode)
{
}

template<typename T>
inline FunctionExpression<T>::~FunctionExpression() {
    if (_function) {
        _function->destroy();
        _function = NULL;
    }
}

template <typename T>
inline bool FunctionExpression<T>::allocate(
        matchdoc::MatchDocAllocator *allocator,
        const std::string &groupName, uint8_t serializeLevel)
{
    if (!AttributeExpressionTyped<T>::allocate(allocator, groupName, serializeLevel)) {
        return false;
    }
    if (_function) {
        _function->setReference(this->_ref);
    }
    return true;
}

template<typename T>
inline void FunctionExpression<T>::evaluate(const matchdoc::MatchDoc &matchDoc) {
    assert(_function);
    T value = _function->evaluate(matchDoc);
    this->storeValue(matchDoc, value);
}

template<typename T>
inline void FunctionExpression<T>::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
    assert(_function);
    if (_batchEvalMode == FUNC_BATCH_CUSTOM_MODE)
    {
        _function->batchEvaluate(matchDocs, docCount);
        return;
    }

    assert(_batchEvalMode == FUNC_BATCH_DEFAULT_MODE);
    for (uint32_t i = 0; i < docCount; i++) {
        const matchdoc::MatchDoc &matchDoc = matchDocs[i];
        T value = _function->evaluate(matchDoc);
        this->storeValue(matchDoc, value);
    }
}

template<typename T>
inline void FunctionExpression<T>::setIsSubExpression(bool isSub) {
    AttributeExpression::setIsSubExpression(isSub);
    if (_function) {
        _function->setSubScope(isSub);
    }
}

template<>
class FunctionExpression<void> : public AttributeExpressionTyped<void>
{
public:
    FunctionExpression(const std::string &exprStr, FunctionInterfaceTyped<void> *function,
                       FuncBatchEvaluateMode batchEvalMode);
    ~FunctionExpression();
private:
    FunctionExpression(const FunctionExpression &);
    FunctionExpression& operator=(const FunctionExpression &);
public:
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc);
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount);
    /* override */ void setIsSubExpression(bool isSub);

private:
    FunctionInterfaceTyped<void> *_function;
    FuncBatchEvaluateMode _batchEvalMode;
private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////
inline void FunctionExpression<void>::setIsSubExpression(bool isSub) {
    AttributeExpression::setIsSubExpression(isSub);
    if (_function) {
        _function->setSubScope(isSub);
    }
}

inline FunctionExpression<void>::FunctionExpression(
        const std::string &exprStr, FunctionInterfaceTyped<void> *function,
        FuncBatchEvaluateMode batchEvalMode)
    : AttributeExpressionTyped<void>(
            ET_FUNCTION,
            AttrValueType2ExprValueType<void>::evt,
            AttrValueType2ExprValueType<void>::isMulti,
            exprStr)
    , _function(function)
    , _batchEvalMode(batchEvalMode)
{
}

inline FunctionExpression<void>::~FunctionExpression() {
    if (_function) {
        _function->destroy();
        _function = NULL;
    }
}

inline void FunctionExpression<void>::evaluate(const matchdoc::MatchDoc &matchDoc) {
    assert(_function);
    _function->evaluate(matchDoc);
}

inline void FunctionExpression<void>::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount) {
    assert(_function);
    if (_batchEvalMode == FUNC_BATCH_CUSTOM_MODE)
    {
        _function->batchEvaluate(matchDocs, docCount);
        return;
    }
    assert(_batchEvalMode == FUNC_BATCH_DEFAULT_MODE);
    for (uint32_t i = 0; i < docCount; i++) {
        _function->evaluate(matchDocs[i]);
    }
}

}
#endif //ISEARCH_EXPRESSION_FUNCTIONEXPRESSION_H
