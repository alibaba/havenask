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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace suez {
namespace turing {
template <typename T>
class FunctionInterfaceTyped;

template <typename T>
class FunctionExpression : public AttributeExpressionTyped<T> {
public:
    FunctionExpression(FunctionInterfaceTyped<T> *function);
    ~FunctionExpression();

private:
    FunctionExpression(const FunctionExpression &);
    FunctionExpression &operator=(const FunctionExpression &);

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    ExpressionType getExpressionType() const override { return ET_FUNCTION; }
    void setEvaluated() override {
        AttributeExpressionTyped<T>::setEvaluated();
        _function->setExpressionEvaluated();
    }

private:
    FunctionInterfaceTyped<T> *_function;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////

template <typename T>
FunctionExpression<T>::FunctionExpression(FunctionInterfaceTyped<T> *function) {
    _function = function;
}

template <typename T>
FunctionExpression<T>::~FunctionExpression() {
    if (_function) {
        _function->endRequest();
        _function->destroy();
        _function = NULL;
    }
}

template <typename T>
bool FunctionExpression<T>::evaluate(matchdoc::MatchDoc matchDoc) {
    T value = FunctionExpression<T>::evaluateAndReturn(matchDoc);
    this->setValue(value);
    return true;
}

template <typename T>
inline T FunctionExpression<T>::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(_function);

    T value = T();
    if (!this->tryFetchValue(matchDoc, value)) {
        value = _function->evaluate(matchDoc);
        this->storeValue(matchDoc, value);
    }
    return value;
}

template <typename T>
bool FunctionExpression<T>::batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
    assert(_function);
    auto reference = this->getReference();
    if (!reference) {
        return false;
    }
    if (this->isEvaluated()) {
        return true;
    }
    for (uint32_t i = 0; i < matchDocCount; ++i) {
        matchdoc::MatchDoc matchDoc = matchDocs[i];
        T value = _function->evaluate(matchDoc);
        this->storeValue(matchDoc, value);
    }
    return true;
}

} // namespace turing
} // namespace suez
