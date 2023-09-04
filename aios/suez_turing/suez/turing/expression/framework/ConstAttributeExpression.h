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

#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace suez {
namespace turing {

template <typename T>
class ConstAttributeExpression : public AttributeExpressionTyped<T> {
public:
    ConstAttributeExpression(const T &constValue) {
        this->setValue(constValue);
        this->setEvaluated();
    }
    virtual ~ConstAttributeExpression() {}

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        this->storeValue(matchDoc, this->_value);
        return true;
    }
    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        this->storeValue(matchDoc, this->_value);
        return this->_value;
    }
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count) override {
        if (this->getReference()) {
            for (size_t i = 0; i < count; i++) {
                this->storeValue(matchDocs[i], this->_value);
            }
        }
        return true;
    }
    void setEvaluated() override { this->_isEvaluated = true; }
    ExpressionType getExpressionType() const override { return ET_CONST; }
    bool operator==(const AttributeExpression *checkExpr) const override;
};

template <typename T>
inline bool ConstAttributeExpression<T>::operator==(const AttributeExpression *checkExpr) const {
    assert(checkExpr);
    const ConstAttributeExpression<T> *expr = dynamic_cast<const ConstAttributeExpression<T> *>(checkExpr);

    if (expr && this->_value == expr->_value && this->getType() == expr->getType()) {
        return true;
    }
    return false;
}

} // namespace turing
} // namespace suez
