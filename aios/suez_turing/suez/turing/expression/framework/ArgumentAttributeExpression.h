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
#include <stdint.h>
#include <string>
#include <tr1/type_traits>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"

namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
} // namespace matchdoc

namespace suez {
namespace turing {

#define GET_EXPR_IF_ARGUMENT(expr) (suez::turing::ET_ARGUMENT == (expr)->getExpressionType() ? expr : nullptr)

template <typename T>
class ArgumentAttributeExpressionTyped : public AttributeExpressionTyped<T> {
public:
    ArgumentAttributeExpressionTyped(const T &constValue) {
        this->setValue(constValue);
        this->setEvaluated();
    }
    ~ArgumentAttributeExpressionTyped() {}

public:
    ExpressionType getExpressionType() const override { return ET_ARGUMENT; }

    bool evaluate(matchdoc::MatchDoc matchDoc) override { return true; }

    T evaluateAndReturn(matchdoc::MatchDoc matchDoc) override { return AttributeExpressionTyped<T>::getValue(); }

    bool allocate(matchdoc::MatchDocAllocator *allocator) override { return true; }

    matchdoc::ReferenceBase *getReferenceBase() const override {
        assert(0);
        return nullptr;
    }

    T getValue(matchdoc::MatchDoc matchDoc) const override { return AttributeExpressionTyped<T>::getValue(); }

    matchdoc::Reference<T> *getReference() const override {
        assert(0);
        return nullptr;
    }

    bool operator==(const AttributeExpression *checkExpr) const override {
        assert(checkExpr);
        const ArgumentAttributeExpressionTyped<T> *expr =
            dynamic_cast<const ArgumentAttributeExpressionTyped<T> *>(checkExpr);

        if (expr && this->getOriginalString() == expr->getOriginalString()) {
            return true;
        }
        return false;
    }

    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override { return true; }

    void setEvaluated() override { this->_isEvaluated = true; }

    uint64_t evaluateHash(matchdoc::MatchDoc matchDoc) override {
        assert(false);
        return 0;
    }
};

} // namespace turing
} // namespace suez
