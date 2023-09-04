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

#include <stdint.h>
#include <vector>

#include "cava/common/Common.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace suez {
namespace turing {

class TupleAttributeExpression : public AttributeExpressionTyped<uint64_t> {
public:
    TupleAttributeExpression() {}
    ~TupleAttributeExpression() {}

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        _value = evaluateHash(matchDoc);
        return true;
    }
    uint64_t evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        if (!this->tryFetchValue(matchDoc, _value)) {
            evaluate(matchDoc);
            this->storeValue(matchDoc, _value);
        }
        return _value;
    }
    uint64_t evaluateHash(matchdoc::MatchDoc matchDoc) override {
        uint64_t ret = 0;
        for (auto expr : _exprs) {
            cava::HashCombine(ret, expr->evaluateHash(matchDoc));
        }
        return ret;
    }
    ExpressionType getExpressionType() const override { return ET_TUPLE; }
    bool operator==(const AttributeExpression *expr) const override;

public:
    void addAttributeExpression(AttributeExpression *expr) {
        this->andIsDeterministic(expr->isDeterministic());
        _exprs.push_back(expr);
    }
    const std::vector<AttributeExpression *> &getAttributeExpressions() const { return _exprs; }

private:
    std::vector<AttributeExpression *> _exprs;
};

} // namespace turing
} // namespace suez
