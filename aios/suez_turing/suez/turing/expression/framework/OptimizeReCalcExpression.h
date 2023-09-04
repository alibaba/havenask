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

namespace matchdoc {
class MatchDocAllocator;
class ReferenceBase;
} // namespace matchdoc

namespace suez {
namespace turing {

class OptimizeReCalcExpression : public AttributeExpression {
public:
    OptimizeReCalcExpression(const ExpressionVector &exprs);
    ~OptimizeReCalcExpression();

public:
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    bool batchAndSetEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount);
    bool allocate(matchdoc::MatchDocAllocator *allocator) override;

    ExpressionType getExpressionType() const override { return ET_OPTIMIZE; }
    matchdoc::ReferenceBase *getReferenceBase() const override {
        assert(false);
        return NULL;
    }
    bool operator==(const AttributeExpression *checkExpr) const override {
        assert(false);
        return false;
    }
    uint64_t evaluateHash(matchdoc::MatchDoc matchDoc) override {
        assert(false);
        return 0;
    }
    void setEvaluated() override;

    const ExpressionVector &getExpressions() const { return _exprs; }

private:
    ExpressionVector _exprs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
