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
#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/plugin/ScorerWrapper.h"

namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class Scorer;
class ScoringProvider;

class RankAttributeExpression : public AttributeExpressionTyped<score_t> {
public:
    RankAttributeExpression(Scorer *scorer, matchdoc::Reference<score_t> *scoreRef);
    ~RankAttributeExpression();

public:
    bool allocate(matchdoc::MatchDocAllocator *allocator) override;
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    bool batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) override;
    bool operator==(const AttributeExpression *checkExpr) const override;
    ExpressionType getExpressionType() const override { return ET_RANK; }
    void setEvaluated() override;

public:
    bool beginRequest(ScoringProvider *provider);
    void endRequest();
    const std::string &getWarningInfo() { return _rankScorer.getWarningInfo(); }

private:
    ScorerWrapper _rankScorer;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
