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

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"
#include "ha3/config/IndexInfoHelper.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/plugin/ScorerWrapper.h"

namespace autil {
namespace mem_pool {
class Pool;
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class Ha3MatchDocAllocator;
class Request;
}  // namespace common
namespace search {
class SearchCommonResource;
class SearchPartitionResource;
struct SearchProcessorResource;
}  // namespace search
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
template <typename T> class Reference;
}  // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class Scorer;
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {
class ScoringProvider;

} // namespace rank
} // namespace isearch

namespace ha3 {
class ScorerProvider;
}

namespace isearch {
namespace search {

class MatchDocScorers
{
public:
    MatchDocScorers(uint32_t reRankSize,
                    common::Ha3MatchDocAllocator *allocator,
                    autil::mem_pool::Pool *pool);
    ~MatchDocScorers();
private:
    MatchDocScorers(const MatchDocScorers &);
    MatchDocScorers& operator=(const MatchDocScorers &);
public:
    bool init(const std::vector<suez::turing::Scorer*> &scorerVec,
              const common::Request* request,
              const config::IndexInfoHelper *indexInfoHelper,
              SearchCommonResource &resource,
              SearchPartitionResource &partitionResource,
              SearchProcessorResource &processorResource);
    bool beginRequest();
    void setTotalMatchDocs(uint32_t v);
    void setAggregateResultsPtr(const common::AggregateResultsPtr &aggResultsPtr);
    void endRequest();
    uint32_t scoreMatchDocs(
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
    suez::turing::AttributeExpression* getScoreAttributeExpression();
    void sethasAssginedScoreValue(bool bScored) {
        _hasAssginedScoreValue = bScored;
    }
    size_t getScorerCount() const {
        return _scorerWrappers.size();
    }
    std::vector<std::string> getWarningInfos() {
        std::vector<std::string> warnInfos;
        for (auto wrapper : _scorerWrappers) {
            auto &warnInfo = wrapper->getWarningInfo();
            if (!warnInfo.empty()) {
                warnInfos.emplace_back(warnInfo);
            }
        }
        return warnInfos;
    }
private:
    uint32_t doScore(uint32_t phase,
                     autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
private:
    std::vector<suez::turing::ScorerWrapper *> _scorerWrappers;
    std::vector<rank::ScoringProvider *> _scoringProviders;
    std::vector<::ha3::ScorerProvider *> _cavaScorerProviders;
    matchdoc::Reference<score_t> *_scoreRef;
    suez::turing::AttributeExpressionTyped<score_t> *_scoreAttrExpr;
    common::Ha3MatchDocAllocator *_allocator;
    uint32_t _reRankSize;
    bool _hasAssginedScoreValue;
    autil::mem_pool::Pool *_pool;
private:
    friend class MatchDocScorersTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDocScorers> MatchDocScorersPtr;

} // namespace search
} // namespace isearch
