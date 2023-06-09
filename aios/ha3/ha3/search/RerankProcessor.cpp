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
#include "ha3/search/RerankProcessor.h"

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "matchdoc/MatchDoc.h"
#ifndef AIOS_OPEN_SOURCE
#include "kvtracer/KVTracer.h"
#endif
#include "suez/turing/common/KvTracerMacro.h"
#include "suez/turing/expression/util/TableInfo.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/config/IndexInfoHelper.h"
#include "ha3/config/LegacyIndexInfoHelper.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/Comparator.h"
#include "ha3/rank/RankProfile.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocScorers.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SortExpressionCreator.h"
#include "autil/Log.h"

namespace suez {
namespace turing {
class Scorer;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, RerankProcessor);

RerankProcessor::RerankProcessor(SearchCommonResource &resource,
                                 SearchPartitionResource &partitionResource,
                                 SearchProcessorResource &processorResource)
    : _resource(resource)
    , _partitionResource(partitionResource)
    , _processorResource(processorResource)
    , _matchDocScorers(NULL)
    , _indexInfoHelper(POOL_NEW_CLASS(_resource.pool, config::LegacyIndexInfoHelper,
                    _resource.tableInfo->getIndexInfos()))
{
}

RerankProcessor::~RerankProcessor() {
    if (_matchDocScorers) {
        _matchDocScorers->endRequest();
        for (auto &warnInfo : _matchDocScorers->getWarningInfos()) {
            _resource.errorResult->addError(ERROR_RERANK_WARNING, warnInfo);
        }
    }
    POOL_DELETE_CLASS(_matchDocScorers);
    POOL_DELETE_CLASS(_indexInfoHelper);
}

bool RerankProcessor::init(const common::Request* request) {
    if (!request->getConfigClause()->needRerank() ||
        !_processorResource.rankProfile)
    {
        return true;
    }
    if(!createMatchDocScorers(request)) {
        AUTIL_LOG(WARN, "create MatchDocScorers failed!");
        _resource.errorResult->addError(ERROR_MATCHDOC_SCORERS_CREATE_FAILED, "");
        return false;
    }
    if (_matchDocScorers) {
        assert(_processorResource.scoringProvider);
        assert(_processorResource.rankProfile);
        if (!_matchDocScorers->beginRequest()) {
            std::string errorMsg = "Scorer begin request failed";
            AUTIL_LOG(WARN, "%s", errorMsg.c_str());
            _resource.errorResult->addError(ERROR_SCORER_BEGIN_REQUEST, errorMsg);
            return false;
        }
        if (!_processorResource.scoreExpression) {
            _processorResource.scoreExpression =
                _matchDocScorers->getScoreAttributeExpression();
        }
    }

    return true;
}

void RerankProcessor::process(const common::Request *request,
                              bool ranked, const rank::ComboComparator *rankComp,
                              InnerSearchResult& result)
{
    _resource.sessionMetricsCollector->reRankStartTrigger();
    if (_matchDocScorers && _matchDocScorers->getScorerCount() > 0) {
        KVTRACE_MODULE_SCOPE_WITH_TRACER(
                _resource.sessionMetricsCollector->getTracer(), "rerank");
        if (result.matchDocVec.size() == 0) {
            _resource.sessionMetricsCollector->reRankCountTrigger(0);
            return;
        }
        sortMatchDocs(result.matchDocVec, rankComp);
        uint32_t rerankCount = std::min((uint32_t)result.matchDocVec.size(),
                _processorResource.docCountLimits.rerankSize);
        _resource.sessionMetricsCollector->reRankCountTrigger(rerankCount);
        _matchDocScorers->setAggregateResultsPtr(result.aggResultsPtr);
        _matchDocScorers->setTotalMatchDocs(result.totalMatchDocs);
        _matchDocScorers->sethasAssginedScoreValue(ranked);
        uint32_t deletedCount = _matchDocScorers->scoreMatchDocs(result.matchDocVec);
        result.totalMatchDocs -= deletedCount;
        result.actualMatchDocs -= deletedCount;
    }
}

bool RerankProcessor::createMatchDocScorers(const common::Request* request) {
    uint32_t startPhase = 0;
    if (_processorResource.sortExpressionCreator->getRankExpression()) {
        startPhase = 1;
    }
    _matchDocScorers = POOL_NEW_CLASS(_resource.pool, MatchDocScorers,
            _processorResource.docCountLimits.rerankSize,
            _resource.matchDocAllocator.get(), _resource.pool);
    std::vector<Scorer*> scorerVec;
    if (!_processorResource.rankProfile->getScorers(scorerVec, startPhase)) {
        AUTIL_LOG(ERROR, "create match doc scorers failed "
                "because of get scorers failure.");
        return false;
    }
    return _matchDocScorers->init(scorerVec, request, _indexInfoHelper,
                                  _resource, _partitionResource, _processorResource);
}

class MatchDocComp {
public:
    MatchDocComp(const rank::Comparator *comp) {
        _comp = comp;
    }
public:
    inline bool operator() (matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) const {
        return _comp->compare(rht, lft);
    }
private:
    const rank::Comparator *_comp;
};

void RerankProcessor::sortMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect,
                                     const rank::ComboComparator *rankComp)
{
    if (matchDocVect.size() <= _processorResource.docCountLimits.rerankSize ||
        !rankComp) {
        return;
    }
    std::sort(matchDocVect.begin(), matchDocVect.end(), MatchDocComp(rankComp));
}

} // namespace search
} // namespace isearch
