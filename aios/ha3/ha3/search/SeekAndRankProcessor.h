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

#include "ha3/search/HashJoinInfo.h"
#include "ha3/isearch.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/RankSearcher.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class ComboComparator;
} // namespace rank
} // namespace isearch

namespace isearch {
namespace search {

class MatchDocSearchStrategy;
class HitCollectorManager;

struct SeekAndRankResult {
    bool ranked;
    const rank::ComboComparator *rankComp;
    SeekAndRankResult()
        : ranked(false)
        , rankComp(NULL)
    { }
};

class SeekAndRankProcessor
{
public:
    SeekAndRankProcessor(SearchCommonResource &resource,
                         SearchPartitionResource &partitionResouce,
                         SearchProcessorResource &processorResource,
                         const config::AggSamplerConfigInfo &aggSamplerConfigInfo);
    ~SeekAndRankProcessor();

private:
    SeekAndRankProcessor(const SeekAndRankProcessor &);
    SeekAndRankProcessor& operator=(const SeekAndRankProcessor &);

public:
    bool init(const common::Request* request);
    SeekAndRankResult process(const common::Request *request,
                              MatchDocSearchStrategy *searchStrategy,
                              InnerSearchResult& innerResult);

    SeekAndRankResult processWithCache(const common::Request *request,
            SearcherCacheInfo *cacheInfo,
            InnerSearchResult& innerResult);

    SeekAndRankResult processWithCacheAndJoinInfo(const common::Request *request,
            SearcherCacheInfo *cacheInfo,
            search::HashJoinInfo *hashJoinInfo,
            InnerSearchResult &innerResult);

    void initJoinFilter(const common::Request* request) {
        // create join filter have to go after createAttributeExpression (to create docid convert)
        _rankSearcher.createJoinFilter(
                _partitionResource.attributeExpressionCreator->getJoinDocIdConverterCreator(),
                getJoinType(request));
    }
private:
    void optimizeHitCollector(const common::Request *request,
                              HitCollectorManager *hitCollectorManager,
                              uint32_t phaseCount) const;
    HitCollectorManager *createHitCollectorManager(const common::Request *request) const;

    static bool needFlattenMatchDoc(const common::Request *request) {
        if (request->getConfigClause()) {
            return request->getConfigClause()->getSubDocDisplayType()
                == SUB_DOC_DISPLAY_FLAT;
        }
        return false;
    }

    static JoinType getJoinType(const common::Request *request) {
        JoinType joinType = DEFAULT_JOIN;
        const common::ConfigClause *configClause = request->getConfigClause();
        if (configClause) {
            joinType = configClause->getJoinType();
        }
        return joinType;
    }

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    RankSearcher _rankSearcher;
    suez::turing::RankAttributeExpression *_rankExpression;
    HitCollectorManager *_hitCollectorManager;
private:
    friend class MatchDocSearcherTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SeekAndRankProcessor> SeekAndRankProcessorPtr;

} // namespace search
} // namespace isearch

