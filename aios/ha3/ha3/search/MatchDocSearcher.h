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

#include "suez/turing/expression/plugin/SorterManager.h"

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Result.h"
#include "ha3/search/HashJoinInfo.h"
#include "ha3/isearch.h"
#include "ha3/search/ExtraRankProcessor.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/RerankProcessor.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "ha3/search/SearcherCacheManager.h"
#include "ha3/search/SeekAndRankProcessor.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace rank {
class ScoringProvider;
class RankProfileManager;
} // namespace rank
} // namespace isearch

namespace isearch {
namespace turing {
class Ha3BizMeta;
} // namespace turing
} // namespace isearch

namespace isearch {
namespace search {

class OptimizerChainManager;
class SortExpressionCreator;

class MatchDocSearcher
{
public:
    MatchDocSearcher(
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchRuntimeResource &runtimeResource,
            const rank::RankProfileManager *rankProfileMgr,
            const OptimizerChainManager *optimizerChainManager,
            const suez::turing::SorterManager *sorterManager,
            const config::AggSamplerConfigInfo& aggSamplerConfigInfo,
            const config::ClusterConfigInfo &clusterConfigInfo,
            SearcherCache *searcherCache = nullptr,
            const rank::RankProfile *rankProfile = NULL,
            const LayerMetasPtr &layerMetas = LayerMetasPtr(),
            const isearch::turing::Ha3BizMeta *ha3BizMeta = nullptr);
    ~MatchDocSearcher();

private:
    MatchDocSearcher(const MatchDocSearcher &);
    MatchDocSearcher& operator=(const MatchDocSearcher &);

public:
    common::ResultPtr search(const common::Request *request);
    bool seek(const common::Request *request,
              search::SearcherCacheInfo *searcherCacheInfo,
              search::InnerSearchResult &innerResult);
    bool seekAndJoin(const common::Request *request,
                     search::SearcherCacheInfo *searcherCacheInfo,
                     search::HashJoinInfo *hashJoinInfo,
                     search::InnerSearchResult &innerResult);

    static rank::ScoringProvider *createScoringProvider(
            const common::Request *request,
            const config::IndexInfoHelper *indexInfoHelper,
            SearchCommonResource &resource,
            SearchPartitionResource &partitionResource,
            SearchProcessorResource &processorResource);
private:
    bool init(const common::Request *request);
    bool initProcessorResource(const common::Request *request);
    bool doInit(const common::Request *request);
    bool doOptimize(const common::Request *request,
                                  IndexPartitionReaderWrapper *readerWrapper);
    MatchDocSearchStrategy *getSearchStrategy(const common::Request *request) const;
    uint32_t getMatchDocMaxCount(int runtimeTopK) const;

    bool doCreateAttributeExpressionVec(const common::AttributeClause *attributeClause);
    bool initExtraDocIdentifier(const common::ConfigClause *configClause);
    bool getPKExprInfo(uint8_t phaseOneInfoMask,
                       std::string &exprName, std::string &refName) const;

    void fillGlobalInformations(common::Result *result) const;
    void doLazyEvaluate(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect) const;
    void fillAttributePhaseOne(common::MatchDocs *matchDocs) const;

    common::ResultPtr constructErrorResult() const;
    common::MatchDocs *createMatchDocs(InnerSearchResult &innerResult) const;
    common::Result *constructResult(InnerSearchResult &innerResult) const;
    bool initAuxJoin(search::HashJoinInfo *hashJoinInfo);

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    const config::IndexInfoHelper *_indexInfoHelper;
    const rank::RankProfileManager *_rankProfileMgr;
    const OptimizerChainManager *_optimizerChainManager;
    const suez::turing::SorterManager *_sorterManager;
    std::vector<suez::turing::AttributeExpression *> _attributeExpressionVec;
    SearcherCacheManager _searcherCacheManager;
    SearchProcessorResource _processorResource;
    SeekAndRankProcessor _seekAndRankProcessor;
    RerankProcessor _rerankProcessor;
    ExtraRankProcessor _extraRankProcessor;
    const config::ClusterConfigInfo &_clusterConfigInfo;
    search::LayerMetasPtr _layerMetas;
    const isearch::turing::Ha3BizMeta *_ha3BizMeta;
private:
    friend class MatchDocSearcherTest;
    friend class CachedMatchDocSearcherTest;
    friend class SearcherCacheManagerTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDocSearcher> MatchDocSearcherPtr;

} // namespace search
} // namespace isearch
