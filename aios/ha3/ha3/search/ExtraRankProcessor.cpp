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
#include "ha3/search/ExtraRankProcessor.h"

#include <cstddef>
#include <string>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "suez/turing/common/KvTracerMacro.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/plugin/Sorter.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/plugin/SorterWrapper.h"
#include "suez/turing/expression/util/TableInfo.h"
#ifndef AIOS_OPEN_SOURCE
#include "kvtracer/KVTracer.h"
#endif

#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/FinalSortClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/rank/GlobalMatchData.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/InnerSearchResult.h"
#include "ha3/search/MatchDocSearcherProcessorResource.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/sorter/SorterProvider.h"
#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h"

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, ExtraRankProcessor);

ExtraRankProcessor::ExtraRankProcessor(SearchCommonResource &resource,
                                       SearchPartitionResource &partitionResouce,
                                       SearchProcessorResource &processorResource)
    : _resource(resource)
    , _partitionResource(partitionResouce)
    , _processorResource(processorResource)
    , _sorterProvider(NULL)
    , _finalSorterWrapper(NULL)
{
}

ExtraRankProcessor::~ExtraRankProcessor() {
    if (_finalSorterWrapper) {
        _finalSorterWrapper->endSort();
        POOL_DELETE_CLASS(_finalSorterWrapper);
    }
    POOL_DELETE_CLASS(_sorterProvider);
}

bool ExtraRankProcessor::init(const common::Request *request) {
    sorter::SorterResource sorterResource = createSorterResource(request);
    _sorterProvider = POOL_NEW_CLASS(_resource.pool, sorter::SorterProvider, sorterResource);
    _finalSorterWrapper = createFinalSorterWrapper(request,
            _processorResource.sorterManager);
    if (!_finalSorterWrapper) {
        AUTIL_LOG(WARN, "The final sorter is not exist!");
        return false;
    }
    if (!_finalSorterWrapper->beginSort(_sorterProvider)) {
        std::string errorMsg = "Sorter begin request failed : [" +
                               _sorterProvider->getErrorMessage() + "]";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        _resource.errorResult->addError(ERROR_SORTER_BEGIN_REQUEST, errorMsg);
        return false;
    }
    return true;
}

void ExtraRankProcessor::process(const common::Request *request,
                                 InnerSearchResult& result, uint32_t extraRankCount)
{
    KVTRACE_MODULE_SCOPE_WITH_TRACER(_resource.sessionMetricsCollector->getTracer(),
            "extraRank");
    _resource.sessionMetricsCollector->extraRankStartTrigger();
    if (result.matchDocVec.size() == 0) {
        _resource.sessionMetricsCollector->extraRankCountTrigger(0);
        _resource.sessionMetricsCollector->extraRankEndTrigger();
        return;
    }
    _sorterProvider->setAggregateResultsPtr(result.aggResultsPtr);
    SortParam sortParam(_resource.pool);
    sortParam.requiredTopK = extraRankCount;
    sortParam.totalMatchCount = result.totalMatchDocs;
    sortParam.actualMatchCount = result.actualMatchDocs;
    _resource.sessionMetricsCollector->extraRankCountTrigger(result.matchDocVec.size());
    sortParam.matchDocs.swap(result.matchDocVec);
    _finalSorterWrapper->sort(sortParam);
    result.matchDocVec.swap(sortParam.matchDocs);
    result.totalMatchDocs = sortParam.totalMatchCount;
    result.actualMatchDocs = sortParam.actualMatchCount;
    _resource.sessionMetricsCollector->extraRankEndTrigger();
}

sorter::SorterResource ExtraRankProcessor::createSorterResource(const common::Request *request) {
    sorter::SorterResource sorterResource;

    sorterResource.location = sorter::SL_SEARCHER;
    sorterResource.scoreExpression = _processorResource.scoreExpression;
    sorterResource.requestTracer = _resource.tracer;
    sorterResource.pool = _resource.pool;
    sorterResource.request = request;

    int32_t totalDocCount = _partitionResource.getTotalDocCount();
    sorterResource.globalMatchData.setDocCount(totalDocCount);
    sorterResource.matchDataManager = &_partitionResource.matchDataManager;
    sorterResource.attrExprCreator = _partitionResource.attributeExpressionCreator.get();
    sorterResource.dataProvider = &_resource.dataProvider;
    sorterResource.matchDocAllocator = _resource.matchDocAllocator;
    sorterResource.fieldInfos = _resource.tableInfo->getFieldInfos();
    // support sorter update requiredTopk!!.
    sorterResource.requiredTopk = &_processorResource.docCountLimits.requiredTopK;
    sorterResource.comparatorCreator = _processorResource.comparatorCreator;
    sorterResource.sortExprCreator = _processorResource.sortExpressionCreator;
    sorterResource.queryTerminator = _resource.timeoutTerminator;
    sorterResource.kvpairs = &request->getConfigClause()->getKVPairs();
    sorterResource.partitionReaderSnapshot = _partitionResource.partitionReaderSnapshot;

    return sorterResource;
}

SorterWrapper *ExtraRankProcessor::createFinalSorterWrapper(
        const common::Request *request,
        const SorterManager *sorterManager) const
{
    std::string sorterName = "DEFAULT";
    auto finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        sorterName = finalSortClause->getSortName();
    }
    auto sorter = sorterManager->getSorter(sorterName);
    if (!sorter) {
        return NULL;
    }
    return POOL_NEW_CLASS(_resource.pool, SorterWrapper, sorter->clone());
}

} // namespace search
} // namespace isearch
