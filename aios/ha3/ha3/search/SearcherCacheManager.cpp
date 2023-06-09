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
#include "ha3/search/SearcherCacheManager.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/expression/common.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/LayerDescription.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/QueryLayerClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/CacheHitSearchStrategy.h"
#include "ha3/search/CacheMissSearchStrategy.h"
#include "ha3/search/CacheResult.h"
#include "ha3/search/DefaultSearcherCacheStrategy.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/search/SearcherCacheStrategy.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
class MultiErrorResult;
}  // namespace common
namespace search {
struct DocCountLimits;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class SorterManager;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::monitor;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SearcherCacheManager);

SearcherCacheManager::SearcherCacheManager(
        SearcherCache *searcherCache,
        const IndexPartitionReaderWrapperPtr &indexPartReaderWrapper,
        Pool *pool,
        bool hasSubSchema,
        SearchCommonResource &resource,
        SearchPartitionResource &searchPartitionResource)
    : _searcherCache(searcherCache)
    , _cacheStrategy(new DefaultSearcherCacheStrategy(
                    pool, hasSubSchema, resource, searchPartitionResource))
    , _pool(pool)
    , _indexPartReaderWrapper(indexPartReaderWrapper)
    , _mainTable(searchPartitionResource.mainTable)
    , _partitionReaderSnapshot(searchPartitionResource.partitionReaderSnapshot)
    , _tracer(resource.tracer)
{
}

SearcherCacheManager::~SearcherCacheManager() {
    DELETE_AND_SET_NULL(_cacheResult);
}

bool SearcherCacheManager::initCacheBeforeOptimize(const Request *request,
        SessionMetricsCollector *sessionMetricsCollector,
        MultiErrorResult *errorResult, uint32_t requiredTopK)
{
    if (!useCache(request)) {
        return true;
    }
    _sessionMetricsCollector = sessionMetricsCollector;

    if (!_cacheStrategy->init(request, errorResult)) {
        AUTIL_LOG(WARN, "init searcher cache strategy failed!");
        return false;
    }

    _sessionMetricsCollector->increaseCacheGetNum();
    _searcherCache->increaseCacheGetNum();

    CacheMissType missType;
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    uint64_t key = cacheClause->getKey();
    autil::PartitionRange partRange = cacheClause->getPartRange();
    uint32_t curTime = cacheClause->getCurrentTime();
    bool cacheHit = _searcherCache->get(key, partRange, curTime,
            _cacheResult, _pool, missType);
    if (!cacheHit) {
        reportCacheMissMetrics(missType);
        return true;
    }
    recoverDocIds(_cacheResult, _indexPartReaderWrapper);
    // disable truncate if necessary
    ConfigClause *configClause = request->getConfigClause();
    bool useTruncate = configClause->useTruncateOptimizer()
                       && _cacheResult->useTruncateOptimizer();
    configClause->setUseTruncateOptimizer(useTruncate);

    bool validate = _searcherCache->validateCacheResult(key, partRange,
            useTruncate, requiredTopK, _cacheStrategy,
            _cacheResult, missType);
    if (validate) {
        AUTIL_LOG(DEBUG, "query hit the cache, key:[%lu]", key);
        assert(_cacheResult != NULL);
        if (!modifyRequest(request, _cacheResult, _indexPartReaderWrapper)) {
            AUTIL_LOG(WARN, "modify request failed!");
            return false;
        }
    } else {
        DELETE_AND_SET_NULL(_cacheResult);
        assert(missType != CMT_NONE);
        reportCacheMissMetrics(missType);
    }
    return true;
}

void SearcherCacheManager::reportCacheMissMetrics(CacheMissType missType) {
    switch (missType) {
    case CMT_NOT_IN_CACHE:
        _sessionMetricsCollector->increaseMissByNotInCacheNum();
        REQUEST_TRACE(TRACE3, "cache missed by not in cache");
        break;
    case CMT_EMPTY_RESULT:
        _sessionMetricsCollector->increaseMissByEmptyResultNum();
        REQUEST_TRACE(TRACE3, "cache missed by empty result");
        break;
    case CMT_EXPIRE:
        _sessionMetricsCollector->increaseMissByExpireNum();
        REQUEST_TRACE(TRACE3, "cache missed by expire");
        break;
    case CMT_DELETE_TOO_MUCH:
        _sessionMetricsCollector->increaseMissByDelTooMuchNum();
        REQUEST_TRACE(TRACE3, "cache missed by deleted too much");
        break;
    case CMT_TRUNCATE:
        _sessionMetricsCollector->increaseMissByTruncateNum();
        REQUEST_TRACE(TRACE3, "cache missed by truncate");
        break;
    default:
        break;
    }
}

MatchDocSearchStrategy *SearcherCacheManager::getSearchStrategy(
        const SorterManager *sorterManager,
        const Request *request,
        const DocCountLimits& docCountLimits) const
{
    if (cacheHit()) {
        return new CacheHitSearchStrategy(docCountLimits, request, this,
                sorterManager, _pool, _mainTable, _partitionReaderSnapshot);
    } else {
        return new CacheMissSearchStrategy(docCountLimits, request, this);
    }
}

void SearcherCacheManager::recoverDocIds(CacheResult *cacheResult,
        const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper)
{
    common::Result *result = cacheResult->getResult();
    if (!result) {
        return;
    }
    MatchDocs *matchDocs = result->getMatchDocs();
    if (!matchDocs) {
        return;
    }
    const shared_ptr<PartitionInfoWrapper> &partitionInfo =
        idxPartReaderWrapper->getPartitionInfo();
    const vector<globalid_t> &gids = cacheResult->getGids();
    assert(gids.size() == matchDocs->size());
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        matchdoc::MatchDoc &matchDoc = matchDocs->getMatchDoc(i);
        docid_t docId = partitionInfo->GetDocId(cacheResult->getPartInfoHint(), gids[i]);
        if (docId == INVALID_DOCID) {
            matchDoc.setDeleted();
        } else {
            matchDoc.setDocId(docId);
        }
    }
    const vector<globalid_t> &subDocGids = cacheResult->getSubDocGids();
    if (subDocGids.size() > 0) {
        recoverSubDocIds(matchDocs, subDocGids, idxPartReaderWrapper);
    }
}

void SearcherCacheManager::recoverSubDocIds(MatchDocs *matchDocs,
        const vector<globalid_t> &subDocGids,
        const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper)
{
    const auto &allocator = matchDocs->getMatchDocAllocator();
    assert(allocator && allocator->hasSubDocAllocator());
    const auto &subPartitionInfo = idxPartReaderWrapper->getSubPartitionInfo();
    size_t beginPos = 0;
    vector<int32_t> docIds;
    docIds.reserve(subDocGids.size());
    for (const auto gid : subDocGids) {
        docIds.push_back(subPartitionInfo->GetDocId(gid));
    }
    auto subAccessor = allocator->getSubDocAccessor();
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto matchDoc = matchDocs->getMatchDoc(i);
        if (matchDoc.isDeleted()) {
            continue;
        } else {
            subAccessor->setSubDocIds(matchDoc, docIds, beginPos);
        }
    }
}

bool SearcherCacheManager::modifyRequest(const Request *request,
        CacheResult *cacheResult,
        const IndexPartitionReaderWrapperPtr& idxPartReaderWrapper)
{
    const indexlib::index::PartitionInfoHint &partInfoHint = cacheResult->getPartInfoHint();

    indexlib::DocIdRangeVector docIdRanges;
    const shared_ptr<PartitionInfoWrapper> &partInfoPtr =
        idxPartReaderWrapper->getPartitionInfo();
    if (partInfoPtr == NULL) {
        AUTIL_LOG(WARN, "get partition info failed!");
        return false;
    }

    if (!partInfoPtr->GetDiffDocIdRanges(partInfoHint, docIdRanges)) {
        AUTIL_LOG(DEBUG, "get diff docid range failed");
    }

    QueryLayerClause *layerClause = createLayerClause(docIdRanges);
    Request* nonConstRequest = const_cast<Request*>(request);
    nonConstRequest->setQueryLayerClause(layerClause);

    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);
    cacheClause->setCurrentTime(cacheResult->getHeader()->time);

    return true;
}

QueryLayerClause* SearcherCacheManager::createLayerClause(
        const indexlib::DocIdRangeVector& docIdRanges)
{
    QueryLayerClause *layerClause = new QueryLayerClause();
    LayerDescription *layerDescription = new LayerDescription();
    LayerKeyRange *layerKeyRange = new LayerKeyRange;
    layerKeyRange->attrName = LAYERKEY_DOCID;
    for (indexlib::DocIdRangeVector::const_iterator it = docIdRanges.begin();
         it != docIdRanges.end(); it++)
    {
        // doc id range from indexlib is [first, second)
        const indexlib::DocIdRange &docIdRange = *it;
        if (docIdRange.second < docIdRange.first + 1) {
            continue;
        }
        LayerAttrRange keyRange;
        keyRange.from = StringUtil::toString(docIdRange.first);
        keyRange.to = StringUtil::toString(docIdRange.second);
        layerKeyRange->ranges.push_back(keyRange);
        AUTIL_LOG(DEBUG, "layerRange:%s-%s",
                keyRange.from.c_str(), keyRange.to.c_str());
    }
    layerDescription->addLayerRange(layerKeyRange);
    layerDescription->setQuotaMode(QM_PER_LAYER);
    layerDescription->setQuota(END_DOCID);
    layerClause->addLayerDescription(layerDescription);

    return layerClause;
}

} // namespace search
} // namespace isearch
