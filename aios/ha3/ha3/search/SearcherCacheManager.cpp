#include <ha3/search/SearcherCacheManager.h>
#include <ha3/search/CacheHitSearchStrategy.h>
#include <ha3/search/CacheMissSearchStrategy.h>
#include <ha3/search/HitCollectorManager.h>
#include <ha3/search/MatchDocSearchStrategy.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <indexlib/partition/index_partition_reader.h>
#include <autil/StringUtil.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/SubDocAccessor.h>
#include <suez/turing/expression/plugin/SorterManager.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SearcherCacheManager);

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
    , _partitionReaderSnapshot(searchPartitionResource.partitionReaderSnapshot.get())
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
        HA3_LOG(WARN, "init searcher cache strategy failed!");
        return false;
    }

    _sessionMetricsCollector->increaseCacheGetNum();
    _searcherCache->increaseCacheGetNum();

    CacheMissType missType;
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    uint64_t key = cacheClause->getKey();
    uint32_t curTime = cacheClause->getCurrentTime();

    bool cacheHit = _searcherCache->get(key, curTime,
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

    bool validate = _searcherCache->validateCacheResult(key,
            useTruncate, requiredTopK, _cacheStrategy,
            _cacheResult, missType);
    if (validate) {
        HA3_LOG(DEBUG, "query hit the cache, key:[%lu]", key);
        assert(_cacheResult != NULL);
        if (!modifyRequest(request, _cacheResult, _indexPartReaderWrapper)) {
            HA3_LOG(WARN, "modify request failed!");
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
    Result *result = cacheResult->getResult();
    if (!result) {
        return;
    }
    MatchDocs *matchDocs = result->getMatchDocs();
    if (!matchDocs) {
        return;
    }
    const PartitionInfoPtr &partitionInfo =
        idxPartReaderWrapper->getPartitionInfo();
    const vector<globalid_t> &gids = cacheResult->getGids();
    assert(gids.size() == matchDocs->size());
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        matchdoc::MatchDoc &matchDoc = matchDocs->getMatchDoc(i);
        docid_t docId = partitionInfo->GetDocId(gids[i]);
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
    const PartitionInfoHint &partInfoHint = cacheResult->getPartInfoHint();

    DocIdRangeVector docIdRanges;
    const PartitionInfoPtr &partInfoPtr =
        idxPartReaderWrapper->getPartitionInfo();
    if (partInfoPtr == NULL) {
        HA3_LOG(WARN, "get partition info failed!");
        return false;
    }

    if (!partInfoPtr->GetDiffDocIdRanges(partInfoHint, docIdRanges)) {
        HA3_LOG(DEBUG, "get diff docid range failed");
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
        const DocIdRangeVector& docIdRanges)
{
    QueryLayerClause *layerClause = new QueryLayerClause();
    LayerDescription *layerDescription = new LayerDescription();
    LayerKeyRange *layerKeyRange = new LayerKeyRange;
    layerKeyRange->attrName = LAYERKEY_DOCID;
    for (DocIdRangeVector::const_iterator it = docIdRanges.begin();
         it != docIdRanges.end(); it++)
    {
        // doc id range from indexlib is [first, second)
        const DocIdRange &docIdRange = *it;
        if (docIdRange.second < docIdRange.first + 1) {
            continue;
        }
        LayerAttrRange keyRange;
        keyRange.from = StringUtil::toString(docIdRange.first);
        keyRange.to = StringUtil::toString(docIdRange.second);
        layerKeyRange->ranges.push_back(keyRange);
        HA3_LOG(DEBUG, "layerRange:%s-%s",
                keyRange.from.c_str(), keyRange.to.c_str());
    }
    layerDescription->addLayerRange(layerKeyRange);
    layerDescription->setQuotaMode(QM_PER_LAYER);
    layerDescription->setQuota(END_DOCID);
    layerClause->addLayerDescription(layerDescription);

    return layerClause;
}

END_HA3_NAMESPACE(search);
