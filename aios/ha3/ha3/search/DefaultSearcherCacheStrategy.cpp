#include <ha3/search/DefaultSearcherCacheStrategy.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/common/DataProvider.h>
#include <ha3/search/CacheResult.h>
#include <ha3/common/SearcherCacheClause.h>
#include <autil/TimeUtility.h>
#include <matchdoc/SubDocAccessor.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, DefaultSearcherCacheStrategy);

const uint32_t DefaultSearcherCacheStrategy::DEFAULT_EXPIRE_TIME_IN_SECONDS = 30;

class AllocatorScope {
public:
    AllocatorScope(func_expression::FunctionProvider *provider,
                   common::Ha3MatchDocAllocator *allocator)
        : _provider(provider)
    {
        if (_provider) {
            _oldAllocator = _provider->setAllocator(allocator);
        }
    }
    ~AllocatorScope() {
        if (_provider) {
            _provider->setAllocator(_oldAllocator);
        }
    }
private:
    func_expression::FunctionProvider *_provider;
    matchdoc::MatchDocAllocator *_oldAllocator;
};

DefaultSearcherCacheStrategy::DefaultSearcherCacheStrategy(mem_pool::Pool *pool,
        bool hasSubSchema,
        SearchCommonResource &resource,
        SearchPartitionResource &searchPartitionResource)
    : _pool(pool)
    , _partitionReaderSnapshot(searchPartitionResource.partitionReaderSnapshot.get())
    , _cavaAllocator(resource.cavaAllocator)
    , _functionProvider(&searchPartitionResource.functionProvider)
    , _functionCreator(resource.functionCreator)
    , _hasSubSchema(hasSubSchema)
    , _mainTable(searchPartitionResource.mainTable)
    , _tableInfoPtr(resource.tableInfoPtr)
    , _cavaPluginManagerPtr(resource.cavaPluginManagerPtr)
{
}

DefaultSearcherCacheStrategy::DefaultSearcherCacheStrategy(autil::mem_pool::Pool *pool)
    : _pool(pool)
{
}

DefaultSearcherCacheStrategy::~DefaultSearcherCacheStrategy() {
    POOL_DELETE_CLASS(_filter);
    POOL_DELETE_CLASS(_attributeExpressionCreator);
}

bool DefaultSearcherCacheStrategy::init(const Request *request,
                                        MultiErrorResult* errorResult)
{
    _matchDocAllocatorPtr.reset(new Ha3MatchDocAllocator(
                    _pool, needSubDoc(request)));
    AllocatorScope scope(_functionProvider, _matchDocAllocatorPtr.get());
    if (!initAttributeExpressionCreator(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "init AttributeExpressionCreator failed");
        HA3_LOG(WARN, "init AttributeExpressionCreator failed");
        return false;
    }

    if (!createExpireTimeExpr(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "init ExpireTimeExpr failed");
        HA3_LOG(WARN, "init ExpireTimeExpr failed");
        return false;
    }

    if (!createFilter(request)) {
        errorResult->addError(ERROR_CACHE_STRATEGY_INIT,
                              "create Filter failed");
        HA3_LOG(WARN, "create Filter failed");
        return false;
    }

    return true;
}

bool DefaultSearcherCacheStrategy::initAttributeExpressionCreator(
        const Request *request)
{
    // for test
    if (_attributeExpressionCreator != NULL) {
        return true;
    }
    _attributeExpressionCreator = POOL_NEW_CLASS(_pool,
            AttributeExpressionCreator,
            _pool,
            _matchDocAllocatorPtr.get(),
            _mainTable,
            _partitionReaderSnapshot,
            _tableInfoPtr,
            (request->getVirtualAttributeClause()
             ? request->getVirtualAttributeClause()->getVirtualAttributes()
             : suez::turing::VirtualAttributes()),
            _functionCreator,
            _cavaPluginManagerPtr,
            _functionProvider);
    return true;
}

bool DefaultSearcherCacheStrategy::genKey(const Request *request,
        uint64_t &key)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    if (!cacheClause) {
        return false;
    }

    if (!cacheClause->getUse()) {
        return false;
    }

    key = cacheClause->getKey();
    return true;
}

bool DefaultSearcherCacheStrategy::beforePut(uint32_t curTime,
        CacheResult *cacheResult)
{
    if (curTime == SearcherCacheClause::INVALID_TIME) {
        curTime = TimeUtility::currentTimeInSeconds();
    }
    uint32_t defaultExpireTime = curTime + DEFAULT_EXPIRE_TIME_IN_SECONDS;

    if (!calcAndfillExpireTime(cacheResult, defaultExpireTime)) {
        return false;
    }

    cacheResult->getHeader()->time = curTime;
    return true;
}

bool DefaultSearcherCacheStrategy::calcAndfillExpireTime(
        CacheResult *cacheResult, uint32_t defaultExpireTime)
{
    if (_expireTimeExpr) {
        Result *result = cacheResult->getResult();
        assert(result);
        MatchDocs *matchDocs = result->getMatchDocs();
        assert(matchDocs);

        uint32_t expireTime = (uint32_t)-1;
        if (!calcExpireTime(_expireTimeExpr, matchDocs, expireTime)) {
            return false;
        }
        if (expireTime == (uint32_t)-1) {
            expireTime = defaultExpireTime;
        }
        cacheResult->getHeader()->expireTime = expireTime;
    }
    return true;
}

uint32_t DefaultSearcherCacheStrategy::getCacheDocNum(const Request *request,
        uint32_t requireTopK)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);
    const vector<uint32_t> &cacheDocNumVec = cacheClause->getCacheDocNumVec();
    uint32_t size = cacheDocNumVec.size();
    assert(size > 0);
    for (size_t i = 0; i < size; ++i) {
        if (cacheDocNumVec[i] >= requireTopK) {
            return cacheDocNumVec[i];
        }
    }
    return *cacheDocNumVec.rbegin();
}

bool DefaultSearcherCacheStrategy::calcExpireTime(
        AttributeExpression *expireExpr,
        MatchDocs *matchDocs, uint32_t &expireTime)
{
    expireTime = (uint32_t)-1;
    auto ref = dynamic_cast<matchdoc::Reference<uint32_t> *>(
            expireExpr->getReferenceBase());
    if (ref == NULL) {
        HA3_LOG(WARN, "invalid expire time expr reference type:[%d]",
                (int)expireExpr->getType());
        return false;
    }

    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto orignalMatchDoc = matchDocs->getMatchDoc(i);
        if (matchdoc::INVALID_MATCHDOC == orignalMatchDoc) {
            return false;
        }
        auto tmpMatchDoc = allocateTmpMatchDoc(matchDocs, i);
        if (!expireExpr->evaluate(tmpMatchDoc)) {
            HA3_LOG(WARN, "calcluate expire time for doc[%d] failed" ,
                    tmpMatchDoc.getDocId());
            _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
            return false;
        }
        uint32_t tmpVal = 0;
        tmpVal = ref->get(tmpMatchDoc);
        expireTime = expireTime > tmpVal ? tmpVal : expireTime;
        _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
    }
    return true;
}

bool DefaultSearcherCacheStrategy::createExpireTimeExpr(
        const Request *request)
{
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);
    SyntaxExpr *expireSyntaxExpr = cacheClause->getExpireExpr();
    if (expireSyntaxExpr) {
        assert(!expireSyntaxExpr->isSubExpression());
        assert(_attributeExpressionCreator != NULL);
        AttributeExpression *expireExpr =
            _attributeExpressionCreator->createAttributeExpression(
                    expireSyntaxExpr);
        if (!expireExpr || !expireExpr->allocate(_matchDocAllocatorPtr.get())) {
            HA3_LOG(WARN, "create expire time expression failed");
            return false;
        }
        _expireTimeExpr = expireExpr;
    }

    return true;
}

bool DefaultSearcherCacheStrategy::createFilter(const Request *request) {
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    assert(cacheClause);

    FilterClause *filterClause = cacheClause->getFilterClause();
    if (filterClause) {
        assert(!filterClause->getRootSyntaxExpr()->isSubExpression());
        _filter = Filter::createFilter(filterClause,
                _attributeExpressionCreator, _pool);
        if (_filter == NULL) {
            return false;
        }
    }

    return true;
}

uint32_t DefaultSearcherCacheStrategy::filterCacheResult(
        CacheResult *cacheResult)
{
    Result *result = cacheResult->getResult();
    assert(result);
    MatchDocs *matchDocs = result->getMatchDocs();
    assert(matchDocs);
    uint32_t delCount = filterCacheResult(matchDocs);
    if (delCount > 0) {
        releaseDeletedMatchDocs(matchDocs);
    }
    return delCount;
}

uint32_t DefaultSearcherCacheStrategy::filterCacheResult(MatchDocs *matchDocs)
{
    uint32_t delCount = 0;
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto &matchDoc = matchDocs->getMatchDoc(i);
        if (matchDoc.isDeleted()) {
            delCount++;
            continue;
        }
        if (!_filter) {
            continue;
        }
        auto tmpMatchDoc = allocateTmpMatchDoc(matchDocs, i);
        if (!_filter->pass(tmpMatchDoc)) {
            matchDoc.setDeleted();
            delCount++;
        }
        _matchDocAllocatorPtr->deallocate(tmpMatchDoc);
    }
    return delCount;
}

matchdoc::MatchDoc DefaultSearcherCacheStrategy::allocateTmpMatchDoc(
        MatchDocs *matchDocs, uint32_t pos)
{
    auto matchDoc = matchDocs->getMatchDoc(pos);
    auto ret = _matchDocAllocatorPtr->allocate(matchDoc.getDocId());
    if (!_matchDocAllocatorPtr->hasSubDocAllocator()) {
        return ret;
    }
    const auto &allocator = matchDocs->getMatchDocAllocator();
    auto accessor = allocator->getSubDocAccessor();
    vector<int32_t> subDocids;
    accessor->getSubDocIds(matchDoc, subDocids);
    for (const auto &subDocId : subDocids) {
        _matchDocAllocatorPtr->allocateSub(ret, subDocId);
    }
    return ret;
}

void DefaultSearcherCacheStrategy::releaseDeletedMatchDocs(
        MatchDocs *matchDocs)
{
    const auto &allocator = matchDocs->getMatchDocAllocator();
    assert(allocator);
    vector<matchdoc::MatchDoc> matchDocsVect;
    for (uint32_t i = 0; i < matchDocs->size(); i++) {
        auto matchDoc = matchDocs->getMatchDoc(i);
        if (matchDoc.isDeleted()) {
            allocator->deallocate(matchDoc);
        } else {
            matchDocsVect.push_back(matchDoc);
        }
    }
    if (matchDocsVect.size() != matchDocs->size()) {
        auto &matchDocsVectRef = matchDocs->getMatchDocsVect();
        matchDocsVectRef = matchDocsVect;
    }
}

bool DefaultSearcherCacheStrategy::needSubDoc(const Request *request) const {
    ConfigClause *configClause = request->getConfigClause();
    if (configClause == NULL) {
        return false;
    }
    return configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO && _hasSubSchema;
}

END_HA3_NAMESPACE(search);
