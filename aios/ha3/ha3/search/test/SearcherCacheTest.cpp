#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SearcherCache.h>
#include <ha3/search/DefaultSearcherCacheStrategy.h>
#include <autil/legacy/jsonizable.h>

using namespace std;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(search);

class SearcherCacheTest : public TESTBASE {
public:
    SearcherCacheTest();
    ~SearcherCacheTest();
public:
    void setUp();
    void tearDown();
protected:
    CacheResult* createCacheResult();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SearcherCacheTest);

class FakeSearcherCacheStrategy : public DefaultSearcherCacheStrategy
{
public:
    FakeSearcherCacheStrategy(autil::mem_pool::Pool *pool)
        : DefaultSearcherCacheStrategy(pool)
    {
        _delCount = 0;
    }
    uint32_t filterCacheResult(CacheResult *cacheResult) {
        return _delCount;
    }
    void setDelCount(uint32_t delCount) {
        _delCount = delCount;
    }
private:
    uint32_t _delCount;
};

SearcherCacheTest::SearcherCacheTest() {
}

SearcherCacheTest::~SearcherCacheTest() {
}

void SearcherCacheTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SearcherCacheTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearcherCacheTest, testInit) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCacheConfig config;
    config.maxSize = 100;
    config.maxItemNum = 10;

    SearcherCache cache;
    bool ret = cache.init(config);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ToJsonString(config),
                         ToJsonString(cache.getConfig()));
}

TEST_F(SearcherCacheTest, testPut) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            new DefaultSearcherCacheStrategy(&_pool));
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCache cache;
    ASSERT_TRUE(cache.init(config));

    CacheResult *cacheResult = createCacheResult();
    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
    uint64_t key = 123456;
    uint32_t curTime = 0;

    ASSERT_TRUE(cache.put(key, curTime, searcherCacheStrategyPtr,
                             cacheResult, &_pool));

    // release the Result in cacheResult
    unique_ptr<Result> tmpresult_ptr(cacheResult->stealResult());

    MemCache *memCache = cache.getMemCache();
    MCElem mckey((char*)&key, sizeof(key));
    MemCacheItem *item = memCache->get(mckey);
    ASSERT_TRUE(item);

    autil::DataBuffer dataBuffer(item->value.dataSize);
    cache._pool->read(item->value, dataBuffer.getStart());
    CacheResult cacheResult2;
    cacheResult2.deserialize(dataBuffer, &_pool);

    ASSERT_EQ((uint32_t)2, cacheResult2.getHeader()->expireTime);
    ASSERT_DOUBLE_EQ((score_t)0.1,
                     cacheResult2.getHeader()->minScoreFilter.getMinScore(0));

    Result *result = cacheResult2.stealResult();
    unique_ptr<Result> result_ptr(result);
    ASSERT_TRUE(result);
    ASSERT_EQ((uint32_t)100, result->getTotalMatchDocs());
}

TEST_F(SearcherCacheTest, testPutWithResultDocNumlimit) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            new DefaultSearcherCacheStrategy(&_pool));
    {
        SearcherCacheConfig config;
        config.maxSize = 1024;
        config.minAllowedCacheDocNum = 1000;

        SearcherCache cache;
        ASSERT_TRUE(cache.init(config));

        CacheResult *cacheResult = createCacheResult();
        unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
        uint64_t key = 123456;
        uint32_t curTime = 0;

        ASSERT_FALSE(cache.put(key, curTime, searcherCacheStrategyPtr,
                               cacheResult, &_pool));
    }

    {
        SearcherCacheConfig config;
        config.maxSize = 1024;
        config.maxAllowedCacheDocNum = 90;

        SearcherCache cache;
        ASSERT_TRUE(cache.init(config));

        CacheResult *cacheResult = createCacheResult();
        unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
        uint64_t key = 123456;
        uint32_t curTime = 0;

        ASSERT_FALSE(cache.put(key, curTime, searcherCacheStrategyPtr,
                               cacheResult, &_pool));
    }

    {
        SearcherCacheConfig config;
        config.maxSize = 1024;
        config.minAllowedCacheDocNum = 100;
        config.maxAllowedCacheDocNum = 100;

        SearcherCache cache;
        ASSERT_TRUE(cache.init(config));

        CacheResult *cacheResult = createCacheResult();
        unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
        uint64_t key = 123456;
        uint32_t curTime = 0;

        ASSERT_TRUE(cache.put(key, curTime, searcherCacheStrategyPtr,
                              cacheResult, &_pool));
        // release the Result in cacheResult
        unique_ptr<Result> tmpresult_ptr(cacheResult->stealResult());

        MemCache *memCache = cache.getMemCache();
        MCElem mckey((char*)&key, sizeof(key));
        MemCacheItem *item = memCache->get(mckey);
        ASSERT_TRUE(item);

        autil::DataBuffer dataBuffer(item->value.dataSize);
        cache._pool->read(item->value, dataBuffer.getStart());
        CacheResult cacheResult2;
        cacheResult2.deserialize(dataBuffer, &_pool);

        ASSERT_EQ((uint32_t)2, cacheResult2.getHeader()->expireTime);
        ASSERT_DOUBLE_EQ((score_t)0.1,
                         cacheResult2.getHeader()->minScoreFilter.getMinScore(0));

        Result *result = cacheResult2.stealResult();
        unique_ptr<Result> result_ptr(result);
        ASSERT_TRUE(result);
        ASSERT_EQ((uint32_t)100, result->getTotalMatchDocs());
    }
}

TEST_F(SearcherCacheTest, testDoGet) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            new DefaultSearcherCacheStrategy(&_pool));
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCache cache;
    ASSERT_TRUE(cache.init(config));

    uint64_t key = 123456;
    CacheResult *cacheResult = NULL;
    uint32_t curTime = 0;
    CacheMissType missType = CMT_UNKNOWN;

    // test failed by miss
    bool ret = cache.doGet(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(CMT_NOT_IN_CACHE == missType);

    CacheResult *putCacheResult = createCacheResult();
    unique_ptr<CacheResult> cacheResult_ptr1(putCacheResult);
    ASSERT_TRUE(cache.put(key, curTime, searcherCacheStrategyPtr,
                             putCacheResult, &_pool));
    unique_ptr<Result> result_ptr1(putCacheResult->stealResult());

    // success
    curTime = 1;
    ret = cache.doGet(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(ret);
    ASSERT_EQ(CMT_NONE, missType);

    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);

    ASSERT_EQ((uint32_t)2, cacheResult->getHeader()->expireTime);
    ASSERT_DOUBLE_EQ((score_t)0.1,
                     cacheResult->getHeader()->minScoreFilter.getMinScore(0));

    Result *result = cacheResult->stealResult();
    unique_ptr<Result> result_ptr2(result);
    ASSERT_TRUE(result);
    ASSERT_EQ((uint32_t)100, result->getTotalMatchDocs());


    // test failed by expire
    CacheResult *cacheResult2 = NULL;
    curTime = 3;
    ret = cache.doGet(key, curTime, cacheResult2, &_pool, missType);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(!cacheResult2);
    ASSERT_TRUE(CMT_EXPIRE == missType);
}

TEST_F(SearcherCacheTest, testGet) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            new DefaultSearcherCacheStrategy(&_pool));
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCache cache;
    ASSERT_TRUE(cache.init(config));


    uint64_t key = 123456;
    CacheResult *cacheResult = NULL;
    uint32_t curTime = 0;
    uint32_t requiredTopk = 1;
    CacheMissType missType = CMT_UNKNOWN;

    // test failed by miss
    bool ret = cache.get(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(CMT_NOT_IN_CACHE == missType);

    // test failed by validate fail and result = NULL
    curTime = 1;
    CacheResult *putCacheResult = createCacheResult();
    unique_ptr<CacheResult> cacheResult_ptr1(putCacheResult);
    unique_ptr<Result> result_ptr1(putCacheResult->stealResult());
    ASSERT_TRUE(cache.put(key, curTime,
                             searcherCacheStrategyPtr,
                             putCacheResult, &_pool));

    ret = cache.get(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(ret);
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr, cacheResult, missType);
    ASSERT_TRUE(!ret);
    ASSERT_TRUE(CMT_EMPTY_RESULT == missType);
    delete cacheResult;

    CacheResult *putCacheResult2 = createCacheResult();
    putCacheResult2->setTruncated(true);
    unique_ptr<CacheResult> cacheResult_ptr2(putCacheResult2);
    putCacheResult2->getResult()->setTotalMatchDocs(101);
    ASSERT_TRUE(cache.put(key, curTime, searcherCacheStrategyPtr,
                             putCacheResult2, &_pool));
    unique_ptr<Result> result_ptr2(putCacheResult2->stealResult());

    // test failed by validate fail and result != NULL
    requiredTopk = 2;
    ret = cache.get(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(ret);
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr, cacheResult,
                                    missType);
    ASSERT_EQ(CMT_TRUNCATE, missType);
    delete cacheResult;

    // test success
    requiredTopk = 1;
    ret = cache.get(key, curTime, cacheResult, &_pool, missType);
    ASSERT_TRUE(ret);
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    cacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(cacheResult);
    ASSERT_TRUE(CMT_NONE == missType);

    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);

    ASSERT_EQ((uint32_t)2, cacheResult->getHeader()->expireTime);
    ASSERT_DOUBLE_EQ((score_t)0.1,
                     cacheResult->getHeader()->minScoreFilter.getMinScore(0));

    Result *result = cacheResult->stealResult();
    unique_ptr<Result> result_ptr3(result);
    ASSERT_TRUE(result);
    ASSERT_EQ((uint32_t)101, result->getTotalMatchDocs());
}

TEST_F(SearcherCacheTest, testValidateCacheResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    uint64_t key = 12345;
    FakeSearcherCacheStrategy *fakeSearcherCacheStrategy =
        new FakeSearcherCacheStrategy(&_pool);
    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            fakeSearcherCacheStrategy);
    CacheMissType missType = CMT_UNKNOWN;

    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCache cache;
    ASSERT_TRUE(cache.init(config));

    CacheResult * cacheResult = new CacheResult;
    uint32_t requiredTopk = 10;
    bool ret = cache.validateCacheResult(key, false, requiredTopk,
            searcherCacheStrategyPtr, cacheResult, missType);
    ASSERT_TRUE(!ret);
    delete cacheResult;

    CacheResult *putCacheResult = createCacheResult();

    requiredTopk = 1;
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    putCacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(putCacheResult != NULL);

    requiredTopk = 10;
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    putCacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(putCacheResult != NULL);

    //test need delete too much
    fakeSearcherCacheStrategy->setDelCount(1);
    requiredTopk = 1;
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    putCacheResult, missType);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(CMT_DELETE_TOO_MUCH, missType);
    delete putCacheResult;
}

TEST_F(SearcherCacheTest, testValidateCacheResultWithTrucateOptimizer) {
    uint64_t key = 12345;
    FakeSearcherCacheStrategy *fakeSearcherCacheStrategy =
        new FakeSearcherCacheStrategy(&_pool);
    SearcherCacheStrategyPtr searcherCacheStrategyPtr(
            fakeSearcherCacheStrategy);
    CacheMissType missType = CMT_UNKNOWN;

    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCache cache;
    ASSERT_TRUE(cache.init(config));

    uint32_t requiredTopk = 1;

    CacheResult *cacheResult = createCacheResult();
    // false, false
    bool ret = cache.validateCacheResult(key, true, requiredTopk,
            searcherCacheStrategyPtr, cacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(cacheResult != NULL);

    // true, false
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    cacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(cacheResult != NULL);

    cacheResult->setUseTruncateOptimizer(true);

    // false, true
    ret = cache.validateCacheResult(key, true, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    cacheResult, missType);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(cacheResult != NULL);

    // true, true
    ret = cache.validateCacheResult(key, false, requiredTopk,
                                    searcherCacheStrategyPtr,
                                    cacheResult, missType);
    ASSERT_TRUE(!ret);
    delete cacheResult;
}

TEST_F(SearcherCacheTest, testCacheGetNumAndCacheHitNumOperator) {
    HA3_LOG(DEBUG, "Begin Test!");

    SearcherCache searcherCache;
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheHitNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheHitNum());

    searcherCache.increaseCacheGetNum();
    searcherCache.increaseCacheGetNum();
    searcherCache.increaseCacheHitNum();
    ASSERT_EQ((uint32_t)2, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)1, searcherCache.getAndResetCacheHitNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheHitNum());

    searcherCache.increaseCacheGetNum();
    searcherCache.increaseCacheGetNum();
    searcherCache.increaseCacheGetNum();
    searcherCache.increaseCacheHitNum();
    searcherCache.increaseCacheHitNum();
    searcherCache.increaseCacheHitNum();
    ASSERT_EQ((uint32_t)3, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheGetNum());
    ASSERT_EQ((uint32_t)3, searcherCache.getAndResetCacheHitNum());
    ASSERT_EQ((uint32_t)0, searcherCache.getAndResetCacheHitNum());
}

CacheResult* SearcherCacheTest::createCacheResult() {
    CacheResult *cacheResult = new CacheResult();
    CacheHeader *header = cacheResult->getHeader();
    header->expireTime = 2;
    header->minScoreFilter.addMinScore(0.1);
    cacheResult->setResult(new Result(new MatchDocs));
    cacheResult->getResult()->setTotalMatchDocs(100);

    MatchDocs *matchDocs = cacheResult->getResult()->getMatchDocs();
    matchDocs->setActualMatchDocs(5);
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);

    vector<globalid_t> gids;
    for (uint32_t i = 0; i < 1; i ++) {
        matchDocs->addMatchDoc(matchDocAllocator->allocate(i));
        gids.push_back(i);
    }

    cacheResult->setGids(gids);

    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);
    return cacheResult;
}


END_HA3_NAMESPACE(search);
