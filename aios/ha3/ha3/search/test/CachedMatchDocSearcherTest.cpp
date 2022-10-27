#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearcher.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <ha3/search/test/MatchDocSearcherTest.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/DefaultSearcherCacheStrategy.h>
#include <build_service/plugin/PlugInManager.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/test/test.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/test/CachedMatchDocSearcherTest.h>
#include <ha3/search/test/MatchDocSearcherTest.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <ha3/search/CacheMissSearchStrategy.h>
#include <ha3/search/CacheHitSearchStrategy.h>
#include <ha3/search/CacheResult.h>
#include <ha3/rank/RankProfileManagerCreator.h>

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;
using namespace build_service::plugin;

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, CachedMatchDocSearcherTest);


CachedMatchDocSearcherTest::CachedMatchDocSearcherTest() {
    _pool = new autil::mem_pool::Pool(1024);
    _matchDocSearcherCreatorPtr.reset(new MatchDocSearcherCreator(_pool));
}

CachedMatchDocSearcherTest::~CachedMatchDocSearcherTest() {
    _matchDocSearcherCreatorPtr.reset();
    delete _pool;
}

void CachedMatchDocSearcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _tableInfoPtr = _matchDocSearcherCreatorPtr->makeFakeTableInfo();
    _requestStr = "config=cluster:default,start:5,hit:3,rerank_size:9&&query=phrase:with";
    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));
}

void CachedMatchDocSearcherTest::tearDown() {
    _snapshotPtr.reset();
}

IndexPartitionReaderWrapperPtr CachedMatchDocSearcherTest::makeFakeIndexPartReader()
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] =
        "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n"
        "hehe:10[1];\n";
    fakeIndex.attributes =
        "price:uint64_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 10;"
        "attr_expire_time:uint32_t:3, 10, 11, 4, 5, 6, 7, 8, 9, 12;"
        "attr_sort:uint32_t:110, 111, 113, 114, 115, 116, 117, 118, 119, 112;"
        "attr_sort2:uint32_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 12;";

    return FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
}

#define ASSERT_RESULT(expectMatchDocCount, result) {                    \
        ASSERT_TRUE(result);                                         \
        ASSERT_TRUE(!result->hasError());                            \
        MatchDocs *matchDocs = result->getMatchDocs();                  \
        ASSERT_TRUE(matchDocs);                                      \
        ASSERT_EQ((uint32_t)expectMatchDocCount, matchDocs->size()); \
    }

/*TEST_F(CachedMatchDocSearcherTest, testProcessMiss) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareRequest(_requestStr);

    SearcherCacheConfig config;
    config.maxSize = 1024;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        makeFakeIndexPartReader();
    MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, config);
    ResultPtr result = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(8, result.get());

    // check score
    MatchDocs *matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(NULL != allocatorPtr);
    matchdoc::Reference<uint32_t> *scoreRef1 =
        allocatorPtr->findReference<uint32_t>("attr_sort");
    matchdoc::Reference<score_t> *scoreRef2 =
        allocatorPtr->findReference<score_t>(SCORE_REF);
    ASSERT_TRUE(scoreRef1);
    ASSERT_TRUE(scoreRef2);

    for (uint32_t i = 0; i < 8; i++) {
        score_t score = scoreRef2->get(matchDocs->getMatchDoc(i));
        ASSERT_DOUBLE_EQ((score_t)(1), score);
        uint32_t sort_attr = scoreRef1->get(matchDocs->getMatchDoc(i));
        ASSERT_EQ((uint32_t)(119 - i), sort_attr);
    }

    SearcherCachePtr cachePtr = searcherPtr->_searcherCacheManager.getSearcherCache();
    CacheResult *cacheResult = NULL;
    uint64_t key = 123456;
    CacheMissType missType = CMT_UNKNOWN;
    ASSERT_TRUE(cachePtr->get(key, requestPtr->getSearcherCacheClause()->getCurrentTime(),
                                 cacheResult, _pool, missType));
    ASSERT_TRUE(cachePtr->validateCacheResult(key, false, 8,
                    searcherPtr->_searcherCacheManager.getCacheStrategy(),
                    cacheResult, missType));
    ASSERT_TRUE(cacheResult);
    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
    Result *result2 = cacheResult->stealResult();
    unique_ptr<Result> result2_ptr(result2);
    ASSERT_RESULT(9, result2);

    ASSERT_EQ((uint32_t)4, cacheResult->getHeader()->expireTime);
    ASSERT_DOUBLE_EQ((score_t)111, cacheResult->getHeader()->minScoreFilter.getMinScore(0));
}

TEST_F(CachedMatchDocSearcherTest, testProcessMissNotPut) {
    HA3_LOG(DEBUG, "Begin Test!");
    string query = "query=phrase:with";
    RequestPtr requestPtr = RequestCreator::prepareRequest(query,
            "phrase");
    uint64_t key = 123456;
    SearcherCacheClause *searcherCacheClause = new SearcherCacheClause();
    searcherCacheClause->setUse(true);
    searcherCacheClause->setKey(key);

    vector<uint32_t> docNumLimit;
    docNumLimit.push_back(200);
    docNumLimit.push_back(3000);
    searcherCacheClause->setCacheDocNumVec(docNumLimit);
    requestPtr->setSearcherCacheClause(searcherCacheClause);

    SearcherCacheConfig cacheConfig;
    cacheConfig.maxSize = 1024;
    cacheConfig.latencyLimitInMs = 1000;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        makeFakeIndexPartReader();
    MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, cacheConfig);

    ResultPtr result = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(10, result);

    SearcherCachePtr cachePtr = searcherPtr->_searcherCacheManager.getSearcherCache();
    CacheResult *cacheResult = NULL;
    int64_t curTime = requestPtr->getSearcherCacheClause()->getCurrentTime();
    CacheMissType missType = CMT_UNKNOWN;
    ASSERT_TRUE(!cachePtr->get(key, curTime, cacheResult, _pool, missType));
    ASSERT_TRUE(!cacheResult);
}

TEST_F(CachedMatchDocSearcherTest, testCacheHitWithNoResults) {
    HA3_LOG(DEBUG, "Begin Test!");
    string requestStr = "config=cluster:default,start:5,hit:3,rerank_size:9&&query=phrase:with AND phrase:hehe";
    RequestPtr requestPtr = prepareRequest(requestStr);

    SearcherCacheConfig cacheConfig;
    cacheConfig.maxSize = 1024;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        makeFakeIndexPartReader();
    MatchDocSearcherPtr searcherPtr =
        _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, cacheConfig);
    ResultPtr result = searcherPtr->search(requestPtr.get());
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());

    CacheResult *cacheResult = NULL;
    SearcherCachePtr cachePtr = searcherPtr->_searcherCacheManager.getSearcherCache();
    uint64_t key = 123456;
    int64_t curTime = requestPtr->getSearcherCacheClause()->getCurrentTime();
    CacheMissType missType = CMT_UNKNOWN;
    ASSERT_TRUE(cachePtr->get(key, curTime, cacheResult, _pool, missType));
    ASSERT_TRUE(cachePtr->validateCacheResult(key, false, 8,
                    searcherPtr->_searcherCacheManager.getCacheStrategy(), cacheResult, missType));
    ASSERT_TRUE(cacheResult);
    ASSERT_EQ((uint32_t)32, cacheResult->getHeader()->expireTime);
    DELETE_AND_SET_NULL(cacheResult);

    curTime = 36;
    ASSERT_TRUE(!cachePtr->get(key, curTime, cacheResult, _pool, missType));
    ASSERT_EQ(CMT_EXPIRE, missType);
}

TEST_F(CachedMatchDocSearcherTest, testProcessHit) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest(_requestStr);

    SearcherCacheConfig cacheConfig;
    cacheConfig.maxSize = 1024;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        makeFakeIndexPartReader();
    MatchDocSearcherPtr searcherPtr =
        _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, cacheConfig);
    ResultPtr result = searcherPtr->search(requestPtr.get());
    ASSERT_TRUE(result);
    ASSERT_TRUE(!result->hasError());
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)8, matchDocs->size());

    CacheResult *cacheResult = NULL;
    SearcherCachePtr cachePtr = searcherPtr->_searcherCacheManager.getSearcherCache();
    uint64_t key = 123456;
    int64_t curTime = requestPtr->getSearcherCacheClause()->getCurrentTime();
    CacheMissType missType = CMT_UNKNOWN;
    ASSERT_TRUE(cachePtr->get(key, curTime, cacheResult, _pool, missType));
    ASSERT_TRUE(cachePtr->validateCacheResult(key, false, 8,
                    searcherPtr->_searcherCacheManager.getCacheStrategy(), cacheResult, missType));
    ASSERT_TRUE(cacheResult);
    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
    Result *result2 = cacheResult->stealResult();
    unique_ptr<Result> result2_ptr(result2);
    ASSERT_TRUE(result2);
    ASSERT_EQ((uint32_t)9, result2->getMatchDocs()->size());

    ASSERT_EQ((uint32_t)4, cacheResult->getHeader()->expireTime);
    ASSERT_DOUBLE_EQ((score_t)111, cacheResult->getHeader()->minScoreFilter.getMinScore(0));

    // searcher must reset first, to avoid resource release first
    searcherPtr.reset();
    searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, cachePtr);
    SearcherCacheClause *searcherCacheClause = requestPtr->getSearcherCacheClause();
    searcherCacheClause->setCurrentTime(1);

    ResultPtr result3 = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(8, result3);
    ASSERT_EQ((uint32_t)2, searcherCacheClause->getCurrentTime());
    ASSERT_EQ((uint32_t)10, result3->getTotalMatchDocs());
    ASSERT_EQ((uint32_t)10, result3->getActualMatchDocs());

    // cache hit with filter
    FilterClause *filterClause = new FilterClause;
    SyntaxExpr *syntaxExpr1 = new AtomicSyntaxExpr(
            "attr_sort2", vt_uint32, ATTRIBUTE_NAME);
    SyntaxExpr *syntaxExpr2 = new AtomicSyntaxExpr(
            "8", vt_uint32, INTEGER_VALUE);
    SyntaxExpr *filterExpr = new NotEqualSyntaxExpr(syntaxExpr1, syntaxExpr2);
    filterClause->setRootSyntaxExpr(filterExpr);
    searcherCacheClause->setFilterClause(filterClause);
    requestPtr->setQueryLayerClause(NULL);
    searcherPtr.reset();
    searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(), indexPartReaderPtr, cachePtr);
    ResultPtr result4 = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(8, result4);
    ASSERT_EQ((uint32_t)2, searcherCacheClause->getCurrentTime());
    ASSERT_EQ((uint32_t)9, result4->getTotalMatchDocs());
    ASSERT_EQ((uint32_t)9, result4->getActualMatchDocs());
}

TEST_F(CachedMatchDocSearcherTest, testProcessWithMultiCacheDocNum) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "with:0[0];1[1];2[2];4[4];5[5];6[6];7[7];8[8];9[9];\n";
    fakeIndex.attributes = "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";

    string query = "config=cluster:cluster1,hit:3,rerank_size:10&&query=phrase:with&&sort=price;RANK&&"
                   "searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8";
    RequestPtr requestPtr = MatchDocSearcherTest::prepareRequest(query, _tableInfoPtr);
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCachePtr cachePtr(new SearcherCache());
    ASSERT_TRUE(cachePtr->init(config));
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "9,8,7", 9);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "9,8,7,6");

    query = "config=cluster:cluster1,hit:4,rerank_size:10&&query=phrase:with&&sort=price;RANK&&"
                   "searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "9,8,7,6", 9);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "9,8,7,6");

    query = "config=cluster:cluster1,hit:6,rerank_size:10&&query=phrase:with&&sort=price;RANK&&"
            "searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "9,8,7,6,5,4", 9);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "9,8,7,6,5,4,2,1");

    query = "config=cluster:cluster1,hit:8,rerank_size:10&&query=phrase:with&&sort=price;RANK&&"
                   "searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "9,8,7,6,5,4,2,1", 9);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "9,8,7,6,5,4,2,1");

    query = "config=cluster:cluster1,hit:10,rerank_size:10&&query=phrase:with&&sort=price;RANK&&"
                   "searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "9,8,7,6,5,4,2,1,0", 9);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "9,8,7,6,5,4,2,1,0");
}

TEST_F(CachedMatchDocSearcherTest, testCacheDistinctQuery) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "with:0;1;2;3;\n";
    fakeIndex.attributes = "price: int32_t : 0,1,2,3;"
                           "type:  int32_t : 0,0,1,1;";
    fakeIndex.segmentDocCounts = "4";
    string query = "config=cluster:cluster1,hit:10&&query=phrase:with"
                   "&&sort=price&&searcher_cache=use:yes,key:1"
                   "&&distinct=dist_key:type,dist_count:1,dist_times:1";
    RequestPtr requestPtr = MatchDocSearcherTest::prepareRequest(query, _tableInfoPtr);
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCachePtr cachePtr(new SearcherCache());
    ASSERT_TRUE(cachePtr->init(config));
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "3,1,2,0", 4);

    FakeIndex fakeIndex2;
    fakeIndex2.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;\n";
    fakeIndex2.attributes = "price: int32_t : 0,1,2,3,4,5,6,7;"
                            "type:  int32_t : 0,0,1,1,0,0,1,1;";
    fakeIndex2.segmentDocCounts = "4,4";
    indexPartReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex2);
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "7,5,6,4,3,2,1,0", 8);
}

TEST_F(CachedMatchDocSearcherTest, testCacheHitDistinctQuery) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "with:0;1;2;3;\n";
    fakeIndex.attributes = "price: int32_t : 0,1,2,3;"
                           "type:  int32_t : 1,2,3,4;";
    fakeIndex.segmentDocCounts = "4";
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    SearcherCacheConfig config;
    config.maxSize = 1024;
    SearcherCachePtr cachePtr(new SearcherCache());
    ASSERT_TRUE(cachePtr->init(config));
    string query = "config=cluster:cluster1,start:0,hit:1,rerank_size:1000"
                   "&&query=phrase:with&&searcher_cache=use:yes"
                   "&&distinct=dist_key:type,dist_count:1,dist_times:1";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "0,", 4);

    indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    query = "config=cluster:cluster1,start:1,hit:1,rerank_size:1000"
            "&&query=phrase:with&&searcher_cache=use:yes"
            "&&distinct=dist_key:type,dist_count:1,dist_times:1";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "0,1", 4);

    indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    query = "config=cluster:cluster1,start:0,hit:0,rerank_size:1000"
            "&&query=phrase:with&&searcher_cache=use:yes"
            "&&distinct=dist_key:type,dist_count:1,dist_times:1";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "", 4);
}

TEST_F(CachedMatchDocSearcherTest, testWithSubDoc) {
    FakeIndex fakeIndex;
    fakeIndex.tablePrefix.push_back("sub_");
    fakeIndex.indexes["index_name"] = "with:0;2;\n";
    fakeIndex.indexes["sub_index_name"] = "with:0;1;4;6;7;9\n";
    fakeIndex.attributes = "sub_price: int32_t : 2,1,1, 1,2, 3,1,2,3,3;"
                           "sub_expire_time: uint32_t : 2,1,1, 1,2, 3,1,2,3,3;"
                           "price: int32_t : 0,1,2;";
    QueryExecutorTestHelper::addSubDocAttrMap(fakeIndex, "3,5,10");
    IndexPartitionReaderWrapperPtr indexPartReaderPtr =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    string query = "config=cluster:cluster1,sub_doc:group&&query=index_name:with OR sub_index_name:with&&sort=+price&&filter=sub_price>1&&searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8,expire_time:sub_max(sub_expire_time),cur_time:0&&virtual_attribute=sub_doc_price:sub_join(sub_price)&&attribute=sub_doc_price";
    RequestPtr requestPtr = MatchDocSearcherTest::prepareRequest(query, _tableInfoPtr);
    ASSERT_TRUE(requestPtr);

    SearcherCacheConfig config;
    config.maxSize = 1024;
    config.incDeletionPercent = 100;
    SearcherCachePtr cachePtr(new SearcherCache());
    ASSERT_TRUE(cachePtr->init(config));

    innerTestQuery(indexPartReaderPtr, cachePtr, query, "0,1,2", 3);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "0,1,2", "0,4,5,7,8,9");

    // cache_filter
    query = "config=cluster:cluster1,sub_doc:group&&query=index_name:with OR sub_index_name:with&&sort=+price&&filter=sub_price>1&&searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8,cur_time:0,cache_filter:sub_max(sub_price) > 2&&virtual_attribute=sub_doc_price:sub_join(sub_price)&&attribute=sub_doc_price";
    innerTestQuery(indexPartReaderPtr, cachePtr, query, "2", 1);

    // delete by expire time
    query = "config=cluster:cluster1,sub_doc:group&&query=index_name:with OR sub_index_name:with&&sort=+price&&filter=sub_price>1&&searcher_cache=use:yes,key:1,cache_doc_num_limit:4;8,cur_time:3&&virtual_attribute=sub_doc_price:sub_join(sub_price)&&attribute=sub_doc_price";
    // innerTestQuery(indexPartReaderPtr, cachePtr, query, "", 3);
    requestPtr = MatchDocSearcherTest::prepareRequest(query, _tableInfoPtr);
    compareCacheResult(cachePtr, requestPtr, indexPartReaderPtr, "", "");
}


TEST_F(CachedMatchDocSearcherTest, testGlobalVariableWhenHit) {
    HA3_LOG(DEBUG, "Begin Test!");
    string requestStr = "config=cluster:default,start:0,hit:2,rerank_size:5,rerank_hint:true&&query=phrase:with&&rank=rank_profile:global_val_profile";

    RequestPtr requestPtr = prepareRequest(requestStr);
    SearcherCacheConfig cacheConfig;
    cacheConfig.maxSize = 1024;
    IndexPartitionReaderWrapperPtr indexPartReaderPtr = makeFakeIndexPartReader();

    string profilePath = TOP_BUILDDIR;
    profilePath += "/testdata/conf/rankprofile.conf";
    string configStr = FileUtil::readFile(profilePath);
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(TEST_DATA_CONF_PATH_HA3));
    RankProfileManagerCreator creator(resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    RankProfileManagerPtr rankProfileManagerPtr = creator.createFromString(configStr);
    ASSERT_TRUE(rankProfileManagerPtr.get());
    ASSERT_TRUE(rankProfileManagerPtr->init(resourceReaderPtr, _cavaPluginManagerPtr, NULL));


    MatchDocSearcherPtr searcherPtr =
        _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(),
                indexPartReaderPtr, cacheConfig, rankProfileManagerPtr.get());
    ResultPtr result = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(2, result);
    vector<int32_t*> valVec = result->getGlobalVarialbes<int32_t>("global_val1");
    ASSERT_EQ(size_t(1), valVec.size());
    ASSERT_EQ(int32_t(10), *(valVec[0]));

    CacheResult *cacheResult = NULL;
    SearcherCachePtr cachePtr = searcherPtr->_searcherCacheManager.getSearcherCache();
    uint64_t key = 123456;
    int64_t curTime = requestPtr->getSearcherCacheClause()->getCurrentTime();
    CacheMissType missType = CMT_UNKNOWN;
    ASSERT_TRUE(cachePtr->get(key, curTime, cacheResult, _pool, missType));
    ASSERT_TRUE(cachePtr->validateCacheResult(key, false, 2,
                    searcherPtr->_searcherCacheManager.getCacheStrategy(), cacheResult, missType));
    ASSERT_TRUE(cacheResult);
    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);

    // searcher must reset first, to avoid resource release first
    searcherPtr.reset();
    searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(requestPtr.get(),
            indexPartReaderPtr, cachePtr, rankProfileManagerPtr.get());
    SearcherCacheClause *searcherCacheClause = requestPtr->getSearcherCacheClause();
    searcherCacheClause->setCurrentTime(1);

    ResultPtr result1 = searcherPtr->search(requestPtr.get());
    ASSERT_RESULT(2, result1);
    vector<int32_t*> valVec1 = result1->getGlobalVarialbes<int32_t>("global_val1");
    ASSERT_EQ(size_t(2), valVec1.size());
    ASSERT_EQ(int32_t(10), *(valVec1[0]));
    ASSERT_EQ(int32_t(0), *(valVec1[1]));
    }*/

ResultPtr CachedMatchDocSearcherTest::innerTestQuery(
        const IndexPartitionReaderWrapperPtr& indexPartReaderPtr,
        const SearcherCachePtr& cachePtr,
        const string &query, const string &resultStr,
        uint32_t totalHit)
{
    RequestPtr request = MatchDocSearcherTest::prepareRequest(query, _tableInfoPtr,
            _matchDocSearcherCreatorPtr);
    assert(request);
    request->setPool(_pool);

    MatchDocSearcherPtr searcherPtr = _matchDocSearcherCreatorPtr->createSearcher(
            request.get(), indexPartReaderPtr, cachePtr.get(), _snapshotPtr);
    assert(searcherPtr);

    ResultPtr result = searcherPtr->search(request.get());
    assert(result);

    if (!((uint32_t)totalHit == result->getTotalMatchDocs())) {
        HA3_LOG(ERROR, "assert false, query is: %s", query.c_str());
        assert(false);
    }

    MatchDocs * matchDocs = result->getMatchDocs();
    assert(matchDocs);
    vector<docid_t> results = SearcherTestHelper::covertToResultDocIds(resultStr);

    if (!((uint32_t)results.size() == matchDocs->size())) {
        HA3_LOG(ERROR, "assert false, query is: %s", query.c_str());
        assert(false);
    }

    for (size_t i = 0; i < results.size(); ++i) {
        if (!(results[i] == matchDocs->getMatchDoc(i).getDocId())) {
            HA3_LOG(ERROR, "assert false, query is: %s", query.c_str());
            assert(false);
        }
    }
    return result;
}

RequestPtr CachedMatchDocSearcherTest::prepareRequest(const string &requestStr) {
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr, "phrase");
    assert(requestPtr);

    SyntaxExpr *sortExpr = new AtomicSyntaxExpr("attr_sort",
            vt_uint32, ATTRIBUTE_NAME);
    SortClause *sortClause = new SortClause;
    sortClause->addSortDescription(sortExpr, false);
    sortClause->addSortDescription(NULL, true);
    requestPtr->setSortClause(sortClause);

    uint64_t key = 123456;
    SearcherCacheClause *searcherCacheClause = new SearcherCacheClause();
    searcherCacheClause->setUse(true);
    searcherCacheClause->setKey(key);
    SyntaxExpr *expireTimeExpr = new AtomicSyntaxExpr("attr_expire_time",
            vt_uint32, ATTRIBUTE_NAME);
    searcherCacheClause->setExpireExpr(expireTimeExpr);
    searcherCacheClause->setCurrentTime(2);

    vector<uint32_t> docNumLimit;
    docNumLimit.push_back(200);
    docNumLimit.push_back(3000);
    searcherCacheClause->setCacheDocNumVec(docNumLimit);

    requestPtr->setSearcherCacheClause(searcherCacheClause);
    requestPtr->setPool(_pool);
    return requestPtr;
}

void CachedMatchDocSearcherTest::compareCacheResult(
        SearcherCachePtr cachePtr, RequestPtr requestPtr,
        const IndexPartitionReaderWrapperPtr& indexPartitionReaderWrapper,
        const string &cacheIDStr,
        const string &subDocGidStr)
{
    CacheResult *cacheResult = NULL;
    uint64_t key = 1;
    int64_t curTime = requestPtr->getSearcherCacheClause()->getCurrentTime();
    vector<docid_t> results = SearcherTestHelper::covertToResultDocIds(cacheIDStr);
    CacheMissType missType = CMT_UNKNOWN;
    bool ret = cachePtr->get(key, curTime, cacheResult, _pool, missType);
    (void)ret;
    if (results.empty()) {
        assert(!ret);
        return;
    }
    assert(ret);
    assert(cacheResult);
    SearcherCacheManager::recoverDocIds(cacheResult, indexPartitionReaderWrapper);
    unique_ptr<CacheResult> cacheResult_ptr(cacheResult);
    Result *result = cacheResult->stealResult();
    assert(result);
    unique_ptr<Result> resultPtr(result);
    MatchDocs *matchDocs = resultPtr->getMatchDocs();
    assert(matchDocs);
    ASSERT_EQ((uint32_t)results.size(), matchDocs->size());
    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_EQ(results[i],
                             matchDocs->getMatchDoc(i).getDocId());
    }
    if (!subDocGidStr.empty()) {
        const vector<globalid_t> &subDocGids = cacheResult->getSubDocGids();
        vector<globalid_t> expectedGids;
        StringUtil::fromString<globalid_t>(subDocGidStr, expectedGids, ",");
        ASSERT_EQ(expectedGids.size(), subDocGids.size());
        assert(expectedGids == subDocGids);
    }
}

END_HA3_NAMESPACE(search);
