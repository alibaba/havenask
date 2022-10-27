#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/RankSearcher.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <autil/SimpleSegregatedAllocator.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/rank/HitCollector.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/search/test/LayerMetasConstructor.h>
#include <ha3/search/test/QueryExecutorMock.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/search/test/FakeAttributeExpressionFactory.h>
#include <ha3/search/AggregatorCreator.h>
#include <ha3/search/NormalAggregator.h>
#include <ha3/queryparser/ClauseParserContext.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/qrs/RequestValidator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/test/test.h>
#include <ha3/rank/ComparatorCreator.h>
#include <ha3/rank/HitCollector.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/test/FakeJoinDocidReader.h>
#include <ha3/search/LayerMetaUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(qrs);

IE_NAMESPACE_USE(index);

BEGIN_HA3_NAMESPACE(search);

class RankSearcherTest : public TESTBASE {
/////////////////////begin test multi layer search//////////////////////
/////////////////////end test multi layer search//////////////////////
public:
    RankSearcherTest();
    ~RankSearcherTest();
public:
    void setUp();
    void tearDown();
/////////////////////begin test multi layer search//////////////////////
/////////////////////end test multi layer search//////////////////////
protected:
    void initTableInfos();
    void initClusterConfigMap();
    void internalTestInit(const std::string &requestStr, const index::FakeIndex &fakeIndex,
                          const std::string &resultStr, const std::string &aggResultStr,
                          uint32_t totalHit, ErrorCode errorCode = ERROR_NONE,
                          int32_t seekDocCount = -1);
    void initHitCollector();
    void createQueryExecutors(RankSearcher &searcher, const std::string &docsInIndex);
    void createConverterFactory();
    void initFilter(RankSearcher &searcher, const std::string &filterDocsStr);
    void checkResult(const std::string &resultStr, rank::HitCollectorPtr &hitCollectorPtr);
    void checkAttrExpr(RankSearcher *searcher);
protected:
    std::tr1::shared_ptr<autil::mem_pool::Pool> _pool;
    IndexPartitionReaderWrapperPtr _reader;

    std::vector<bool> *_values;
    FakeAttributeExpression<bool> *_attrExpr;
    std::vector<float> *_floatValues;
    FakeAttributeExpression<float> *_hitAttrExpr;

    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    rank::HitCollectorPtr _hitCollectorPtr;
    RankSearcherResource _resource;
    monitor::SessionMetricsCollectorPtr _sessionMetricsCollector;

    ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;

    RankSearcher *_searcher;
    MatchDataManager _matchDataManager;
    SortExpressionCreator *_sortExprCreator;

    JoinDocIdConverterCreator *_converterFactory;
    common::MultiErrorResultPtr _errorResult;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, RankSearcherTest);


static const uint32_t MAX_DOC_COUNT = 10000;

RankSearcherTest::RankSearcherTest() {
    _pool.reset(new autil::mem_pool::Pool(1024));
    _sortExprCreator = new SortExpressionCreator(NULL, NULL, 
            NULL, _errorResult, _pool.get());
    _converterFactory = NULL;
}

RankSearcherTest::~RankSearcherTest() {
    DELETE_AND_SET_NULL(_converterFactory);
    DELETE_AND_SET_NULL(_sortExprCreator);
}

void RankSearcherTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

    _values = NULL;
    _attrExpr = NULL;
    _hitAttrExpr = NULL;
    _floatValues = NULL;

    _allocatorPtr.reset(new common::Ha3MatchDocAllocator(_pool.get()));
    _sessionMetricsCollector.reset(new monitor::SessionMetricsCollector(NULL));

    _resource._requiredTopK = MAX_DOC_COUNT;
    _resource._rankSize = numeric_limits<uint32_t>::max();
    _resource._matchDocAllocator = _allocatorPtr.get();
    _resource._sessionMetricsCollector = _sessionMetricsCollector.get();

    initTableInfos();
    initClusterConfigMap();

    FakeIndex fakeIndex;
    _reader = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);

    _searcher = new RankSearcher(MAX_DOC_COUNT, _pool.get(), config::AggSamplerConfigInfo());
    _searcher->_matchDataManager = &_matchDataManager;
    _searcher->_indexPartReaderPtr = _reader->getReader();
}

void RankSearcherTest::tearDown() {
    DELETE_AND_SET_NULL(_values);
    DELETE_AND_SET_NULL(_attrExpr);
    DELETE_AND_SET_NULL(_hitAttrExpr);
    DELETE_AND_SET_NULL(_floatValues);
    DELETE_AND_SET_NULL(_searcher);
    DELETE_AND_SET_NULL(_converterFactory);
}

TEST_F(RankSearcherTest, testSimpleProcess) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3\n";
    string resultStr = "3,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 3);
}

TEST_F(RankSearcherTest, testFilter) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3&&filter=price>30";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "price:int32_t:89,10,20,30,40,50,23,24,25,90,100";
    string resultStr = "10,9,5,4";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 4);
}

TEST_F(RankSearcherTest, testPKFilter) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3&&pkfilter=a1";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.indexes["pk"] = "a0:0,a1:1,a2:2,a3:3,a4:4,a5:5,a6:6,a7:7,a8:8,a9:9,a10:10";
    fakeIndex.attributes = "price:int32_t:89,10,20,30,40,50,23,24,25,90,100";
    string resultStr = "1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 1);
}

void RankSearcherTest::createConverterFactory() {
    _converterFactory = new JoinDocIdConverterCreator(_pool.get(),
            _resource._matchDocAllocator);
    map<docid_t, docid_t> docIdMap;
    docIdMap[1] = 0;
    for (docid_t i = 2; i <= 10; i++) {
        if (i % 2 == 1) {
            docIdMap[i] = 0;
        } else {
            docIdMap[i] = -1;
        }
    }
    FakeJoinDocidReader* joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    JoinDocIdConverterBase* fakeConverter = 
        POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr1", joinDocidReader);
    fakeConverter->setStrongJoin(false);
    _converterFactory->addConverter("attr1", fakeConverter);

    docIdMap[1] = 0;
    for (docid_t i = 2; i <= 10; i++) {
        if (i % 2 == 1) {
            docIdMap[i] = -1;
        } else {
            docIdMap[i] = 0;
        }
    }

    joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    fakeConverter =  POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr2", joinDocidReader);
    fakeConverter->setStrongJoin(true);
    _converterFactory->addConverter("attr2", fakeConverter);
}

TEST_F(RankSearcherTest, testDefaultJoinFilter) {
    createConverterFactory();

    string requestStr = "config=cluster:simple&&query=phrase:mp3";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    string resultStr = "10,8,6,4,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 6);
}

TEST_F(RankSearcherTest, testWeakJoinFilter) {
    createConverterFactory();

    string requestStr = "config=cluster:simple,join_type:weak&&query=phrase:mp3";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    string resultStr = "10,9,8,7,6,5,4,3,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 10);
}

TEST_F(RankSearcherTest, testStrongJoinFilter) {
    createConverterFactory();

    string requestStr = "config=cluster:simple,join_type:strong&&query=phrase:mp3";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    string resultStr = "1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 1);
}

TEST_F(RankSearcherTest, testAggregate) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3"
                        "&&aggregate=group_key:type,agg_fun:count()#max(price)#min(price)#sum(price)";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1, 6, 3, 2,  6, 1, 6, 1, 2, 3;"
                           "price:int32_t: 0, 1, 2, 3, 14, 5, 6, 7, 8, 9, 10;";
    string resultStr = "10,9,8,7,6,5,4,3,2,1";
    string aggResultStr = "1: 3, 8,  1, 15;"
                          "2: 2, 14, 9, 23;"
                          "3: 2, 10, 3, 13;"
                          "6: 3, 7,  2, 14;";

    internalTestInit(requestStr, fakeIndex, resultStr, aggResultStr, 10);
}

TEST_F(RankSearcherTest, testAndQueryEstimateTotalHit) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3#first AND phrase:'1'#second";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n"
                                  "1:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.indexes["first"] = "mp3:1;2;3;4;5;\n";
    fakeIndex.indexes["second"] = "1:4;5;6;\n";
    string resultStr = "5,4";

    internalTestInit(requestStr, fakeIndex, resultStr, "", 6);
}

TEST_F(RankSearcherTest, testORQueryEstimateTotalHit) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3#first OR phrase:'1'#second";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n"
                                  "1:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.indexes["first"] = "mp3:1;2;3;4;5;\n";
    fakeIndex.indexes["second"] = "1:4;5;6;\n";
    string resultStr = "6,5,4,3,2,1";

    internalTestInit(requestStr, fakeIndex, resultStr, "", 15);
}

TEST_F(RankSearcherTest, testSearchTruncateIndex) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3#price";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.indexes["price"] = "mp3:1;3;4;\n";
    fakeIndex.attributes = "type:int32_t:  6, 1, 6, 3, 2,  6, 1, 6, 1, 2, 3;"
                           "price:int32_t: 0, 1, 2, 3, 14, 5, 6, 7, 8, 9, 10;";
    string resultStr = "4,3,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 10);
}

TEST_F(RankSearcherTest, testFilterAndAggregate) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3"
                        "&&aggregate=group_key:type,agg_fun:count()#max(price)#min(price)#sum(price)"
                        "&&filter=price>5";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1, 6, 3, 2,  6, 1, 6, 1, 2, 3;"
                           "price:int32_t: 0, 1, 2, 3, 14, 5, 6, 7, 8, 9, 10;";
    string resultStr = "10,9,8,7,6,4";
    string aggResultStr = "1: 2, 8,  6, 14;"
                          "2: 2, 14, 9, 23;"
                          "3: 1, 10, 10, 10;"
                          "6: 1, 7,  7, 7;";
    internalTestInit(requestStr, fakeIndex, resultStr, aggResultStr, 6);
}

TEST_F(RankSearcherTest, testMultiLayerSearch) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{1,2,6}*int32{[10,20]},quota:5"
                        "&&query=phrase:mp3";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "5,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 3);
}

TEST_F(RankSearcherTest, testMultiLayerMultiQuerySearch) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{1,2,6}*int32{[10,20]},quota:5;range:price{[3,5]},quota:1;range:price{10},quota:1"
                        "&&query=phrase:mp3;phrase:phone";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "9,5,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 4);
}

TEST_F(RankSearcherTest, testEstimateOtherLayer) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{1,2},quota:5;range:%other"
                        "&&query=phrase:mp3";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6;"
                           "id   :int32_t: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "5,4,3,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 7500);
}

TEST_F(RankSearcherTest, testNoEstimateOtherLayer) {
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6;"
                           "id   :int32_t: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;";
    fakeIndex.partitionMeta = "+price,+int32";
    {
        string requestStr = "config=cluster:simple"
                            "&&layer=range:price{1,2},quota:2;range:%other"
                            "&&query=phrase:mp3";
        string resultStr = "2,1";
        internalTestInit(requestStr, fakeIndex, resultStr, "", 2);
    }
    tearDown();
    setUp();
    {
        string requestStr = "config=cluster:simple"
                            "&&layer=range:price{1,2},quota:1;range:%other"
                            "&&query=phrase:mp3";
        string resultStr = "1";
        internalTestInit(requestStr, fakeIndex, resultStr, "", 2);
    }
}

TEST_F(RankSearcherTest, testMultiLayerSearchEmptyLayer) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{15}*int32{[10,20]},quota:5;range:price{1,2,6}*int32{[10,20]}"
                        "&&query=phrase:mp3";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "5,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 3);
}

TEST_F(RankSearcherTest, testMultiLayerSearchWithMultiQuery) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{1,2,6}*int32{[10,20]},quota:5;range:price{[3,5]},quota:0"
                        "&&query=phrase:mp3;phrase:phone";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "5,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 3);
}

TEST_F(RankSearcherTest, testMultiLayerSearchFailed) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{6},quota:5;range:price{0},quota:5"
                        "&&query=phrase:mp3;phrase:phone";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n"
                                  "phone:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price,+int32";
    string resultStr = "7,6,5";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 3);
}

TEST_F(RankSearcherTest, testCreateAggregateFailed) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3"
                        "&&aggregate=group_key:type,agg_fun:count()#max(price)#min(price)#sum(price)";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1\n";
    internalTestInit(requestStr, fakeIndex, "", "", 0, ERROR_SEARCH_SETUP_AGGREGATOR);
}

TEST_F(RankSearcherTest, testCreateFilterFailed) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3"
                        "&&filter=price>5";
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    internalTestInit(requestStr, fakeIndex, "", "", 0, ERROR_SEARCH_SETUP_FILTER);
}

TEST_F(RankSearcherTest, testTermSeekDocCount) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    string resultStr = "10,9,8,7,6,5,4,3,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 10, ERROR_NONE, 11);
}

TEST_F(RankSearcherTest, testPKSeekDocCount) {
    string requestStr = "config=cluster:simple&&query=phrase:mp3";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n";
    string resultStr = "10,9,8,7,6,5,4,3,2,1";
    internalTestInit(requestStr, fakeIndex, resultStr, "", 10, ERROR_NONE, 11);
}

TEST_F(RankSearcherTest, testAndOrSeekDocCount) {
    string requestStr = "config=cluster:simple&&query=(phrase:mp3 AND phrase:1) OR phrase:term";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n"
                                  "1:1;2;3;4;5\n"
                                  "term:1;3;5;7;9\n";
    string resultStr = "9,7,5,4,3,2,1";

    internalTestInit(requestStr, fakeIndex, resultStr, "", 7, ERROR_NONE, 17);
}

TEST_F(RankSearcherTest, testMultiLayerMultiQuerySeekDocCount) {
    string requestStr = "config=cluster:simple"
                        "&&layer=range:price{1,2},quota:2;range:price{3,5},quota:1;range:price{10},quota:1"
                        "&&query=phrase:mp3;phrase:phone";

    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] = "mp3:1;2;3;4;5;6;7;8;9;10\n"
                                  "phone:1;2;3;4;5;6;7;8;9;10\n";
    fakeIndex.attributes = "type:int32_t:  6, 1,  6,  3, 2,  6,  1,  6,  1, 2,  3;"
                           "price:int32_t: 0, 1,  1,  3, 5,  6,  6,  6,  9, 10, 12;"
                           "int32:int32_t: 0, 10, 20, 2, 40, 20, 25, 30, 8, 7,  6";
    fakeIndex.partitionMeta = "+price";
    string resultStr = "9,3,2,1";
    // totalhits is ResultEstimator
    // query1 : 1 2 E; query2: 3 4; query3: 9 E
    internalTestInit(requestStr, fakeIndex, resultStr, "", 5, ERROR_NONE, 7);
}

void RankSearcherTest::internalTestInit(const string &requestStr, const FakeIndex &fakeIndex,
                                        const string &resultStr, const string &aggResultStr,
                                        uint32_t totalHit, ErrorCode errorCode,
                                        int32_t seekDocCount)
{
    common::Ha3MatchDocAllocator *matchDocAllocator = _resource._matchDocAllocator;
    IndexPartitionReaderWrapperPtr wrapperPtr = 
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _searcher->_indexPartReaderPtr = wrapperPtr->getReader();
        
    RequestPtr requestPtr = RequestCreator::prepareRequest(requestStr, "phrase");
    ASSERT_TRUE(requestPtr);
    RequestValidator requestValidator(_clusterTableInfoMapPtr,
            10000, _clusterConfigMapPtr, ClusterFuncMapPtr(new ClusterFuncMap), CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));
    ASSERT_TRUE(requestValidator.validate(requestPtr));

    FakeAttributeExpressionCreator creator(_pool.get(), wrapperPtr, NULL, NULL, NULL, NULL, NULL);
    ConfigClause *configClause = requestPtr->getConfigClause();
    uint32_t rankSize = numeric_limits<uint32_t>::max();
    if (configClause->getRankSize() > 0) {
        rankSize = configClause->getRankSize();
    }
    MultiErrorResultPtr errorResultPtr(new MultiErrorResult());
    bool expectInitResult = (errorCode == ERROR_NONE);
    wrapperPtr->setTopK(1000);
    Tracer tracer;
    tracer.setLevel("DEBUG");
    RankSearcherParam param;
    param.request = requestPtr.get();
    param.attrExprCreator = &creator;
    param.matchDataManager = &_matchDataManager;
    param.readerWrapper = wrapperPtr.get();
    param.rankSize = rankSize;
    param.timeoutTerminator = NULL;
    param.tracer = &tracer;
    param.errorResultPtr = errorResultPtr;
    param.matchDocAllocator = _allocatorPtr.get();
    LayerMetasPtr layerMetas = LayerMetaUtil::createLayerMetas(
            requestPtr->getQueryLayerClause(),
            requestPtr->getRankClause(),
            wrapperPtr.get(),
            _pool.get());
    param.layerMetas = layerMetas.get();

    ASSERT_TRUE(expectInitResult == _searcher->init(param));
    _searcher->createJoinFilter(_converterFactory, configClause->getJoinType());
    if (errorCode != ERROR_NONE) {
        ASSERT_TRUE(errorResultPtr->getErrorCount() > 0);
        ASSERT_EQ(errorCode, errorResultPtr->getErrorResults()[0].getErrorCode());
        return;
    }
    initHitCollector();
    RankSearcherResource resource;
    resource._requiredTopK = configClause->getStartOffset() + configClause->getHitCount();
    resource._rankSize = rankSize;
    resource._matchDocAllocator = matchDocAllocator;
    resource._sessionMetricsCollector = _sessionMetricsCollector.get();
    ASSERT_EQ(totalHit, _searcher->search(resource, _hitCollectorPtr.get()));

    checkAttrExpr(_searcher);
    checkResult(resultStr, _hitCollectorPtr);
    if (!aggResultStr.empty()) {
        ASSERT_TRUE(_searcher->_aggregator);
        common::AggregateResultsPtr aggResults;
        _searcher->collectAggregateResults(aggResults);
        ASSERT_TRUE(aggResults);
        SearcherTestHelper::checkAggregatorResult(*aggResults.get(), aggResultStr);
    }
    if (seekDocCount != -1) {
        ASSERT_EQ(seekDocCount, resource._sessionMetricsCollector->getSeekDocCount());
    }
}

void RankSearcherTest::checkAttrExpr(RankSearcher *searcher) {
    if (_hitAttrExpr) {
        ASSERT_TRUE(_hitAttrExpr->isEvaluated());
    }
}

void RankSearcherTest::initTableInfos() {
    TableInfoConfigurator tableInfoConfigurator;
    TableInfoPtr tableInfoPtr = tableInfoConfigurator.createFromFile(TEST_DATA_PATH"/searcher_test_schema.json");

    ASSERT_TRUE(tableInfoPtr);

    //modify the 'Analyzer' setting of 'phrase' index. (temp solution)
    IndexInfos *indexInfos = (IndexInfos *)tableInfoPtr->getIndexInfos();
    ASSERT_TRUE(indexInfos);
    IndexInfo *indexInfo = (IndexInfo *)indexInfos->getIndexInfo("phrase");
    ASSERT_TRUE(indexInfo);
    indexInfo->setAnalyzerName("default");
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    (*_clusterTableInfoMapPtr)["simple.default"] = tableInfoPtr;
}

void RankSearcherTest::initClusterConfigMap() {
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
    ClusterConfigInfo clusterInfo1;
    clusterInfo1._tableName = "table_name";
    _clusterConfigMapPtr->insert(make_pair("simple.default", clusterInfo1));
}

/////////////////////begin test multi layer search//////////////////////

TEST_F(RankSearcherTest, testSearchWithMultiLayer) {
    HA3_LOG(DEBUG, "Begin Test!");
    initHitCollector();
    createQueryExecutors(*_searcher, "1,2,3,4|3,4,6,7");
    initFilter(*_searcher, "1,2,6,7");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,3|5,7,2");

    ASSERT_EQ((uint32_t)4, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("7,6,2,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testSwitchLayer) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "1,2,3,4|3,4,6,7");
    initFilter(*_searcher, "1,2,6,7");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,5|5,7,1");

    ASSERT_EQ((uint32_t)4, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));
    checkResult("7,6,2,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testSwitchLayerWithoutFilter) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "1,2,3,4|3,4,6,7");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,5|7,7,1");

    ASSERT_EQ((uint32_t)5, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("7,4,3,2,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testSwitchLayerOutofRange) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "10,12,13,14|13,14,16,17");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,5|7,7,1");

    ASSERT_EQ((uint32_t)0, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testMultiLayerWithMultiRange) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "1,4,6,8,9,13,14,16,17|1,4,6,8,9,13,14,16,17");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,5;13,15,1|7,9,1");

    ASSERT_EQ((uint32_t)6, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("14,13,9,8,4,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testSearchWithSingleLayer) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "1,2,3,4");
    initFilter(*_searcher, "1,2,6,7");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,3");

    ASSERT_EQ((uint32_t)2, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("2,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testMatchDocCountPrecent) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher,
                         "1,4,7,12,16,18,22,23,24,25,85,93,100,200,300"
                         "|1,4,7,12,16,18,22,23,24,25,85,93,100,200,300"
                         "|1,4,7,12,16,18,22,23,24,25,85,93,100,200,300");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "param,2;11,13,0;15,19,0|param,2;1,5,0;7,9,0|param,7;22,33,0;44,55,0");

    ASSERT_EQ((uint32_t)12, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("25,24,23,22,16,12,4,1", _hitCollectorPtr);
}

TEST_F(RankSearcherTest, testMultiLayerQuota) {
    HA3_LOG(DEBUG, "Begin Test!");

    initHitCollector();
    createQueryExecutors(*_searcher, "1-50,55-60|1-50,55-60|1-50,55-60");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "param,41;1,40,0;50,55,0|param,1;25,30,0|param,3;55,60,0");

    ASSERT_EQ(uint32_t(52), _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));

    checkResult("59-55,50,39-1", _hitCollectorPtr);
}

/////////////////////begin test multi layer search//////////////////////

TEST_F(RankSearcherTest, testSearchAggregator) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeAttributeExpressionCreator creator(_pool.get(), _reader, NULL, NULL, NULL, NULL, NULL);
    FakeAttributeExpressionFactory *facotry =
        POOL_NEW_CLASS(_pool.get(), FakeAttributeExpressionFactory, "attribute",
                       "1,1,1,1,1,1,2,2", _pool.get());
    creator.resetAttrExprFactory(facotry);
    AggregatorCreator aggCreator(&creator, _pool.get());

    AggregateClause aggClause;
    string aggDescStr = "group_key:attribute, agg_fun:count()";
    queryparser::ClauseParserContext ctx;
    ASSERT_TRUE(ctx.parseAggClause(aggDescStr.c_str()));
    AggregateDescription *aggDescription = ctx.stealAggDescription();
    aggDescription->getGroupKeyExpr()->setMultiValue(false);
    aggDescription->getGroupKeyExpr()->setExprResultType(vt_int32);
    aggClause.addAggDescription(aggDescription);
    _searcher->_aggregator = aggCreator.createAggregator(&aggClause);

    ASSERT_TRUE(_searcher->_aggregator);
    initHitCollector();
    createQueryExecutors(*_searcher, "1,2,3,4|3,4,6,7");
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool.get(), "1,4,3|5,7,2");

    ASSERT_EQ((uint32_t)6, _searcher->searchMultiLayers(_resource,
                    _hitCollectorPtr.get(), &layerMetas));
    checkResult("7,6,3,2,1", _hitCollectorPtr);

    typedef VariableTypeTraits<vt_int32, false>::AttrExprType T;
    typedef VariableTypeTraits<vt_int32, false>::AttrExprType AttrT;
    NormalAggregator<T, AttrT> *normalAgg = dynamic_cast<NormalAggregator<T, AttrT>*>(_searcher->_aggregator);
    ASSERT_TRUE(normalAgg);
    const NormalAggregator<int32_t>::FunctionVector &functions = normalAgg->getAggregateFunctions();
    ASSERT_EQ((size_t)1, functions.size());
    ASSERT_EQ(string("count"), functions[0]->getFunctionName());

    common::AggregateResultsPtr aggResults = normalAgg->collectAggregateResult();
    (*aggResults)[0]->constructGroupValueMap();
    int64_t count = 0;
    count = (*aggResults)[0]->getAggFunResult<int64_t>("1", 0, 0);
    ASSERT_EQ(int64_t(4), count);
    count = (*aggResults)[0]->getAggFunResult<int64_t>("2", 0, 0);
    ASSERT_EQ(int64_t(2), count);
}

/*
 * [group1]group_key1:funcValue1,funcValue2;group_key2:funcValue1,funcValue2
 * [group2]other_group_key1:funcValue1,funcValue2;other_group_key2:funcValue1,funcValue2
 */

void RankSearcherTest::checkResult(const string &resultStr,
                                   HitCollectorPtr &hitCollectorPtr)
{
    vector<docid_t> results = SearcherTestHelper::covertToResultDocIds(resultStr);
    ASSERT_EQ((uint32_t)results.size(), hitCollectorPtr->getItemCount());
    const ComboComparator *cmp = hitCollectorPtr->getComparator();
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> matchDocs(_pool.get());
    hitCollectorPtr->stealAllMatchDocs(matchDocs);
    ASSERT_EQ(matchDocs.size(), results.size());
    MatchDocComp comp(cmp);
    sort(matchDocs.begin(), matchDocs.end(), comp);
    for (size_t i = 0; i < matchDocs.size() ; ++i) {
        HA3_LOG(INFO, "expect result is [%s]", resultStr.c_str());
        assert(results[i] == matchDocs[i].getDocId());
        _resource._matchDocAllocator->deallocate(matchDocs[i]);
    }
}

void RankSearcherTest::initFilter(RankSearcher &searcher, const string &filterDocsStr) {
    _values = new vector<bool> (MAX_DOC_COUNT, false);

    if (!filterDocsStr.empty()) {
        StringTokenizer st(filterDocsStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            (*_values)[StringUtil::fromString<docid_t>(st[i].c_str())] = true;
        }
        _attrExpr = new FakeAttributeExpression<bool>("filter", _values);
        Filter *filter = POOL_NEW_CLASS(_pool.get(), Filter, _attrExpr);
        searcher._filterWrapper = POOL_NEW_CLASS(_pool.get(), FilterWrapper);
        searcher._filterWrapper->setFilter(filter);
    }
}

void RankSearcherTest::initHitCollector() {
    _floatValues = new vector<float>(MAX_DOC_COUNT);
    for (size_t i = 0; i < MAX_DOC_COUNT; ++i) {
        (*_floatValues)[i] = i;
    }
    _hitAttrExpr = new FakeAttributeExpression<float>("float", _floatValues);
    ASSERT_TRUE(_hitAttrExpr->allocate(_resource._matchDocAllocator));
    
    ComparatorCreator creator(_pool.get(), false);
    SortExpression *sortExpr = 
        _sortExprCreator->createSortExpression(_hitAttrExpr, false);
    SortExpressionVector sortExprVec;
    sortExprVec.push_back(sortExpr);
    _hitCollectorPtr.reset(new HitCollector(_resource._requiredTopK, _pool.get(),
                    _allocatorPtr, _hitAttrExpr,
                    creator.createSortComparator(sortExprVec)));
}

void RankSearcherTest::createQueryExecutors(RankSearcher &searcher,
        const string &docsInIndex)
{
    StringTokenizer st(docsInIndex, "|", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        searcher._queryExecutors.push_back(
                POOL_NEW_CLASS(_pool.get(), QueryExecutorMock, st[i]));
    }
}

END_HA3_NAMESPACE(seacrh);

