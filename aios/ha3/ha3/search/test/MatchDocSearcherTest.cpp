#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/AggregateClause.h>
#include <ha3/common/Hits.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/index/index.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/test/SearcherTestHelper.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <autil/StringUtil.h>
#include <ha3/common/Term.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/Hits.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <ha3/common/Request.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/common/QueryClause.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/RankClause.h>
#include <ha3/common/SortDescription.h>
#include <ha3/common/AggregateResult.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/rank/RankProfileManager.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <ha3/search/test/FakeTimeoutTerminator.h>
#include <ha3_sdk/testlib/index/FakeIndexReader.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <ha3/common/Tracer.h>
#include <ha3/proxy/Merger.h>
#include <ha3/sorter/DefaultSorter.h>
#include <iostream>
#include <ha3/search/test/QueryExecutorConstructor.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>
#include <ha3/search/PKQueryExecutor.h>
#include <ha3/qrs/MatchDocs2Hits.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <ha3/qrs/RequestValidator.h>
#include <ha3/queryparser/SyntaxExpr.h>
#include <ha3/qrs/QueryFlatten.h>
#include <ha3/service/SearcherResource.h>
#include <suez/turing/expression/plugin/SorterModuleFactory.h>
#include <ha3/search/test/QueryExecutorTestHelper.h>
#include <ha3/search/test/MatchDocSearcherTest.h>
#include <build_service/analyzer/BuildInTokenizerFactory.h>
#include <build_service/analyzer/Tokenizer.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <build_service/analyzer/Tokenizer.h>

#include <unordered_map>

using namespace std;
using namespace std::tr1;
using namespace autil::mem_pool;
using namespace autil;
using namespace suez::turing;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(qrs);
USE_HA3_NAMESPACE(sorter);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(proxy);

using namespace build_service::plugin;
using namespace build_service::analyzer;

IE_NAMESPACE_USE(common);

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, MatchDocSearcherTest);

#define CHECK_ERROR(errorCode)                                          \
    ErrorResults  errors = result->getMultiErrorResult()->getErrorResults(); \
    ASSERT_TRUE(result->hasError());                                \
    ASSERT_EQ(errorCode, errors[0].getErrorCode())


//static (init 'AliWS' take too much time)
build_service::analyzer::AnalyzerFactoryPtr MatchDocSearcherTest::_analyzerFactoryPtr = MatchDocSearcherTest::initFactory();
MatchDocSearcherTest::MatchDocSearcherTest() {
    _queryParser = NULL;
    _ctx = NULL;
    _aggSamplerConfigInfo = NULL;
    _requestTracer = NULL;
    _pool = new autil::mem_pool::Pool(1024);
}

MatchDocSearcherTest::~MatchDocSearcherTest() {
    _partitionReaderPtr.reset();
    delete _pool;
}

void MatchDocSearcherTest::initRankProfileManager(
        RankProfileManagerPtr &rankProfileMgrPtr, uint32_t rankSize,
        uint32_t rerankSize)
{
    PlugInManagerPtr nullPtr;
    rankProfileMgrPtr.reset(new RankProfileManager(nullPtr));
    ScorerInfo scorerInfo;
    {
        scorerInfo.scorerName = "DefaultScorer";
        scorerInfo.rankSize = rankSize;
        RankProfile *rankProfile = new RankProfile(DEFAULT_RANK_PROFILE);
        rankProfile->addScorerInfo(scorerInfo);
        scorerInfo.rankSize = rerankSize;
        rankProfile->addScorerInfo(scorerInfo); //two scorer
        rankProfileMgrPtr->addRankProfile(rankProfile);
    }
    {
        scorerInfo.scorerName = "DefaultScorer";
        RankProfile *rankProfile = new RankProfile("OneScorerProfile");
        scorerInfo.rankSize = rankSize;
        rankProfile->addScorerInfo(scorerInfo);
        rankProfileMgrPtr->addRankProfile(rankProfile);
    }
    {
        scorerInfo.scorerName = DEFAULT_DEBUG_SCORER;
        scorerInfo.rankSize = rankSize;
        RankProfile *rankProfile = new RankProfile(DEFAULT_DEBUG_RANK_PROFILE );
        rankProfile->addScorerInfo(scorerInfo);
        rankProfileMgrPtr->addRankProfile(rankProfile);
    }
    ResourceReaderPtr resourceReader;
    rankProfileMgrPtr->init(resourceReader, _cavaPluginManagerPtr, NULL);
}

RequestPtr MatchDocSearcherTest::prepareRequest(const string &query,
        const TableInfoPtr &tableInfoPtr,
        const MatchDocSearcherCreatorPtr &matchDocSearcherCreator,
        const string &defaultIndexName)
{
    RequestPtr requestPtr = RequestCreator::prepareRequest(query,
            defaultIndexName);
    if (!requestPtr) {
        return requestPtr;
    }

    RequestValidateProcessor::fillPhaseOneInfoMask(requestPtr->getConfigClause(),
            tableInfoPtr->getPrimaryKeyIndexInfo());

    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    ClusterConfigInfo clusterInfo1;
    clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterInfo1));

    ClusterFuncMapPtr clusterFuncMap(new ClusterFuncMap);
    // FuncConfigManager funcConfigManager;
    // map<string, FuncConfig> funcConfigMap;
    // funcConfigMap["cluster1.default"] = FuncConfig();
    // funcConfigManager.getFuncInfoMap(funcConfigMap, ResourceReaderPtr(), *clusterFuncMap);
    (*clusterFuncMap)["cluster1.default"] =
        matchDocSearcherCreator->_funcCreatorPtr->getFuncInfoMap();
    RequestValidator requestValidator(clusterTableInfoMapPtr,
            10000, clusterConfigMapPtr,
            clusterFuncMap, CavaPluginManagerMapPtr(),
            ClusterSorterNamesPtr(new ClusterSorterNames));
    bool ret = requestValidator.validate(requestPtr);
    if (!ret) {
        return RequestPtr();
    }

    QueryTokenizer tokenizer(_analyzerFactoryPtr.get());
    ConfigClause configClause;
    tokenizer.setConfigClause(&configClause);
    QueryClause *queryClause = requestPtr->getQueryClause();
    for (size_t i = 0; i < queryClause->getQueryCount(); ++i) {
        tokenizer.tokenizeQuery(queryClause->getRootQuery(i), tableInfoPtr->getIndexInfos());
        Query *tokenizedQuery = tokenizer.stealQuery();
        assert(tokenizedQuery);
        queryClause->setRootQuery(tokenizedQuery, i);
    }

    return requestPtr;
}

Request *MatchDocSearcherTest::prepareRequest() {
    Request *request = new Request;

    QueryClause *queryClause = new QueryClause(NULL);
    request->setQueryClause(queryClause);

    ConfigClause *configClause = new ConfigClause();
    request->setConfigClause(configClause);

    SortClause *sortClause = new SortClause();
    SyntaxExpr *sortExpr = new AtomicSyntaxExpr("price", vt_int32, ::ATTRIBUTE_NAME);
    sortClause->addSortDescription(sortExpr, false);
    request->setSortClause(sortClause);

    SyntaxExpr *distExpr = new AtomicSyntaxExpr("type", vt_int32, ::ATTRIBUTE_NAME);
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "type", 1, 1, 0, true, false, distExpr, NULL);
    DistinctClause *distClause = new DistinctClause;
    distClause->addDistinctDescription(distDesc);
    request->setDistinctClause(distClause);

    RankClause *rankClause = new RankClause();
    rankClause->setRankProfileName("DefaultProfile");
    request->setRankClause(rankClause);

    return request;
}

void MatchDocSearcherTest::setUp() {
    HA3_LOG(INFO, "Set Up");
    _matchDocSearcherCreatorPtr.reset(new MatchDocSearcherCreator(_pool));

    _fakeIndex.clear();

    //make 'Request'
    _request.reset(prepareRequest());
    _request->setPool(_pool);

    //make IndexPartitionReader
    FakeIndex fakeIndex;
    fakeIndex.versionId = 1;
    fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
    fakeIndex.attributes = "int8 : int8_t : 1, 1, 1, 1, 1, 2;"
                           "type : int32_t : 1, 1, 1, 2, 2, 2;"
                           "price : int32_t : 1, 2, 6, 3, 4, 5;";
    _partitionReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    // create empty snapshot
    _snapshotPtr.reset(new PartitionReaderSnapshot(&_emptyTableMem2IdMap,
                    &_emptyTableMem2IdMap, &_emptyTableMem2IdMap,
                    std::vector<IndexPartitionReaderInfo>()));
    //make 'TableInfo'
    _tableInfoPtr = _matchDocSearcherCreatorPtr->makeFakeTableInfo();

    //init 'RankProfileManager'
    initRankProfileManager(_rankProfileMgrPtr);

    _queryParser = new QueryParser("phrase");

    _aggSamplerConfigInfo = new AggSamplerConfigInfo();
    _requestTracer = NULL;
}

void MatchDocSearcherTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    DELETE_AND_SET_NULL(_queryParser);
    DELETE_AND_SET_NULL(_ctx);
    DELETE_AND_SET_NULL(_aggSamplerConfigInfo);
    DELETE_AND_SET_NULL(_requestTracer);
    _matchDocSearcherCreatorPtr.reset();
    _snapshotPtr.reset();
    _indexPartitionMap.clear();
    _indexApp.reset();
    if (!_testPath.empty()) {
        string cmd = "rm -r " + _testPath + "*" ;
        system(cmd.c_str());
    }
}

TEST_F(MatchDocSearcherTest, testSearchNoResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "a:0,1,2,3;\n";
    string query = "config=cluster:cluster1&&query=no_such_term";
    innerTestQuery(query, "", 0);
}

TEST_F(MatchDocSearcherTest, testSearchResultExceptionIndex) {
    ASSERT_TRUE(_request);
    //prepare exception partition reader

    FakeIndexReader *indexReader = new ExceptionIndexReader();
    FakeIndexPartitionReaderPtr fakePartitionReaderPtr =
        std::tr1::dynamic_pointer_cast<FakeIndexPartitionReader>(
                _partitionReaderPtr->getReader());
    assert(fakePartitionReaderPtr);
    IndexReaderPtr indexReaderPtr(indexReader);
    fakePartitionReaderPtr->SetIndexReader(indexReaderPtr);

    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));

    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(
            _request.get(), _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    ResultPtr result = searcher->search(_request.get());

    ASSERT_TRUE(result);
    CHECK_ERROR(ERROR_SEARCH_LOOKUP_FILEIO_ERROR);

    string errorMsg = "ExceptionBase: FileIOException";

    ASSERT_EQ(errorMsg, errors[0].getErrorMsg());
    const MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());
    ASSERT_EQ((uint32_t)0, matchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)0, matchDocs->actualMatchDocs());
}

TEST_F(MatchDocSearcherTest, testSearchRankProfileNotExist) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_request);
    _request->setSortClause(NULL);
    RankClause *rankClause = _request->getRankClause();
    rankClause->setRankProfileName("not_exist_rank_profile");
    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));
    ConfigClause *configClause = _request->getConfigClause();
    configClause->setStartOffset(0);
    configClause->setHitCount(10);
    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(
            _request.get(), _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    ResultPtr result = searcher->search(_request.get());
    ASSERT_TRUE(result);
    CHECK_ERROR(ERROR_NO_RANKPROFILE);
    ASSERT_EQ(string("Get rank profile failed, profileName=not_exist_rank_profile"),
                         errors[0].getErrorMsg());
    const MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());
    ASSERT_EQ((uint32_t)0, matchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)0, matchDocs->actualMatchDocs());
}

TEST_F(MatchDocSearcherTest, testSearchDebugRankProfile) {
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1, 2];2[2, 3, 4];3[1, 2, 3, 4];4[1, 2, 3, 4, 5];5[1, 2, 3, 4, 5, 6];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1,rerank_hint:true,debug_query_key:123&&query=phrase:with&&sort=RANK";
    ResultPtr result = innerTestQuery(query, "0,1,2,3,4,5", 6);
    char sep = ':';
    string checkStr = string("phrase:with") + sep + string("1");
    checkTrace(result, 0, checkStr,"");
    checkTrace(result, 1, checkStr,"");
    query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:with&&sort=RANK";
    innerTestQuery(query, "5, 4 ,3, 2, 1, 0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndDistinct_1_1) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1&&sort=price";
    innerTestQuery(query, "2,5,4,3,1,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortKeyInFinalSort) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1&&sort=price"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#+price";
    innerTestQuery(query, "0,3,1,4,5,2", 6);
}


TEST_F(MatchDocSearcherTest, testSearchWithSortAndGradeDistinct_1_1) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1,grade:5.0&&sort=price";
    innerTestQuery(query, "2,5,4,1,3,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndDistinctWithSameExpr) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1,reserved:false&&sort=+type";
    innerTestQuery(query, "0,3", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndDistinctNotReserveExtraResults_1_1) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1,reserved:false&&sort=price";
    innerTestQuery(query, "2,5", 6);
}

TEST_F(MatchDocSearcherTest, testSearchExtraRank) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1,reserved:false;"
                   "dist_key:type,dist_count:1,dist_times:1,max_item_count:100,reserved:false,update_total_hit:true&&sort=price";
    innerTestQuery(query, "2,5", 6);
}

TEST_F(MatchDocSearcherTest, testSearchExtraRankDoNotUpdateTotalHit) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,"
                   "dist_times:1,max_item_count:100,reserved:false,update_total_hit:true&&sort=price";
    innerTestQuery(query, "2,5", 2);
}

TEST_F(MatchDocSearcherTest, testSearchExtraRankDoNotUpdateTotalHitMaxItemCount0) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1,hit:2&&query=phrase:with&&distinct=dist_key:type,dist_count:1,"
                   "dist_times:1,reserved:false,update_total_hit:true&&sort=+price";
    innerTestQuery(query, "0,3", 4);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndGradeDistinctNotReserveExtraResults_1_1) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:1,"
                   "dist_times:1,reserved:false,grade:4.0&&sort=price";
    innerTestQuery(query, "2,5,3,1", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndDistinct_2_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:17,cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:2,"
                   "dist_times:2&&sort=price";
    innerTestQuery(query, "2,5,4,1,3,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndGradeDistinct_2_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:17,cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:2,"
                   "dist_times:2,grade:4.0&&sort=price";
    innerTestQuery(query, "2,5,4,3,1,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndFilterDistinct) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:17,cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:2,"
                   "dist_times:2,dist_filter:price<3&&sort=price";
    innerTestQuery(query, "2,5,4,1,0,3", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndDistinctNotReserveExtraResults_1_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:17,cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:2,"
                   "dist_times:1,reserved:false&&sort=price";
    innerTestQuery(query, "2,5,4,1", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSortAndGradeDistinctNotReserveExtraResults_1_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:17,cluster:cluster1&&query=phrase:with&&distinct=dist_key:type,dist_count:2,"
                   "dist_times:1,reserved:false,grade:4.0&&sort=price";
    innerTestQuery(query, "2,5,4,3,1,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchTermQueryWithIndexSwitchLimit) {
    HA3_LOG(DEBUG, "Begin Test!");

    //has truncateName, totalMatchDocs:10000
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];";
        _fakeIndex.indexes["weight"] = "abc:0[1];1[1];3[2];4[5];";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";
        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:3;quota:7"
                       "&&query=phrase:abc;phrase:abc#price"
                       "&&aggregate=group_key:type,agg_fun:count();"
                       "group_key:type,agg_fun:count()";
        string aggResultStr = "1:10000\n"
                              "1:10000\n";
        innerTestQuery(query, "0,1,2,5", 10000, aggResultStr);
    }
    tearDown();
    setUp();

    //no truncateName
    {
        _fakeIndex.indexes["phrase"] = "茶:1[4];2[7];3[10];4[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";
        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:3;quota:7"
                       "&&query=phrase:茶&&aggregate=group_key:type,agg_fun:count()";
        string aggResultStr = "1:5000;"
                              "2:2500";
        innerTestQuery(query, "1,2,3,4", 7500, aggResultStr);
    }
    tearDown();
    setUp();
    //indexSwitchLimit = 0
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];";
        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:0;quota:10"
                       "&&query=phrase:abc;phrase:abc#price";
        innerTestQuery(query, "0,1,2,5", 6);
    }
}

TEST_F(MatchDocSearcherTest, testIndexSwitchLimitWithUpackMatchData) {
    HA3_LOG(DEBUG, "Begin Test!");
    //or query
    {
        _fakeIndex.indexes["phrase"] = "unpack:0[1];1[1];4[2];7[1, 2];\n"
                                       "matchdata:4[1];5[1];6[2];\n";
        _fakeIndex.indexes["price"] = "unpack:4[2];7[1,2];\n"
                                      "matchdata:5[1];6[2];\n";
        _fakeIndex.attributes = "int32:int32_t:0,1,2,3,4,5,6,7,8,9";
        string query = "config=cluster:cluster1&&layer=quota:2;quota:8&&sort=+int32"
                       "&&query=unpack OR matchdata;unpack#price OR matchdata#price";
        innerTestQuery(query, "0,1,4,5,6,7", 10000);
    }
    tearDown();
    setUp();
    //rank query
    {
        _fakeIndex.indexes["phrase"] = "unpack:0[1];1[1];4[2];7[1, 2];\n"
                                       "matchdata:4[1];5[1];6[2];\n";
        _fakeIndex.indexes["price"] = "unpack:4[2];7[1,2];\n"
                                      "matchdata:5[1];6[2];\n";
        _fakeIndex.attributes = "int32:int32_t:0,1,2,3,4,5,6,7,8,9";
        string query = "config=cluster:cluster1&&layer=quota:2;quota:8&&sort=+int32"
                       "&&query=phrase:unpack RANK matchdata;"
                       "phrase:unpack#price RANK matchdata#price";
        innerTestQuery(query, "0,1,4,7", 10000);
    }
}

TEST_F(MatchDocSearcherTest, testSearchMultiQueryWithIndexSwitchLimit) {
    HA3_LOG(DEBUG, "Begin Test!");

    // term1#price and term2#price, term2 has no truncateName
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n"
                                       "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";
        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:3;quota:7"
                       "&&query=abc AND with;abc#price AND with#price&&"
                       "aggregate=group_key:type,agg_fun:count()";
        string aggResultStr = "1:10000";
        innerTestQuery(query, "0,1,2,5", 10000, aggResultStr);
    }
    tearDown();
    setUp();

    // term1#price and term2#price, both have truncateName
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n"
                                       "3:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];\n"
                                      "3:0[1];1[1];2[2];3[4];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";

        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:2;quota:8"
                       "&&query=abc AND '3';abc#price AND '3'#price";
        innerTestQuery(query, "0,1,2", 10000);
    }
    tearDown();
    setUp();
    // term1#price or term2#price, both have truncateName, term2 is num term
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n"
                                       "3:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];\n"
                                      "3:0[1];1[1];2[2];3[4];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";

        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:3;quota:7"
                       "&&query=phrase:abc OR '3';phrase:abc#price OR '3'#price&&"
                       "aggregate=group_key:type,agg_fun:count()";
        string aggResultStr = "1:10000";
        innerTestQuery(query, "0,1,2,3,5", 10000, aggResultStr);
    }
    tearDown();
    setUp();
    //test phrase query
    {
        _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n"
                                       "def:0[2];1[2];2[3];3[3];4[2];5[6];\n";
        _fakeIndex.indexes["price"] = "abc:0[1];1[1];2[2];5[5];\n"
                                      "def:0[2];1[2];2[3];4[6];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;";

        string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:2;quota:8"
                       "&&query=phrase:\"abc def\";phrase:\"abc def\"#price&&"
                       "aggregate=group_key:type,agg_fun:count()";
        innerTestQuery(query, "0,1,2", 10000);
    }
}

TEST_F(MatchDocSearcherTest, testSearchQueryWithIndexSwitchLimitAndRankSize) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];";
    _fakeIndex.indexes["weight"] = "abc:0[1];1[1];3[2];4[5];";

    string query = "config=cluster:cluster1,rank_size:3,rerank_hint:true&&layer=quota:2;quota:1"
                   "&&query=phrase:abc;phrase:abc#weight";
    innerTestQuery(query, "0,1,3", 10000);
}

TEST_F(MatchDocSearcherTest, testSearchWithVirtualAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    //test filter
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                                "id  :  int32_t : 1, 2, 3, 4, 5, 6;"
                                "price: int32_t : 1, 2, 6, 3, 4, 5;";
        string query = "config=cluster:cluster1&&query=phrase:with"
                       "&&virtual_attribute=va1:id>4&&filter=va1"
                       "&&sort=price&&distinct=dist_key:type,dist_count:1,dist_times:1";
        innerTestQuery(query, "5,4", 2);
    }
    tearDown();
    setUp();
    //test sort and distinct
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                                "price: int32_t : 1, 2, 6, 3, 4, 5;";
        string query = "config=cluster:cluster1&&query=phrase:with"
                       "&&virtual_attribute=va1:price;va2:type"
                       "&&sort=va1&&distinct=dist_key:va2,dist_count:1,dist_times:1";
        innerTestQuery(query, "2,5,4,3,1,0", 6);
    }
    tearDown();
    setUp();
    //test sort and distinct 2
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                                "price: int32_t : 1, 2, 6, 3, 4, 5;"
                                "int32: int32_t : 3, 8, 3, 2, 3, 1";
        string query = "config=cluster:cluster1&&query=phrase:with"
                       "&&virtual_attribute=va1:price;va2:type;va3:int32;va4:va3+va1"
                       "&&sort=va4&&distinct=dist_key:va2,dist_count:1,dist_times:1";
        innerTestQuery(query, "1,4,2,5,3,0", 6);
    }
    tearDown();
    setUp();
    //test sort and distinct 3
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                                "price: int32_t : 1, 2, 6, 3, 4, 5;"
                                "int32: int32_t : 3, 8, 3, 2, 3, 1";
        string query = "config=cluster:cluster1&&query=phrase:with"
                       "&&virtual_attribute=va1:price;va2:type;va3:int32;"
                       "&&sort=va1+va3&&distinct=dist_key:va1+va3,dist_count:1,dist_times:1";
        innerTestQuery(query, "1,2,4,5,3,0", 6);
    }
    tearDown();
    setUp();
    //test aggregate
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                                "price: int32_t : 1, 2, 6, 3, 4, 5;";
        string query = "config=cluster:cluster1&&query=phrase:with"
                       "&&virtual_attribute=va1:type&&aggregate=group_key:va1,agg_fun:count()"
                       "&&sort=price&&distinct=dist_key:type,dist_count:1,dist_times:1";
        string aggResultStr = "1:3;2:3";
        innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
    }
}

TEST_F(MatchDocSearcherTest, testSearchWithSimpleFilterClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "id  :  int32_t : 1, 2, 3, 4, 5, 6;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&filter=id>4&&query=phrase:with&&sort=price";
    innerTestQuery(query, "5,4", 2);
}

TEST_F(MatchDocSearcherTest, testRemoveFilteredMatchDoc) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "id  :  int32_t : 1, 2, 3, 4, 5, 6;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1,hit:2&&filter=id>4&&query=phrase:with&&sort=price";
    innerTestQuery(query, "5,4", 2);
}

TEST_F(MatchDocSearcherTest, testSearchWithComplicatedFilterClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "id  :  int32_t : 1, 2, 3, 4, 5, 6;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";

    string query = "config=cluster:cluster1&&filter=id < 5 AND id > 2&&query=phrase:with&&sort=price";
    innerTestQuery(query, "2,3", 2);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&aggregate=group_key:type,agg_fun:count()&&query=phrase:with&&sort=price";
    string aggResultStr = "1:3;"
                          "2:3;";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClauseTwoMaxFun) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:    int32_t : 1, 1, 1, 2, 2, 2;"
                            "price:   int32_t : 1, 2, 6, 3, 4, 5;"
                            "shop_id: int64_t : 11,22,33,33,55,66";
    string query = "config=cluster:cluster1&&aggregate=group_key:type,agg_fun:count()#max(price)#max(shop_id)&&query=phrase:with&&sort=price";
    string aggResultStr = "1:3, 6, 33;"
                          "2:3, 5, 66;";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClauseWithSampleByConifg) {
    HA3_LOG(DEBUG, "Begin Test!");
    _aggSamplerConfigInfo->setAggThreshold(3);
    _aggSamplerConfigInfo->setSampleStep(2);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&aggregate=group_key:type,agg_fun:count()&&query=phrase:with&&sort=price";
    string aggResultStr = "1:3;"
                          "2:2;";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClauseWithSampleByRequest) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&aggregate=group_key:type,agg_sampler_threshold:3,agg_sampler_step:2,"
                   "agg_fun:count()&&query=phrase:with&&sort=price";
    string aggResultStr = "1:3;"
                          "2:2;";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClauseWithSampleByRequest2) {
    HA3_LOG(DEBUG, "Begin Test!");

    _aggSamplerConfigInfo->setAggThreshold(3);
    _aggSamplerConfigInfo->setSampleStep(2);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";

    string query = "config=cluster:cluster1&&aggregate=group_key:type,agg_sampler_threshold:0,agg_sampler_step:1,"
                   "agg_fun:count()&&query=phrase:with&&sort=price";
    string aggResultStr = "1:3;"
                          "2:3;";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testTwoPhaseRank) {
    HA3_LOG(DEBUG, "Begin Test!");

    initRankProfileManager(_rankProfileMgrPtr, numeric_limits<uint32_t>::max(), 100);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    string query = "config=cluster:cluster1,hit:2,rerank_hint:true&&query=phrase:with";
    innerTestQuery(query, "3,0", 6);
}

TEST_F(MatchDocSearcherTest, testTwoPhaseRankAndDistinct) {
    HA3_LOG(DEBUG, "Begin Test!");

    initRankProfileManager(_rankProfileMgrPtr, numeric_limits<uint32_t>::max(), 100);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 2, 1, 2, 2;";
    string query = "config=cluster:cluster1,hit:4,rerank_hint:true&&query=phrase:with&&distinct=dist_key:type,dist_count:1,dist_times:1";
    innerTestQuery(query, "3,2,0,1", 6);
}

TEST_F(MatchDocSearcherTest, testThreePhaseRank) {
    HA3_LOG(DEBUG, "Begin Test!");
    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));

    ConfigClause *configClause = _request->getConfigClause();
    configClause->setHitCount(2);
    configClause->setNeedRerank(true);

    _request->setSortClause(NULL);//sort by RANK
    _request->setDistinctClause(NULL);

    RankProfileManagerPtr rankProfileMgrPtr(new RankProfileManager(PlugInManagerPtr()));
    RankProfile *rankProfile = new RankProfile("DefaultProfile");

    //phase 1
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "DefaultScorer";
    scorerInfo.rankSize = 1000;
    rankProfile->addScorerInfo(scorerInfo);

    //phase 2
    scorerInfo.rankSize = 10;
    rankProfile->addScorerInfo(scorerInfo);//add scorerInfo again, but different rankSize

    //phase 3
    scorerInfo.rankSize = 5;
    rankProfile->addScorerInfo(scorerInfo);//add scorerInfo again, but different rankSize

    rankProfileMgrPtr->addRankProfile(rankProfile);
    ResourceReaderPtr resourceReader;
    rankProfileMgrPtr->init(resourceReader, _cavaPluginManagerPtr, NULL);
    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(
            _request.get(), _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    ASSERT_EQ(1, searcher->_partCount);
    ASSERT_EQ(&_matchDocSearcherCreatorPtr->_clusterConfigInfo, &searcher->_clusterConfigInfo);
    searcher->_rankProfileMgr = rankProfileMgrPtr.get();

    ResultPtr result = searcher->search(_request.get());
    ASSERT_TRUE(result);

    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);

    uint32_t size = matchDocs->size();
    ASSERT_EQ((uint32_t)2, size);

    ASSERT_EQ((docid_t)3, matchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)0, matchDocs->getMatchDoc(1).getDocId());
}

TEST_F(MatchDocSearcherTest, testSearchWithZeroStartAndHitCount) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;";

    string query = "config=cluster:cluster1,start:0,hit:0&&query=phrase:with&&sort=price";
    innerTestQuery(query, "", 6);
}

TEST_F(MatchDocSearcherTest, testSearchWithSearcherReturnHits) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1,searcher_return_hits:3&&&&query=phrase:with&&sort=price";
    innerTestQuery(query, "2,5,4", 6);
}

TEST_F(MatchDocSearcherTest, testSearchStringIndex) {
    HA3_LOG(DEBUG, "Begin Test");

    _fakeIndex.indexes["titleindex"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1&&query=titleindex:with&&sort=price";
    innerTestQuery(query, "2,5,4,3,1,0", 6);
}

TEST_F(MatchDocSearcherTest, testMatchDataReset) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    string query = "config=cluster:cluster1,searcher_return_hits:3,rerank_hint:true&&query=phrase:with";
    innerTestQuery(query, "3,0,1", 6);
}

TEST_F(MatchDocSearcherTest, testFilterUseInt64Attribute) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "shop_id: int64_t : 11,22,33,33,55,66;";
    string query = "config=cluster:cluster1,fetch_summary_type:pk&&filter=shop_id=33&&query=phrase:with&&sort=price";
    ResultPtr result = innerTestQuery(query, "2,3", 2);

    MatchDocs * matchDocs = result->getMatchDocs();
    matchdoc::Reference<uint64_t> *ref = matchDocs->getPrimaryKey64Ref();
    ASSERT_TRUE(ref);
    ASSERT_EQ((uint64_t)2, ref->getReference(matchDocs->getMatchDoc(0)));
}

TEST_F(MatchDocSearcherTest, testFillPrimaryKey) {
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=cluster:cluster1,fetch_summary_type:pk&&query=phrase:with&&sort=price";
    ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
    MatchDocs * matchDocs = result->getMatchDocs();
    matchdoc::Reference<uint64_t> *ref = matchDocs->getPrimaryKey64Ref();
    ASSERT_TRUE(ref);
    ASSERT_EQ((uint64_t)2, ref->getReference(matchDocs->getMatchDoc(0)));
    ASSERT_EQ((uint64_t)0, ref->getReference(matchDocs->getMatchDoc(5)));
}

TEST_F(MatchDocSearcherTest, testFillRawPrimaryKey) {

    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                                "id   : uint32_t : 1, 2, 3, 4, 5, 6;";
        string query = "config=cluster:cluster1,fetch_summary_type:rawpk&&query=phrase:with&&sort=price";
        ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
        MatchDocs * matchDocs = result->getMatchDocs();
        matchdoc::ReferenceBase *ref = matchDocs->getRawPrimaryKeyRef();
        matchdoc::Reference<uint32_t> *typedRef = dynamic_cast<matchdoc::Reference<uint32_t>*>(ref);
        ASSERT_TRUE(ref);
        ASSERT_TRUE(typedRef);
        ASSERT_EQ((uint32_t)6, typedRef->getReference(matchDocs->getMatchDoc(1)));
        ASSERT_EQ((uint32_t)3, typedRef->getReference(matchDocs->getMatchDoc(0)));
        ASSERT_EQ((uint32_t)1, typedRef->getReference(matchDocs->getMatchDoc(5)));
    }
    {
        _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                                "id   : string :11,22,33,44,55,666;";
        string query = "config=cluster:cluster1,fetch_summary_type:rawpk&&query=phrase:with&&sort=price";
        ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
        MatchDocs * matchDocs = result->getMatchDocs();
        matchdoc::ReferenceBase *ref = matchDocs->getRawPrimaryKeyRef();
        matchdoc::Reference<MultiValueType<char> > *typedRef = dynamic_cast<matchdoc::Reference<MultiValueType<char> >*>(ref);

        ASSERT_TRUE(ref);
        ASSERT_TRUE(typedRef);
        ASSERT_EQ(string("666"),
                             (VariableTypeTraits<vt_string, false>::convert(
                 typedRef->getReference(matchDocs->getMatchDoc(1)))));
        ASSERT_EQ(string("33"),
                             (VariableTypeTraits<vt_string, false>::convert(
                 typedRef->getReference(matchDocs->getMatchDoc(0)))));
        ASSERT_EQ(string("11"),
                             (VariableTypeTraits<vt_string, false>::convert(
                 typedRef->getReference(matchDocs->getMatchDoc(5)))));

    }
}

TEST_F(MatchDocSearcherTest, testFilterUseMultiValueInt32Attribute) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "multi_int32: multi_int32_t : 1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5";
    string query = "config=cluster:cluster1&&filter=multi_int32=4&&query=phrase:with&&sort=price";
    innerTestQuery(query, "2,5", 2);
}

TEST_F(MatchDocSearcherTest, testSearchWithAggregateClauseWithMultiValueKey) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "multi_int32: multi_int32_t : 1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5";
    string query = "config=cluster:cluster1&&aggregate=group_key:multi_int32,agg_fun:count()&&query=phrase:with&&sort=price";
    string aggResultStr = "1:6;"
                          "2:3;"
                          "3:5;"
                          "4:2;"
                          "5:2";
    innerTestQuery(query, "2,5,4,3,1,0", 6, aggResultStr);
}


TEST_F(MatchDocSearcherTest, testRankTraceInfo) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    string query = "config=cluster:cluster1,rank_trace:ALL,rerank_hint:true&&query=phrase:with";
    ResultPtr result = innerTestQuery(query, "3,0,1,2,4,5", 6);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<Tracer> *tracerRefer = allocatorPtr->findReference<Tracer>(RANK_TRACER_NAME);
    ASSERT_TRUE(tracerRefer);
    HA3_NS(common)::Tracer *tracer = tracerRefer->getPointer(matchDocs->getMatchDoc(0));
    ASSERT_TRUE(tracer);
    ASSERT_TRUE(tracer->getTraceInfo().find("tf=2") != string::npos);
    ASSERT_TRUE(tracer->getTraceInfo().find("[TRACE1]") == 0);
}

//TODO:
TEST_F(MatchDocSearcherTest, testSearchPrimaryKey) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.indexes["pk"] = "a0:0,a1:1,url:2,a3:3,a4:4,a5:5,a6:6,a7:7,a8:8,a9:9,a10:10";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;";

    string query = "config=cluster:cluster1,fetch_summary_type:pk&&query=phrase:with&&pkfilter=url&&sort=price";

    ResultPtr result = innerTestQuery(query, "2", 1);
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs->hasPrimaryKey());
    matchdoc::Reference<uint64_t> *ref = matchDocs->getPrimaryKey64Ref();
    ASSERT_TRUE(ref);
    ASSERT_EQ((uint64_t)2, ref->getReference(matchDocs->getMatchDoc(0)));
}

TEST_F(MatchDocSearcherTest, testSearchWithNotExtraDocIdentifier) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.indexes["pk"] = "a0:0,a1:1,url:2,a3:3,a4:4,a5:5,a6:6,a7:7,a8:8,a9:9,a10:10";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "id : int32_t : 1, 2, 3, 4, 5, 6";

    string query = "config=cluster:cluster1,fetch_summary_type:rawpk&&query=phrase:with&&sort=price";

    ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs->hasPrimaryKey());
}

TEST_F(MatchDocSearcherTest, testSearchPrimaryKeyFail) {
    HA3_LOG(DEBUG, "Begin Test");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.indexes["pk"] = "a0:0,a1:1,url:2,a3:3,a4:4,a5:5,a6:6,a7:7,a8:8,a9:9,a10:10";

    string query = "config=cluster:cluster1&&query=phrase:with&&pkfilter=not_exist";

    innerTestQuery(query, "", 0);
}

TEST_F(MatchDocSearcherTest, testSearchPrimaryKeyFailWithoutIndex) {
    HA3_LOG(DEBUG, "Begin Test");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.indexes["pk"] = "a0:0,a1:1,url:2,a3:3,a4:4,a5:5,a6:6,a7:7,a8:8,a9:9,a10:10";
    string query = "config=cluster:cluster1&&query=phrase:not_exist&&pkfilter=url";
    innerTestQuery(query, "", 0);
}

TEST_F(MatchDocSearcherTest, testSearchBeyondLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2;";
    string query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:with&&aggregate=group_key:type,agg_fun:count()";
    string aggResultStr = "1:2941;"
                          "2:2941;";
    innerTestQuery(query, "3,10,13,0,1,2,4,5,7,16", 5882, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchBeyondLimitWithMultiSegment)
{
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 3);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 2000[2, 5]; 2003[2, 5]; 2010[2, 5]; 2012[2, 5]; 5016[1]; 5019[1]; 5022[1]; 5025[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    _fakeIndex.segmentDocCounts = "2000,3000,5000";
    string query = "config=cluster:cluster1&&query=phrase:with&&aggregate=group_key:type,agg_fun:count()";
    string aggResultStr = "1:5;"
                          "0:13;";
    innerTestQuery(query, "2000,2003,2010,0,1,2,5016,5019,5022,5025", 19, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchEqualLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 14);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    string query = "config=cluster:cluster1&&query=phrase:with&&aggregate=group_key:type,agg_fun:count()";
    string aggResultStr = "1:7;"
                          "2:6";
    innerTestQuery(query, "3,10,13,0,1,2,4,5,7,16", 13, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchNotSeekAllWithBatchAgg)
{
    HA3_LOG(DEBUG, "Begin Test!");
    AggSamplerConfigInfo info = *_aggSamplerConfigInfo;
    _aggSamplerConfigInfo->setAggBatchMode(true);
    _aggSamplerConfigInfo->setAggThreshold(1000);
    initRankProfileManager(_rankProfileMgrPtr, 999);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    string query = "config=cluster:cluster1,rank_size:10,rerank_hint:true&&"
                   "query=phrase:with&&aggregate=group_key:type,agg_fun:count();group_key:type,agg_fun:max(type)";
    string aggResultStr = "1:2941;2:2941\n"
                          "1:1;2:2";

    innerTestQuery(query, "3,10,13,0,1,2,4,5,7,16", 5882, aggResultStr);
    *_aggSamplerConfigInfo = info;
}

TEST_F(MatchDocSearcherTest, testSearchSeekAllWithBatchAgg)
{
    HA3_LOG(DEBUG, "Begin Test!");
    AggSamplerConfigInfo info = *_aggSamplerConfigInfo;
    _aggSamplerConfigInfo->setAggBatchMode(true);
    _aggSamplerConfigInfo->setAggThreshold(1000);
    initRankProfileManager(_rankProfileMgrPtr, 999);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    string query = "config=cluster:cluster1,rank_size:10000&&query=phrase:with&&aggregate=group_key:type,agg_fun:count();group_key:type,agg_fun:max(type)";
    string aggResultStr = "1:7;2:6\n"
                          "1:1;2:2";

    innerTestQuery(query, "3,10,13,0,1,2,4,5,7,16", 13, aggResultStr);
    *_aggSamplerConfigInfo = info;
}

TEST_F(MatchDocSearcherTest, testSearchInLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 999);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    string query = "config=cluster:cluster1&&query=phrase:with&&aggregate=group_key:type,agg_fun:count()";
    string aggResultStr = "1:7;"
                          "2:6";
    innerTestQuery(query, "3,10,13,0,1,2,4,5,7,16", 13, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testSearchZeroLimit)
{
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 0);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];7[1]; 10[2, 5]; 13[2, 5]; 16[1]; 19[1]; 22[1]; 25[1];\n";
    _fakeIndex.attributes = "type : int32_t : 1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2,1, 1, 1, 2, 2, 2";
    string query = "config=cluster:cluster1&&query=phrase:with&&aggregate=group_key:type,agg_fun:count()";
    innerTestQuery(query, "", 0, "");
}

TEST_F(MatchDocSearcherTest, testSearchForLookupTimeOut) {
    FakeTimeoutTerminator timeoutTerminator(1);
    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(_request.get(),
            _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    searcher->_resource.timeoutTerminator = &timeoutTerminator;
    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));
    ResultPtr result = searcher->search(_request.get());
    ASSERT_TRUE(result);
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());

    CHECK_ERROR(ERROR_LOOKUP_TIMEOUT);
}

TEST_F(MatchDocSearcherTest, testSearchForSeekDocTimeOut) {
    FakeTimeoutTerminator timeoutTerminator(2);
    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(_request.get(),
            _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    searcher->_resource.timeoutTerminator = &timeoutTerminator;
    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));
    _request->setSortClause(NULL);
    ResultPtr result = searcher->search(_request.get());
    ASSERT_TRUE(result);
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());

    CHECK_ERROR(ERROR_SEEKDOC_TIMEOUT);
}

TEST_F(MatchDocSearcherTest, testSearchForFileIOException) {
    FakeIndex fakeIndex;
    fakeIndex.versionId = 1;
    fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
    fakeIndex.attributes = "int8 : int8_t : 1, 1, 1, 1, 1, 2;"
                           "type : int32_t : 1, 1, 1, 2, 2, 2;"
                           "price : int32_t : 1, 2, 6, 3, 4, 5;";
    _partitionReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex, IE_NAMESPACE(common)::ErrorCode::FileIO);


    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(_request.get(),
            _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());
    QueryClause *queryClause = _request->getQueryClause();
    queryClause->setRootQuery(PARSE_QUERY("phrase:with"));
    _request->setSortClause(NULL);
    ResultPtr result = searcher->search(_request.get());
    ASSERT_TRUE(result);
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);
    ASSERT_EQ((uint32_t)0, matchDocs->size());

    CHECK_ERROR(ERROR_INDEXLIB_IO);
}

TEST_F(MatchDocSearcherTest, testFillAttributePhaseOne) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "int8: int8_t :   1, 1, 1, 1, 1, 2;"
                            "multi_int32 : multi_int32_t: 1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5;"
                            "attr_string : string : string_0,string_1,string_2,string_3,string_4,string_5;"
                            "multi_string: multi_string : string0_0,string0_1,string0_2#string1_0,string1_1,"
                            "string1_2#string2_0,string2_1#string3_0,string3_1#string4_0#string5_0;";
    string query = "config=cluster:cluster1&&query=phrase:with&&sort=price&&attribute=int8,multi_int32,attr_string,multi_string";
    ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
    checkFillAttributePhaseOneResult(result);
}

TEST_F(MatchDocSearcherTest, testFillAttributePhaseOneWithVirtualAttribute) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "int8: int8_t :   1, 1, 1, 1, 1, 2;"
                            "multi_int32 : multi_int32_t: 1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5;"
                            "attr_string : string : string_0,string_1,string_2,string_3,string_4,string_5;"
                            "multi_string: multi_string : string0_0,string0_1,string0_2#string1_0,string1_1,"
                            "string1_2#string2_0,string2_1#string3_0,string3_1#string4_0#string5_0;";
    string query = "config=cluster:cluster1&&query=phrase:with"
                   "&&virtual_attribute=ma1_v:multi_int32;a2_v:attr_string"
                   "&&attribute=int8,ma1_v,a2_v,multi_string"
                   "&&sort=price&&distinct=dist_key:type,dist_count:1,dist_times:1";
    ResultPtr result = innerTestQuery(query, "2,5,4,3,1,0", 6);
    checkFillAttributePhaseOneResult(result);
}

bool MatchDocSearcherTest::compareVariableReference(matchdoc::ReferenceBase* left,
        matchdoc::ReferenceBase* right)
{
    return (left->getName() < right->getName());
}

void MatchDocSearcherTest::checkFillAttributePhaseOneResult(const ResultPtr& result) {
    MatchDocs * matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs);

    uint32_t size = matchDocs->size();
    ASSERT_EQ((uint32_t)6, size);

    ASSERT_EQ((docid_t)2, matchDocs->getMatchDoc(0).getDocId());
    ASSERT_EQ((docid_t)5, matchDocs->getMatchDoc(1).getDocId());
    ASSERT_EQ((docid_t)4, matchDocs->getMatchDoc(2).getDocId());
    ASSERT_EQ((docid_t)3, matchDocs->getMatchDoc(3).getDocId());
    ASSERT_EQ((docid_t)1, matchDocs->getMatchDoc(4).getDocId());
    ASSERT_EQ((docid_t)0, matchDocs->getMatchDoc(5).getDocId());

    const auto &vsa = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(NULL != vsa);

    auto rv = vsa->getRefBySerializeLevel(SL_ATTRIBUTE);
    sort(rv.begin(), rv.end(), MatchDocSearcherTest::compareVariableReference);
    ASSERT_EQ(size_t(4), rv.size());

    ASSERT_EQ(vt_string, rv[0]->getValueType().getBuiltinType());
    ASSERT_EQ(vt_int8, rv[1]->getValueType().getBuiltinType());
    ASSERT_EQ(vt_int32, rv[2]->getValueType().getBuiltinType());
    ASSERT_EQ(vt_string, rv[3]->getValueType().getBuiltinType());

    ASSERT_TRUE(!rv[0]->getValueType().isMultiValue());
    ASSERT_TRUE(!rv[1]->getValueType().isMultiValue());
    ASSERT_TRUE(rv[2]->getValueType().isMultiValue());
    ASSERT_TRUE(rv[3]->getValueType().isMultiValue());

#define DYNAMIC_CAST(type, value) dynamic_cast< type >(value)

    matchdoc::Reference<MultiValueType<char> > *strRefer = DYNAMIC_CAST(matchdoc::Reference<MultiValueType<char> > *, (rv[0]));
    matchdoc::Reference<int8_t> *int8Refer = DYNAMIC_CAST(matchdoc::Reference<int8_t> *, rv[1]);
    matchdoc::Reference<MultiValueType<int32_t> > *mInt32Refer = DYNAMIC_CAST(matchdoc::Reference<MultiValueType<int32_t> > *, rv[2]);
    matchdoc::Reference<MultiValueType<MultiValueType<char> > > *mStringRefer = DYNAMIC_CAST(matchdoc::Reference<MultiValueType<MultiValueType<char> > > *, rv[3]);

#undef DYNAMIC_CAST

    ASSERT_TRUE(strRefer);
    ASSERT_TRUE(int8Refer);
    ASSERT_TRUE(mInt32Refer);
    ASSERT_TRUE(mStringRefer);
    matchdoc::MatchDoc md0 = matchDocs->getMatchDoc(0);

    ASSERT_EQ(string("string_2"), (VariableTypeTraits<vt_string, false>::convert(strRefer->getReference(md0))));
    ASSERT_EQ(int8_t(1), int8Refer->getReference(md0));
    ASSERT_EQ(uint32_t(3), mInt32Refer->getReference(md0).size());
    ASSERT_EQ(2, (VariableTypeTraits<vt_int32, false>::convert(mInt32Refer->getReference(md0)[0])));
    ASSERT_EQ(4, (VariableTypeTraits<vt_int32, false>::convert(mInt32Refer->getReference(md0)[1])));
    ASSERT_EQ(1, (VariableTypeTraits<vt_int32, false>::convert(mInt32Refer->getReference(md0)[2])));
    ASSERT_EQ(uint32_t(2), mStringRefer->getReference(md0).size());
    ASSERT_EQ(string("string2_0"), (VariableTypeTraits<vt_string, false>::convert(mStringRefer->getReference(md0)[0])));
    ASSERT_EQ(string("string2_1"), (VariableTypeTraits<vt_string, false>::convert(mStringRefer->getReference(md0)[1])));

    MatchDocs2Hits m2h;
    vector<SortExprMeta> sortExprMetaVec;
    ErrorResult errResult;
    vector<string> clusterList;
    unique_ptr<Hits> hits(m2h.convert(matchDocs, _request, sortExprMetaVec,
                    errResult, clusterList));

    ASSERT_EQ(size_t(4), hits->getHit(0)->getAttributeMap().size());
}

TEST_F(MatchDocSearcherTest, testSearchWithCreateHitCollectorsFail) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;";
    string query = "config=cluster:cluster1&&query=phrase:with&&distinct=none_dist;dist_key:price,dist_count:2,dist_times:1";
    ResultPtr result = innerTestQuery(query, "", 0, "", false);

    CHECK_ERROR(ERROR_SORTER_BEGIN_REQUEST);
}

TEST_F(MatchDocSearcherTest, testSearchWithRankSortExtraDist) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:5&&query=phrase:with&&distinct=none_dist;dist_key:type,dist_count:2,dist_times:1,update_total_hit:true";
    innerTestQuery(query, "0,1,2,3,4", 10000);
}

TEST_F(MatchDocSearcherTest, testSearchWithRankSortExtraDistAndHitNumLessThanRerankSize) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:3&&query=phrase:with&&distinct=none_dist;dist_key:type,dist_count:2,dist_times:1,update_total_hit:true";
    innerTestQuery(query, "0,1,2", 10000);
}

TEST_F(MatchDocSearcherTest, testSearchWithRankSortExtraDistAndHitNumMoreThanRerankSize) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:7&&query=phrase:with&&distinct=none_dist;dist_key:type,dist_count:3,dist_times:1,update_total_hit:true";
    innerTestQuery(query, "0,1,2,5,3,4,6", 10000);
}

TEST_F(MatchDocSearcherTest, testSearchWithRankDistExtraNone) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:5&&query=phrase:with&&distinct=dist_key:type,dist_count:2,dist_times:1;none_dist";
    innerTestQuery(query, "0,1,2,5,7", 10000);
}

TEST_F(MatchDocSearcherTest, testSearchWithRankDistExtraDist) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:5&&query=phrase:with"
                   "&&distinct=dist_key:type,dist_count:2,dist_times:1,update_total_hit:true;"
                   "dist_key:type,dist_count:1,dist_times:1";
    innerTestQuery(query, "0,5,1,2,7", 10000);
}

TEST_F(MatchDocSearcherTest, testDoCreateHitCollectorsFail) {
    // HA3_LOG(DEBUG, "Begin Test!");

    // prepareForCreateHitCollector();
    // SyntaxExpr *expr = new FuncSyntaxExpr("not_exist_func");
    // DistinctDescription *distDesc = new DistinctDescription(
    //         string("dist_desc_name"),
    //         string("originalString"), 1, 1, false,
    //         expr, NULL);
    // DistinctClause *distClause = new DistinctClause;
    // distClause->addDistinctDescription(NULL);
    // distClause->addDistinctDescription(distDesc);
    // _request->setDistinctClause(distClause);

    // vector<HitCollectorPtr> hitCollectors;
    // bool ret = _searcher->doCreateHitCollectors(_request, 3, hitCollectors);
    // ASSERT_TRUE(!ret);
    // ASSERT_EQ((size_t)0, hitCollectors.size());
}

TEST_F(MatchDocSearcherTest, testSearchWithExtraHitCollector1) {
    HA3_LOG(DEBUG, "Begin Test!");

    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    _tableInfoPtr->setPrimaryKeyAttributeFlag(false);
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);

    string query = "config=cluster:cluster1,hit:6&&query=phrase:with&&sort=+price"
                   "&&distinct=none_dist;dist_key:type,dist_count:2,dist_times:1";

    innerTestQuery(query, "0,1,5,2,3,4", 10);
}

TEST_F(MatchDocSearcherTest, testSearchWithExtraHitCollector2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    _tableInfoPtr->setPrimaryKeyAttributeFlag(false);
    initRankProfileManager(_rankProfileMgrPtr, 10, 5);

    string query = "config=cluster:cluster1,hit:6&&query=phrase:with&&sort=int32;price"
                   "&&distinct=none_dist;dist_key:type,dist_count:2,dist_times:1";
    innerTestQuery(query, "9,8,6,4,7,5", 10);
}

TEST_F(MatchDocSearcherTest, testMultiQuerySearch) {
    _fakeIndex.indexes["phrase"] = "a:0[1];3[1];7[1];10[1];11[1];17[1];\n"
                                   "b:0[1];1[1];4[1];10[1];13[1];17[1];\n";
    string query = "config=cluster:cluster1,rerank_hint:true&&layer=quota:4;quota:3"
                   "&&query=a OR b;a AND b";
    innerTestQuery(query, "0,10,17,1,3,4", 8000);
}

TEST_F(MatchDocSearcherTest, testAuxOptimizer) {
    _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2];3[1, 2];4[1];5[5];";
    _fakeIndex.indexes["weight"] = "abc:0[1];1[1];3[2];4[5];\n";
    string query = "config=cluster:cluster1,rerank_hint:true,rank_size:3&&layer=quota:2;quota:1"
                   "&&query=phrase:abc&&optimizer=name:AuxChain,option:select#ALL|pos#AFTER|aux_name#weight";
    innerTestQuery(query, "0,1,3", 10000);
}

TEST_F(MatchDocSearcherTest, testRerankSortOrder) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9\n";
    _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;"
                            "score1: double : 55,44,44,33,22,33,22,11,22,66;"
                            "score2: double : 100,144,144,133,122,133,122,111,122,111;";
    string query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:with"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:score2";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 4);
    innerTestQuery(query, "1,2,9,0,3,5,4,6,8,7", 10);
}

TEST_F(MatchDocSearcherTest, testRankScoreAllEqual) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9\n";
    _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;"
                            "score1: double : 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1;"
                            "score2: double : 0,44,44,33,22,33,22,11,22,11;";
    string query = "config=cluster:cluster1,rerank_hint:true,rerank_size:3,start:2,hit:2&&query=phrase:with"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:score2";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 4);
    innerTestQuery(query, "1,2,3,0", 10);
}

TEST_F(MatchDocSearcherTest, testRankResultDistnct) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9\n";
    _fakeIndex.attributes = "type: int32_t : 1,1,1,1,2,3,3,3,5,6;"
                            "score1: double : 55,44,44,33,22,33,22,11,22,11;"
                            "score2: double : 0,44,44,33,22,33,22,11,22,11;";
    string query = "config=cluster:cluster1,hit:5,rerank_hint:true&&query=phrase:with"
                   "&&kvpairs=score_first:score1,score_second:score2"
                   "&&distinct=dist_key:type,dist_count:1,dist_times:1;none_dist";

    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 2);
    innerTestQuery(query, "5,4,8,9,0", 10);
}

TEST_F(MatchDocSearcherTest, testForceEvaluatebyRerankSize) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3\n";
    _fakeIndex.attributes = "score1: double : 44,44,55,33;"
                            "score2: double : 0,44,33,33;";
    string query = "config=cluster:cluster1,hit:5,rerank_hint:true&&query=phrase:with"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:score2";

    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 2);
    innerTestQuery(query, "1,2,3,0", 4);
}

TEST_F(MatchDocSearcherTest, testSingleSortForceEvaluate) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3\n";
    _fakeIndex.attributes = "price : int32_t: 2,3,1,4";
    string query = "config=cluster:cluster1&&query=phrase:with&&sort=-price";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 100);
    innerTestQuery(query, "3,1,0,2", 4);
}

TEST_F(MatchDocSearcherTest, testOptimizeHitCollector) {
    {
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1&&query=phrase:with"
                       "&&kvpairs=no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second", "first");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1&&query=phrase:with"
                       "&&kvpairs=match_data_first:true,no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second", "first");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1&&query=phrase:with"
                       "&&kvpairs=match_data_second:true,no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second", "first");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1&&query=phrase:with"
                       "&&kvpairs=declare_variable_first:true,no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second;first", "");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1&&query=phrase:with"
                       "&&kvpairs=declare_variable_second:true,no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second", "first");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1,optimize_rank:false&&query=phrase:with"
                       "&&kvpairs=no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second;first", "");
        tearDown();
    }
    {
        setUp();
        _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;\n";
        _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;";
        string query = "config=cluster:cluster1,rerank_hint:true,rank_trace:TRACE1,optimize_rank:true&&query=phrase:with"
                       "&&kvpairs=no_declare_string_first:yes,no_declare_string_second:yes";
        initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
        ResultPtr result = innerTestQuery(query, "0,1,2,3,4", 5);
        checkTrace(result, 0, "second", "first");
        tearDown();
    }

}

TEST_F(MatchDocSearcherTest, testDistinctReserveFalseDoNotOptimizeHitCollect) {
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10, 10);
    _fakeIndex.indexes["phrase"] = "abc:0[1];1[1];2[2,3];3[1,3];4[1];5[5];";
    _fakeIndex.attributes = "type: int32_t : 1,2,1,2,1,2;";
    string query = "config=cluster:cluster1,rerank_size:10&&query=phrase:abc"
                   "&&distinct=dist_key:type,dist_count:1,dist_times:1,reserved:false;none_dist";
    innerTestQuery(query, "1,0", 6);
}

TEST_F(MatchDocSearcherTest, testSearchInvalidVirtualAttribute) {
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    {
        string query = "config=cluster:cluster1&&query=phrase:with&&virtual_attribute=a:price&&filter=a>10";
        ResultPtr result = innerTestQuery(query, "", 0, "", false);
        CHECK_ERROR(ERROR_SEARCH_SETUP_FILTER);
    }
    {
        string query = "config=cluster:cluster1&&query=phrase:with&&virtual_attribute=a:price&&aggregate=group_key:a,agg_fun:count()";
        ResultPtr result = innerTestQuery(query, "", 0, "", false);
        CHECK_ERROR(ERROR_SEARCH_SETUP_AGGREGATOR);
    }
    {
        string query = "config=cluster:cluster1&&query=phrase:with&&virtual_attribute=a:price&&distinct=dist_key:a,dist_count:1,dist_times:1";
        ResultPtr result = innerTestQuery(query, "", 0, "", false);
        CHECK_ERROR(ERROR_SEARCH_SETUP_DISTINCT);
    }
}

TEST_F(MatchDocSearcherTest, testBatchScore) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9\n";
    _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;"
                            "score1: double : 55,44,44,33,22,33,22,11,22,11;"
                            "score2: double : 0,44,44,33,22,33,22,11,22,11;";
    string query = "config=cluster:cluster1,rerank_hint:true,batch_score:false&&query=phrase:with"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:score2";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 4);
    innerTestQuery(query, "1,2,3,5,4,6,8,7,9,0", 10);
}

TEST_F(MatchDocSearcherTest, testLazyEvaluateAttrExpr) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9\n";
    _fakeIndex.attributes = "type: int32_t : 55,44,44,33,22,33,22,11,22,11;"
                            "score1: double : 1,1,1,1,1,1,1,1,1,1;"
                            "score2: double : 1,1,1,1,1,1,1,1,1,1;"
                            "price : double : 8,9,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,rerank_hint:true,optimize_comparator:false&&query=phrase:with"
                   "&&sort=score1;score2;RANK&&kvpairs=no_declare_string_first:true,score_first:price,score_second:price";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 10);
    innerTestQuery(query, "1,0,2,3,4,5,6,7,8,9", 10);
}

TEST_F(MatchDocSearcherTest, testLargeDocSearch) {
    const int DOC_COUNT = 10000;
    int outDoc[] = {10,99,555,2345,4532,5452,8763,8888,9109,9999};
    int requiredDoc = sizeof(outDoc) / sizeof(outDoc[0]);
    set<int> outDocSet;
    outDocSet.insert(outDoc, outDoc+requiredDoc);
    ostringstream oss;
    oss<<"with:";
    for(int i = 0; i < DOC_COUNT; i++){
        if(outDocSet.find(i) !=  outDocSet.end()) {
            oss <<i<<"[1,2,3]"<<";";
        }else{
            oss <<i<<"["<<i%10<<"]"<<";";
        }
    }
    _fakeIndex.indexes["phrase"] = oss.str();
    string query = "config=cluster:cluster1,batch_score:false,rank_size:100000&&query=phrase:with"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:score2";
    innerTestQuery(query, "10,99,555,2345,4532,5452,8763,8888,9109,9999", 10000);
}


TEST_F(MatchDocSearcherTest, testBatchAggAndRerank) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2\n";
    _fakeIndex.attributes = "double: double : 55,44,66;"
                            "score1: double : 55,44,66;";
    string query = "config=cluster:cluster1,hit:10,batch_score:false&&query=phrase:with"
                   "&&aggregate=group_key:double,agg_fun:count()"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:double";
    _aggSamplerConfigInfo->setAggBatchMode(true);
    _aggSamplerConfigInfo->setAggThreshold(2);
    _aggSamplerConfigInfo->setBatchSampleMaxCount(2);
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 1);
    string aggResultStr = "55:2;66:2";
    innerTestQuery(query, "2,0,1", 3, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testBatchAggAndRerank2) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2\n";
    _fakeIndex.attributes = "double: double : 55,44,66;"
                            "score1: double : 55,44,66;";
    string query = "config=cluster:cluster1,hit:10,batch_score:false&&query=phrase:with"
                   "&&aggregate=group_key:double,agg_fun:count()"
                   "&&kvpairs=match_data_first:true,score_first:score1,score_second:double";
    _aggSamplerConfigInfo->setAggBatchMode(true);
    _aggSamplerConfigInfo->setAggThreshold(0);
    _aggSamplerConfigInfo->setBatchSampleMaxCount(0);
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 1);
    string aggResultStr = "55:1;44:1;66:1";
    innerTestQuery(query, "2,0,1", 3, aggResultStr);
}

TEST_F(MatchDocSearcherTest, testRankSort) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,hit:5,rerank_size:5&&query=phrase:with"
                   "&&rank=rank_profile:DefaultProfile"
                   "&&rank_sort=sort:RANK,percent:40;sort:score2,percent:60"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "9,8,2,1,0", 10);
}

TEST_F(MatchDocSearcherTest, testRankSortTopK0) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,hit:0,rerank_size:5&&query=phrase:with"
                   "&&rank=rank_profile:DefaultProfile"
                   "&&rank_sort=sort:RANK,percent:10;sort:score2,percent:60"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "", 10);
}

TEST_F(MatchDocSearcherTest, testRankSortFailed) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,hit:5,rerank_size:5&&query=phrase:with"
                   "&&rank_sort=sort:RANK,percent:10;sort:score2,percent:60"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "4,3,2,1,0", 10);
}

TEST_F(MatchDocSearcherTest, testRankSortWithRANK) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,hit:5,rerank_size:5&&query=phrase:with"
                   "&&rank_sort=sort:RANK,percent:40;sort:score2,percent:60"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "9,8,2,1,0", 10);
}

TEST_F(MatchDocSearcherTest, testRankSortNoRankInRankSort) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,hit:5,rerank_size:5,rerank_hint:true&&query=phrase:with"
                   "&&rank=rank_profile:DefaultProfile"
                   "&&rank_sort=sort:score1,percent:40;sort:score2,percent:60"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "9,8,2,1,0", 10);
}

TEST_F(MatchDocSearcherTest, testRankSortStartHitGreaterThanRerankSize) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,2,3,4,5,6,7,8,9;"
                            "score2: double : 9,8,7,6,5,4,3,2,1,0;";
    string query = "config=cluster:cluster1,start:4,hit:4,rerank_size:4,rerank_hint:true&&query=phrase:with"
                   "&&rank=rank_profile:DefaultProfile"
                   "&&rank_sort=sort:score1,percent:50;sort:score2,percent:50"
                   "&&kvpairs=score_first:type,score_second:type"
                   "&&final_sort=sort_name:DEFAULT;sort_param:sort_key#RANK";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "9,8,7,6,0,1,2,3", 10);
}

TEST_F(MatchDocSearcherTest, testSearchWithDistinctUpdateTotalHit) {
    HA3_LOG(DEBUG, "Begin Test!");
    initRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    _fakeIndex.attributes = "type : int32_t : 2,2,2,2,2,1,2,1,1,1;"
                            "int32: int32_t : 0,1,2,3,4,5,4,7,8,8;"
                            "price: int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,hit:10&&query=phrase:with&&distinct=none_dist;"
                   "dist_key:type,dist_count:1,dist_times:1,reserved:false,update_total_hit:true";
    innerTestQuery(query, "0,5", 2);
}

TEST_F(MatchDocSearcherTest, testRankNotInFirstDimen) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,0,3,5,4,6,7,8,9;"
                            "score2: double : 1,1,1,1,1,2,2,2,2,2;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,start:0,rerank_hint:true,hit:1000,rerank_size:500&&query=phrase:with"
                   "&&sort=score1;RANK&&kvpairs=no_declare_string_first:true";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "9,8,7,6,4,5,3,1,2,0", 10);
}


TEST_F(MatchDocSearcherTest, testRankNotInFirstDimen3) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "score1: double : 0,1,0,3,5,4,6,7,8,9;"
                            "score2: double : 1,1,1,1,1,2,2,2,2,2;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,start:0,hit:1000,rerank_size:1&&query=phrase:with"
                   "&&sort=score2;price;RANK&&kvpairs=no_declare_string_first:true";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    innerTestQuery(query, "8,7,6,5,9,2,4,1,3,0", 10);
}

TEST_F(MatchDocSearcherTest, testSetRankSizeWithoutRANKExpression) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,start:0,hit:10,rank_size:5&&query=phrase:with"
                   "&&sort=price";
    innerTestQuery(query, "2,4,1,3,0", 10000);
}

TEST_F(MatchDocSearcherTest, testDefaultFinalSort) {
    initRankProfileManager(_rankProfileMgrPtr, 5, 5);
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,start:0,hit:10,rank_size:5&&query=phrase:with"
                   "&&sort=price&&rank_sort=sort:type,percent:100;";
    innerTestQuery(query, "2,4,1,3,0", 10000);
}

TEST_F(MatchDocSearcherTest, testDefaultFinalSort2) {
    initRankProfileManager(_rankProfileMgrPtr, 5, 5);
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,rerank_hint:true,start:0,hit:6,rank_size:10&&query=phrase:with"
                   "&&sort=+RANK&&rank_sort=sort:type,percent:50;sort:price,percent:50;";
    // for now, 2,5,7,8,9 will process in rerank, so score of doc_6 is 0, the others are 0.1......
    innerTestQuery(query, "6,2,5,7,8,9", 10000);
}

TEST_F(MatchDocSearcherTest, testScorerTerminateSeek) {
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price : int32_t: 1,3,5,2,4,6,7,8,9,1";
    string query = "config=cluster:cluster1,batch_score:false&&query=phrase:with"
                   "&&kvpairs=terminate_seek_first:2";
    innerTestQuery(query, "1,0", 10000);
}

TEST_F(MatchDocSearcherTest, testScorerRequestTracer) {
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,trace:TRACE3,rerank_hint:true&&query=phrase:with";
    innerTestQuery(query, "9,8,7,6,5,4,3,2,1,0", 10);

    ASSERT_TRUE(_requestTracer);
    string traceStr = _requestTracer->getTraceInfo();
    ASSERT_TRUE(traceStr.find("begin request in scorer[first]") != string::npos);
    ASSERT_TRUE(traceStr.find("end request in scorer[first]") != string::npos);
    ASSERT_TRUE(traceStr.find("begin request in scorer[second]") != string::npos);
    ASSERT_TRUE(traceStr.find("end request in scorer[second]") != string::npos);
}

TEST_F(MatchDocSearcherTest, testRequireSimpleMatchData) {
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "type :int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:with&&kvpairs=simple_match_data_first:true";
    innerTestQuery(query, "9,8,7,6,5,4,3,2,1,0", 10);
}

TEST_F(MatchDocSearcherTest, testRequireSimpleMatchDataManyTerm) {
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    string termStr;
    for (size_t i = 0; i < 100; ++i) {
        termStr += autil::StringUtil::toString(i) + ":0;\n";
    }
    _fakeIndex.indexes["phrase"] = termStr;
    _fakeIndex.attributes = "type :int32_t : 0;";
    string queryStr ;
    for (size_t i = 0; i < 100; ++i) {
        queryStr += (i == 0 ? "" :" OR ") + string("phrase:") + autil::StringUtil::toString(i);
    }
    string query = "config=cluster:cluster1,rerank_hint:true&&query=" + queryStr + "&&kvpairs=simple_match_data_first:true";
    innerTestQuery(query, "0", 1);
}

TEST_F(MatchDocSearcherTest, testBinraryExprInFuncExpr) {
    _fakeIndex.indexes["phrase"] = "with:0;1;2;3;4;5;6;7;8;9;\n";
    _fakeIndex.attributes = "price :int32_t : 0,1,2,3,4,5,6,7,8,9;";
    string query = "config=cluster:cluster1&&query=phrase:with&&filter=in(price+1, \"4|5|8\")&&sort=+price";
    innerTestQuery(query, "3,4,7", 3);
}

TEST_F(MatchDocSearcherTest, testSortWithString) {
    string resultString1;
    string resultString2;
    {
        // search result 1
        _fakeIndex.indexes["phrase"] = "with:0;1;2;\n";
        _fakeIndex.attributes = "attr_string :string: cab,a,e;";
        string query = "config=cluster:cluster1&&query=phrase:with&&sort=+attr_string";
        ResultPtr result1 = innerTestQuery(query, "1,0,2", 3);
        result1->serializeToString(resultString1, _pool);
        tearDown();
    }
    {
        // search result 2
        setUp();
        _fakeIndex.indexes["phrase"] = "with:3;4;5;\n";
        _fakeIndex.attributes = "attr_string :string: xx,xx,xx,y,z,accccc;";
        string query = "config=cluster:cluster1&&query=phrase:with&&sort=+attr_string";
        ResultPtr result2 = innerTestQuery(query, "5,3,4", 3);
        result2->serializeToString(resultString2, _pool);
    }

    //merge search result 1 and search result 2
    autil::mem_pool::Pool pool(1024);
    vector<common::ResultPtr> results;
    ResultPtr result1(new common::Result);
    ResultPtr result2(new common::Result);
    result1->deserializeFromString(resultString1, &pool);
    result2->deserializeFromString(resultString2, &pool);
    results.push_back(result1);
    results.push_back(result2);
    ResultPtr mergedResultPtr(new common::Result);
    SorterWrapper *wrapper = new SorterWrapper(new sorter::DefaultSorter());
    Merger::mergeResult(mergedResultPtr, results, _request.get(), wrapper, sorter::SL_PROXY);
    const MatchDocs *mergedMatchDocs = mergedResultPtr->getMatchDocs();
    string expcetdDocids = "1,5,0,2,3,4";
    vector<docid_t> expcetdDocidList =
        SearcherTestHelper::covertToResultDocIds(expcetdDocids);
    ASSERT_TRUE(mergedMatchDocs);
    ASSERT_EQ((uint32_t)expcetdDocidList.size(),
                         mergedMatchDocs->totalMatchDocs());
    ASSERT_EQ((uint32_t)expcetdDocidList.size(),
                         mergedMatchDocs->actualMatchDocs());
    for (size_t i = 0; i < expcetdDocidList.size(); ++i) {
        ASSERT_EQ(expcetdDocidList[i],
                             mergedMatchDocs->getMatchDoc(i).getDocId());
    }
    delete wrapper;
}

TEST_F(MatchDocSearcherTest, testStringForSortAndRankSortAndDistinct) {
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes =
        "price: int32_t : 1, 2, 6, 3, 4, 5;"
        "type:  int32_t : 1, 1, 1, 2, 2, 2;"
        "int8: int8_t :   1, 1, 1, 1, 1, 2;"
        "multi_int32 : multi_int32_t: 1, 2, 3#1, 1, 3#2, 4, 1#1, 5, 3#1, 2, 3#3, 4, 5;"
        "attr_string : string : string_1,string_3,string_5,string_2,string_4,string_6;"
        "multi_string: multi_string : string0_0,string0_1,string0_2#string1_0,string1_1,"
        "string1_2#string2_0,string2_1#string3_0,string3_1#string4_0#string5_0;";

    // sort
    string query = "config=cluster:cluster1&&query=phrase:with&&sort=-attr_string";
    innerTestQuery(query, "5,2,4,1,3,0", 6);

    // rank_sort
    query = "config=cluster:cluster1&&query=phrase:with&&rank_sort=sort:-attr_string,percent:100";
    innerTestQuery(query, "5,2,4,1,3,0", 6);

    // final_sort
    query = "config=cluster:cluster1&&query=phrase:with&&final_sort=sort_name:DEFAULT;sort_param:sort_key#-attr_string";
    innerTestQuery(query, "5,2,4,1,3,0", 6);

    query = "config=cluster:cluster1&&query=phrase:with&&final_sort=sort_name:DEFAULT;sort_param:sort_key#-multi_string";
    innerTestQuery(query, "", 0, "", false);

    // distinct
    _fakeIndex.attributes =
        "price: int32_t : 0, 0, 2, 2, 1, 1;"
        "type:  int32_t : 0, 0, 1, 1, 2, 2;"
        "int8: int8_t :   1, 1, 1, 1, 1, 2;"
        "multi_int32 : multi_int32_t: 1, 1, 1#1, 1, 1#2, 2, 2#2, 2, 2#1, 1, 2#1, 1, 2;"
        "attr_string : string : string_1,string_1,string_3,string_3,string_2,string_2;"
        "multi_string: multi_string : string0_0,string0_1,string0_2#string0_0,string0_1,"
        "string0_2#string2_0,string2_1#string2_0,string2_1#string3_0#string3_0;";

    query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:multi_int32,dist_count:1,dist_times:1&&sort=+price";
    innerTestQuery(query, "0,4,2,1,5,3", 6);

    query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:attr_string,dist_count:1,dist_times:1&&sort=+attr_string";
    innerTestQuery(query, "0,4,2,1,5,3", 6);

    query = "config=cluster:cluster1&&query=phrase:with&&distinct=dist_key:multi_string,dist_count:1,dist_times:1&&sort=+type";
    innerTestQuery(query, "0,2,4,1,3,5", 6);
}

TEST_F(MatchDocSearcherTest, testSubDocSimpleProcess) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:group&&query=sub_index_name:term AND index_name:term"
                   "&&sort=price&&filter=sub_price>5";
    innerTestQuery(query, "2", 1);
}

TEST_F(MatchDocSearcherTest, testGetAllSubDocProcess) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:group,get_all_sub_doc:true&&query=sub_index_name:term AND index_name:term"
                   "&&sort=price&&filter=sub_price>5";
    innerTestQuery(query, "2", 1);
}

TEST_F(MatchDocSearcherTest, testSubDocSimpleProcessWithSubExpressionFilter)
{
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:group&&query=sub_index_name:term AND index_name:term"
                   "&&sort=price&&filter=sub_max(sub_price)>1 AND sub_price>1";
    innerTestQuery(query, "2", 1);
}

TEST_F(MatchDocSearcherTest, testGetAllSubDocProcessWithSubExpressionFilter)
{
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:group,get_all_sub_doc:true&&query=sub_index_name:term AND index_name:term"
                   "&&sort=price&&filter=sub_max(sub_price)>1 AND sub_price>1";
    innerTestQuery(query, "2", 1);
}

TEST_F(MatchDocSearcherTest, testSubDocSimpleFunc) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9;"
                            "price: int32_t : 0,1,2;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:group&&query=sub_index_name:term OR index_name:term"
            "&&virtual_attribute=JOIN_VALUE:sub_join(sub_price)&&attribute=JOIN_VALUE&&sort=+price";
    ResultPtr result = innerTestQuery(query, "0,1,2", 3);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<MultiChar> *joinRef = allocatorPtr->findReference<MultiChar>("sub_join(sub_price)");
    MultiChar str;
    str = joinRef->getReference(matchDocs->getMatchDoc(0));
    ASSERT_EQ(string("0 1 2"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(1));
    ASSERT_EQ(string("4"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(2));
    ASSERT_EQ(string("5 6 7 8 9"), string(str.data(), str.size()));
}


TEST_F(MatchDocSearcherTest, testGetAllSubDocProcessWithSubDocExecutor) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "A:0;4;9;;11;12;14;\n"
                                           "B:3;7;11;13;14;\n";
    _fakeIndex.indexes["index_name"] = "A:1;3;\n"
                                       "B:0;3;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10,11,15");
    string query = "config=cluster:cluster1,sub_doc:group,get_all_sub_doc:true&&"
                   "query=(sub_index_name:A OR index_name:A) AND (sub_index_name:B OR index_name:B)"
                   "&&virtual_attribute=JOIN_VALUE:sub_join(sub_price)&&attribute=JOIN_VALUE&&sort=+price&&filter=sub_price>-1";
    ResultPtr result = innerTestQuery(query, "0,1,3,4", 4);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::SubDocAccessor *accessor = allocatorPtr->getSubDocAccessor();
    matchdoc::Reference<MultiChar> *joinRef = allocatorPtr->findReference<MultiChar>("sub_join(sub_price)");
    MultiChar str;
    matchdoc::MatchDoc matchdoc0 = matchDocs->getMatchDoc(0);
    std::vector<matchdoc::MatchDoc> subDocs0;
    accessor->getSubDocs(matchdoc0, subDocs0);
    ASSERT_EQ(false, subDocs0[0].isDeleted());
    ASSERT_EQ(true, subDocs0[1].isDeleted());
    ASSERT_EQ(true, subDocs0[2].isDeleted());
    str = joinRef->getReference(matchdoc0);
    ASSERT_EQ(string("0 1 2"), string(str.data(), str.size()));

    matchdoc::MatchDoc matchdoc1 = matchDocs->getMatchDoc(1);
    std::vector<matchdoc::MatchDoc> subDocs1;
    accessor->getSubDocs(matchdoc1, subDocs1);
    ASSERT_EQ(false, subDocs1[0].isDeleted());
    ASSERT_EQ(true, subDocs1[1].isDeleted());
    str = joinRef->getReference(matchdoc1);
    ASSERT_EQ(string("3 4"), string(str.data(), str.size()));

    matchdoc::MatchDoc matchdoc2 = matchDocs->getMatchDoc(2);
    std::vector<matchdoc::MatchDoc> subDocs2;
    accessor->getSubDocs(matchdoc2, subDocs2);
    ASSERT_EQ(false, subDocs2[0].isDeleted());
    str = joinRef->getReference(matchdoc2);
    ASSERT_EQ(string("10"), string(str.data(), str.size()));

    matchdoc::MatchDoc matchdoc3 = matchDocs->getMatchDoc(3);
    std::vector<matchdoc::MatchDoc> subDocs3;
    accessor->getSubDocs(matchdoc3, subDocs3);
    ASSERT_EQ(false, subDocs3[0].isDeleted());
    ASSERT_EQ(true, subDocs3[1].isDeleted());
    ASSERT_EQ(true, subDocs3[2].isDeleted());
    ASSERT_EQ(false, subDocs3[3].isDeleted());
    str = joinRef->getReference(matchdoc3);
    ASSERT_EQ(string("11 12 13 14"), string(str.data(), str.size()));
}

TEST_F(MatchDocSearcherTest, testSubDocExecutor) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "A:0;4;9;;11;12;14;\n"
                                           "B:3;7;11;13;14;\n";
    _fakeIndex.indexes["index_name"] = "A:1;3;\n"
                                       "B:0;3;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10,11,15");
    string query = "config=cluster:cluster1,sub_doc:group&&"
                   "query=(sub_index_name:A OR index_name:A) AND (sub_index_name:B OR index_name:B)"
                   "&&virtual_attribute=JOIN_VALUE:sub_join(sub_price)&&attribute=JOIN_VALUE&&sort=+price&&filter=sub_price>-1";
    ResultPtr result = innerTestQuery(query, "0,1,3,4", 4);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<MultiChar> *joinRef = allocatorPtr->findReference<MultiChar>("sub_join(sub_price)");
    MultiChar str;
    str = joinRef->getReference(matchDocs->getMatchDoc(0));
    ASSERT_EQ(string("0"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(1));
    ASSERT_EQ(string("3"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(2));
    ASSERT_EQ(string("10"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(3));
    ASSERT_EQ(string("11 14"), string(str.data(), str.size()));
}

TEST_F(MatchDocSearcherTest, testAndNotSubDocExecutor) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "A:0;4;9;;11;12;14;\n"
                                           "B:3;7;11;13;14;\n";
    _fakeIndex.indexes["index_name"] = "A:1;3;\n"
                                       "B:0;3;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14;"
                            "price: int32_t : 0,1,2,3,4;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10,11,15");
    string query = "config=cluster:cluster1,sub_doc:group&&"
                   "query=sub_index_name:A ANDNOT sub_index_name:B"
                   "&&virtual_attribute=JOIN_VALUE:sub_join(sub_price)&&attribute=JOIN_VALUE&&sort=+price&&filter=sub_price>-1";
    ResultPtr result = innerTestQuery(query, "0,1,2,4", 4);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<MultiChar> *joinRef = allocatorPtr->findReference<MultiChar>("sub_join(sub_price)");
    MultiChar str;
    str = joinRef->getReference(matchDocs->getMatchDoc(0));
    ASSERT_EQ(string("0"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(1));
    ASSERT_EQ(string("4"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(2));
    ASSERT_EQ(string("9"), string(str.data(), str.size()));
    str = joinRef->getReference(matchDocs->getMatchDoc(3));
    ASSERT_EQ(string("12"), string(str.data(), str.size()));
}

TEST_F(MatchDocSearcherTest, testSubDocFlatten) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "term:0;1;4;6;7;9;\n";
    _fakeIndex.indexes["index_name"] = "term:0;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 0,10,2,30,4,50,6,70,8,90;"
                            "price: int32_t : 0,1,2;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,10");
    string query = "config=cluster:cluster1,sub_doc:flat&&query=sub_index_name:term OR index_name:term"
                   "&&attribute=sub_price&&sort=+sub_price&&filter=sub_price>-1";
    innerTestQuery(query, "0,0,1,2,2,0,2,2,2", 9);
}

TEST_F(MatchDocSearcherTest, testMultiLayerWithDiffQueryCount) {
    _fakeIndex.indexes["phrase"] = "a:0;2;\n"
                                   "b:0;2;3;\n"
                                   "c:1;2;3;\n";
    _fakeIndex.attributes = "price: int32_t : 0,1,2,3;";
    string query = "config=cluster:cluster1,rerank_hint:true&&query=phrase:a OR phrase:b OR phrase:c;phrase:a&&layer=quota:1;quota:1";
    // 0: 0.8333, 2: 0.5
    innerTestQuery(query, "0,2", 10000);
}

TEST_F(MatchDocSearcherTest, testSubDocFilter) {
    // doc1: iphone white red
    // doc2: iphone black red
    // doc3: iphone black green
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["sub_index_name"] = "white:0;\n"
                                           "red:1;3;\n"
                                           "black:2;4;\n"
                                           "green:5;\n";
    _fakeIndex.indexes["index_name"] = "iphone:0;1;2;\n";
    _fakeIndex.attributes = "sub_price: int32_t : 1,2,3,2,3,4;"
                            "price: int32_t : 1,2,3;";
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "2,4,6");
    string query = "config=cluster:cluster1,sub_doc:group&&"
                   "query=(sub_index_name:iphone OR index_name:iphone) AND (sub_index_name:black OR index_name:black)"
                   "&&virtual_attribute=JOIN_VALUE:sub_join(sub_price)&&attribute=JOIN_VALUE&&sort=+price";
    ResultPtr result = innerTestQuery(query, "1,2", 2);
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<MultiChar> *joinRef = allocatorPtr->findReference<MultiChar>(
            "sub_join(sub_price)");
    ASSERT_EQ(string("3"), string(joinRef->getReference(
                            matchDocs->getMatchDoc(0)).data(), 1));
    ASSERT_EQ(string("3"), string(joinRef->getReference(
                            matchDocs->getMatchDoc(1)).data(), 1));
}

/*TEST_F(MatchDocSearcherTest, testAuxTableReader) {
    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.tablePrefix.push_back("aux_");
    _fakeIndex.indexes["phrase"] = "term:0;1;2;3\n";
    _fakeIndex.indexes["aux_pk"] = "0:0,1:1\n";
    _fakeIndex.attributes = "aux_attr: int32_t : 1,2;"
                            "type: int32_t : 1,2,3,4;";
    initMultiRoundRankProfileManager(_rankProfileMgrPtr, 10000, 5);
    string query = "config=cluster:cluster1&&query=term&&kvpairs=aux_key_first:1,score_second:type,score_first:type";
    ResultPtr result = innerTestQuery(query, "3,2,1,0", 4);
    vector<int32_t*> vec = result->getGlobalVarialbes<int32_t>("aux_value");
    ASSERT_EQ(size_t(1), vec.size());
    ASSERT_EQ(int32_t(2), *vec[0]);
    }*/

TEST_F(MatchDocSearcherTest, testMatchDataInSubGroupMode) {
    initRankProfileManager(_rankProfileMgrPtr, numeric_limits<uint32_t>::max(), 100);

    _fakeIndex.tablePrefix.push_back("sub_");
    _fakeIndex.indexes["phrase"] = "term:0;2\n";
    _fakeIndex.indexes["sub_index_name"] = "sub_term:0;3;6\n";
    // main 0, 1, 2, 3
    // sub  0,1,2;3,4;5,6;7,8
    // query will recall 0,1,2
    QueryExecutorTestHelper::addSubDocAttrMap(_fakeIndex, "3,5,7,9");
    string query = "config=cluster:cluster1,sub_doc:group,batch_score:no,optimize_rank:no&&query=phrase:term OR sub_index_name:sub_term&&sort=+RANK";

    innerTestQuery(query, "1,0,2", 3);
}

TEST_F(MatchDocSearcherTest, testSeekWithLayerMeta) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;";
    string query = "config=hit:4,cluster:cluster1&&query=phrase:with"
                   "&&aggregate=group_key:type,agg_fun:count()&&sort=price;type";
    string expectAggResult = "1:3;2:3";
    innerTestSeek(query, "2,3,4,5", LayerMetasPtr(), expectAggResult);
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 3});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:3;2:1";
        innerTestSeek(query, "0,1,2,3", layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 4});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:3;2:2";
        innerTestSeek(query, "1,2,3,4", layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:3;2:3";
        innerTestSeek(query, "2,3,4,5", layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({4, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "2:2";
        innerTestSeek(query, "4,5", layerMetas, expectAggResult);
    }
    { //empty layer meta
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "";
        innerTestSeek(query, "", layerMetas, expectAggResult);
    }
    { //invalid range
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({6, 10});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "";
        innerTestSeek(query, "", layerMetas, expectAggResult);
    }
    { // multi range
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 2});
        layerMeta.push_back({4, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:3;2:2";
        innerTestSeek(query, "1,2,4,5", layerMetas, expectAggResult);
    }
}

void MatchDocSearcherTest::checkTrace(const ResultPtr& result,
                                      int idx, const string &traces,
                                      const string &notExistTraces)
{
    MatchDocs * matchDocs = result->getMatchDocs();
    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    matchdoc::Reference<Tracer> *tracerRefer = allocatorPtr->findReference<Tracer>(RANK_TRACER_NAME);
    ASSERT_TRUE(tracerRefer);
    ASSERT_TRUE((uint32_t)idx < result->getTotalMatchDocs());
    HA3_NS(common)::Tracer *tracer = tracerRefer->getPointer(matchDocs->getMatchDoc(idx));
    ASSERT_TRUE(tracer);

    vector<string> traceVec;
    autil::StringUtil::fromString<string>(traces, traceVec, ";");
    for (size_t i = 0; i < traceVec.size(); ++i) {
        string debugStr = tracer->getTraceInfo() + ":" + traceVec[i];
        if (!(tracer->getTraceInfo().find(traceVec[i]) != string::npos)){
            HA3_LOG(ERROR, "get trace failed, traceInfo is [%s]", debugStr.c_str());
            assert(false);
        }
    }
    vector<string> notExsitTraceVec;
    autil::StringUtil::fromString<string>(notExistTraces, notExsitTraceVec, ";");
    for (size_t i = 0; i < notExsitTraceVec.size(); ++i) {
        string debugStr = tracer->getTraceInfo() + ":" + notExsitTraceVec[i];
        if (!(tracer->getTraceInfo().find(notExsitTraceVec[i]) == string::npos)){
            HA3_LOG(ERROR, "get trace failed, traceInfo is [%s]", debugStr.c_str());
            assert(false);
        }
    }
}

void MatchDocSearcherTest::initMultiRoundRankProfileManager(
        RankProfileManagerPtr &rankProfileMgrPtr, int32_t rankSize,
        int32_t rerankSize)
{
    ResourceReaderPtr resourceReader(new ResourceReader(TEST_DATA_PATH"/searcher_test/"));
    PlugInManagerPtr plugInMgrPtr(new PlugInManager(resourceReader));
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "multiphasescorer";
    moduleInfo.modulePath = "libmultiphasescorer.so";
    ModuleInfos moduleInfos;
    moduleInfos.push_back(moduleInfo);
    ASSERT_TRUE(plugInMgrPtr->addModules(moduleInfos));
    rankProfileMgrPtr.reset(new RankProfileManager(plugInMgrPtr));
    ScorerInfos scorerInfos;
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "Scorer_first";
    scorerInfo.moduleName = "multiphasescorer";
    scorerInfo.rankSize = rankSize;
    scorerInfos.push_back(scorerInfo);
    scorerInfo.scorerName = "Scorer_second";
    scorerInfo.moduleName = "multiphasescorer";
    scorerInfo.rankSize = rerankSize;
    if (rerankSize >= 0) {
        scorerInfos.push_back(scorerInfo);
    }

    rankProfileMgrPtr->addRankProfile(
            new RankProfile("DefaultProfile", scorerInfos));
    rankProfileMgrPtr->init(resourceReader, _cavaPluginManagerPtr, NULL);
}

ResultPtr MatchDocSearcherTest::innerTestQuery(
        const string &query,
        const string &resultStr,
        uint32_t totalHit,
        const string &aggResultStr,
        bool searchSuccess)
{
    DELETE_AND_SET_NULL(_requestTracer);
    _request = prepareRequest(query, _tableInfoPtr, _matchDocSearcherCreatorPtr);
    if (!_request) {
        HA3_LOG(ERROR, "query is [%s]", query.c_str());
        assert(false);
    }
    if (_fakeIndex.indexes.find("pk") == _fakeIndex.indexes.end()) {
        _fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
    }
    _partitionReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(_fakeIndex);

    _requestTracer = _request->getConfigClause()->createRequestTracer("fake_part_id", "fake_address");

    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(_request.get(),
            _partitionReaderPtr, _snapshotPtr, _rankProfileMgrPtr.get());

    searcher->_seekAndRankProcessor._rankSearcher._aggSamplerConfigInfo = *_aggSamplerConfigInfo;
    TimeoutTerminator timeoutTerminator(0, 0);
    searcher->_resource.timeoutTerminator = &timeoutTerminator;
    searcher->_resource.tracer = _requestTracer;
    _request->setPool(_pool);
    ResultPtr result = searcher->search(_request.get());
    if (!searchSuccess) {
        if (!result->hasError()) {
            HA3_LOG(ERROR, "query is [%s]", query.c_str());
            assert(false);
        }
        return result;
    } else {
        if (result->hasError()) {
            HA3_LOG(ERROR, "query is [%s]", query.c_str());
            assert(false);
        }
    }
    assert(result);
    if (!((uint32_t)totalHit == result->getTotalMatchDocs())) {
        HA3_LOG(ERROR, "query is [%s]", query.c_str());
        assert(false);
    }
    auto matchDocs = result->getMatchDocs();
    if (matchDocs) {
        auto phaseOneInfoMask = _request->getConfigClause()
                                ->getPhaseOneBasicInfoMask();
        versionid_t versionId = _partitionReaderPtr->getCurrentVersion();
        //const auto &searcherResource = searcher->_searcherResource;
        SearcherResource searcherResource;
        matchDocs->setGlobalIdInfo(searcherResource.getHashIdRange().from(),
                versionId, searcherResource.getFullIndexVersion(), -1,
                phaseOneInfoMask);
        matchDocs->setClusterId(0);
    }
    assert(matchDocs);
    vector<docid_t> results = SearcherTestHelper::covertToResultDocIds(resultStr);

    if (!((uint32_t)results.size() == matchDocs->size())) {
        HA3_LOG(ERROR, "query is [%s]", query.c_str());
        assert(false);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!(results[i] == matchDocs->getMatchDoc(i).getDocId())) {
            HA3_LOG(ERROR, "query is [%s]", query.c_str());
            assert(false);
        }
    }
    if (!aggResultStr.empty()) {
        const common::AggregateResults &aggResults = result->getAggregateResults();
        SearcherTestHelper::checkAggregatorResult(aggResults, aggResultStr);
    }

    return result;
}

void MatchDocSearcherTest::innerTestSeek(
        const string &query,
        const string &resultStr,
        const LayerMetasPtr &layerMetas,
        const string &aggResultStr,
        bool searchSuccess)
{
    DELETE_AND_SET_NULL(_requestTracer);
    _request = prepareRequest(query, _tableInfoPtr, _matchDocSearcherCreatorPtr);
    if (!_request) {
        HA3_LOG(ERROR, "query is [%s]", query.c_str());
        assert(false);
    }
    if (_fakeIndex.indexes.find("pk") == _fakeIndex.indexes.end()) {
        _fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
    }
    _partitionReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(_fakeIndex);

    _requestTracer = _request->getConfigClause()->createRequestTracer("fake_part_id", "fake_address");

    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(
            _request.get(), _partitionReaderPtr, _snapshotPtr,
            _rankProfileMgrPtr.get(), layerMetas);

    searcher->_seekAndRankProcessor._rankSearcher._aggSamplerConfigInfo = *_aggSamplerConfigInfo;
    TimeoutTerminator timeoutTerminator(0, 0);
    searcher->_resource.timeoutTerminator = &timeoutTerminator;
    searcher->_resource.tracer = _requestTracer;
    _request->setPool(_pool);

    InnerSearchResult innerResult(_pool);
    bool ret = searcher->seek(_request.get(), nullptr, innerResult);
    ASSERT_EQ(searchSuccess, ret);
    vector<docid_t> expectResults =
        SearcherTestHelper::covertToResultDocIds(resultStr);
    vector<docid_t> actualResults;
    actualResults.reserve(innerResult.matchDocVec.size());
    for (size_t idx=0; idx<innerResult.matchDocVec.size(); ++idx) {
        actualResults.push_back(innerResult.matchDocVec[idx].getDocId());
    }
    std::sort(expectResults.begin(), expectResults.end());
    std::sort(actualResults.begin(), actualResults.end());
    if (expectResults != actualResults) {
        cout << "====actual result===" << endl;
        for (auto docid : actualResults) {
            cout << docid << ", ";
        }
        cout << endl << "====expect result===" << endl;
        for (auto docid : expectResults) {
            cout << docid << ", ";
        }
        cout << endl;
        ASSERT_TRUE(false);
    }
    if (!aggResultStr.empty()) {
        SearcherTestHelper::checkAggregatorResult(*innerResult.aggResultsPtr,
                aggResultStr);
    }
}

AnalyzerFactoryPtr MatchDocSearcherTest::initFactory() {
    string basePath = ALIWSLIB_DATA_PATH;
    string aliwsConfig = basePath + "/conf/";
    TokenizerManagerPtr _tokenizerManagerPtr;
    BuildInTokenizerFactoryPtr _tokenizerFactoryPtr;
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    _tokenizerFactoryPtr.reset(new BuildInTokenizerFactory());
    ResourceReaderPtr resourceReader(new ResourceReader(aliwsConfig));
    bool ret = _tokenizerFactoryPtr->init(KeyValueMap(), resourceReader);
    (void)ret;
    assert(ret);
    _tokenizerManagerPtr.reset(new TokenizerManager(resourceReader));
    _tokenizerManagerPtr->setBuildInFactory(_tokenizerFactoryPtr);
    Tokenizer *tokenizer = NULL;
    AnalyzerInfo aliwsInfo ;
    aliwsInfo.setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    string tokenizerName = "singlews";
    aliwsInfo.setTokenizerName(tokenizerName);
    aliwsInfo.addStopWord(string("."));
    aliwsInfo.addStopWord(string(","));
    aliwsInfo.addStopWord(string("-"));
    aliwsInfo.addStopWord(string("mm"));
    NormalizeOptions option;
    option.caseSensitive = false;
    aliwsInfo.setNormalizeOptions(option);
    infosPtr->addAnalyzerInfo("default", aliwsInfo);
    tokenizer = _tokenizerFactoryPtr->createTokenizer("singlews");
    ret = tokenizer->init(aliwsInfo.getTokenizerConfigs(), resourceReader);
    assert(ret);
    _tokenizerManagerPtr->addTokenizer(tokenizerName, tokenizer);
    AnalyzerFactory *analyzerFactory = new AnalyzerFactory();
    analyzerFactory->init(infosPtr, _tokenizerManagerPtr);
    AnalyzerFactoryPtr ptr(analyzerFactory);
    return ptr;
}

Query* MatchDocSearcherTest::parseQuery(const char* query, const char*msg) {
    _ctx = _queryParser->parse(query);
    HA3_LOG(DEBUG, "msg is [%s]", msg);
    assert(_ctx);
    assert(ParserContext::OK == _ctx->getStatus());
    assert(size_t(1) == _ctx->getQuerys().size());
    assert(_ctx->getQuerys()[0]);
    return _ctx->stealQuerys()[0];
}

IndexPartitionReaderWrapperPtr MatchDocSearcherTest::makeIndexPartitionReaderWrapper(
        const IndexPartitionReaderPtr &indexPartReaderPtr)
{
    static map<string, uint32_t> attrName2IdMap;
    vector<IndexPartitionReaderPtr> indexPartReaderPtrs;
    indexPartReaderPtrs.push_back(indexPartReaderPtr);
    IndexPartitionReaderWrapperPtr  wrapperPtr(
            new IndexPartitionReaderWrapper(NULL, &attrName2IdMap,
                    indexPartReaderPtrs));
    wrapperPtr->setTopK(10);
    return wrapperPtr;
}

void MatchDocSearcherTest::innerTestSeekAndJoin(const string &query,
        const string &resultStr,
        HashJoinInfo *hashJoinInfo,
        const LayerMetasPtr &layerMetas,
        const string &aggResultStr,
        bool searchSuccess) {
    DELETE_AND_SET_NULL(_requestTracer);
    _request = prepareRequest(query, _tableInfoPtr, _matchDocSearcherCreatorPtr);
    if (!_request) {
        HA3_LOG(ERROR, "query is [%s]", query.c_str());
        assert(false);
    }
    if (_fakeIndex.indexes.find("pk") == _fakeIndex.indexes.end()) {
        _fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
    }
    _partitionReaderPtr = FakeIndexPartitionReaderCreator::createIndexPartitionReader(_fakeIndex);

    _requestTracer = _request->getConfigClause()->createRequestTracer("fake_part_id", "fake_address");

    MatchDocSearcherPtr searcher = _matchDocSearcherCreatorPtr->createSearcher(
            _request.get(), _partitionReaderPtr, _snapshotPtr,
            _rankProfileMgrPtr.get(), layerMetas);

    searcher->_seekAndRankProcessor._rankSearcher._aggSamplerConfigInfo = *_aggSamplerConfigInfo;
    TimeoutTerminator timeoutTerminator(0, 0);
    searcher->_resource.timeoutTerminator = &timeoutTerminator;
    searcher->_resource.tracer = _requestTracer;
    _request->setPool(_pool);

    InnerSearchResult innerResult(_pool);
    bool ret = searcher->seekAndJoin(_request.get(), nullptr, hashJoinInfo, innerResult);
    ASSERT_EQ(searchSuccess, ret);
    vector<docid_t> expectResults =
        SearcherTestHelper::covertToResultDocIds(resultStr);
    vector<docid_t> actualResults;
    actualResults.reserve(innerResult.matchDocVec.size());
    for (size_t idx=0; idx<innerResult.matchDocVec.size(); ++idx) {
        actualResults.push_back(innerResult.matchDocVec[idx].getDocId());
    }
    std::sort(expectResults.begin(), expectResults.end());
    std::sort(actualResults.begin(), actualResults.end());
    if (expectResults != actualResults) {
        cout << "====actual result===" << endl;
        for (auto docid : actualResults) {
            cout << docid << ", ";
        }
        cout << endl << "====expect result===" << endl;
        for (auto docid : expectResults) {
            cout << docid << ", ";
        }
        cout << endl;
        ASSERT_TRUE(false);
    }
    if (!aggResultStr.empty()) {
        SearcherTestHelper::checkAggregatorResult(*innerResult.aggResultsPtr,
                aggResultStr);
    }
}

HashJoinInfoPtr MatchDocSearcherTest::buildHashJoinInfo(
        const std::unordered_map<size_t, std::vector<int32_t>> &hashJoinMap) {
    HashJoinInfoPtr hashJoinInfo(
            new HashJoinInfo(_auxTableName, _joinFieldName));
    hashJoinInfo->getHashJoinMap() = hashJoinMap;
    return hashJoinInfo;
}

TEST_F(MatchDocSearcherTest, testSeekAndJoinWithLayerMeta) {
    HA3_LOG(DEBUG, "Begin Test!");
    _fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
    _fakeIndex.attributes = "type:  int32_t : 1, 1, 1, 2, 2, 2;"
                            "price: int32_t : 1, 2, 6, 3, 4, 5;"
                            "store_id: int32_t : 1, 2, 3, 5, 6, 7;";
    string query = "config=hit:4,cluster:cluster1&&query=phrase:with"
                   "&&aggregate=group_key:type,agg_fun:count()&&sort=price;type";
    string expectAggResult = "1:5;2:3";
    std::unordered_map<size_t, vector<int32_t>> hashJoinMap{
        { 1, { 0, 1 } },
	{ 2, { 2 } },
	{ 3, { 3, 4 } },
	{ 4, { 5 } }
    };
    HashJoinInfoPtr hashJoinInfo = buildHashJoinInfo(hashJoinMap);
    innerTestSeekAndJoin(
            query, "2,2,4,5", hashJoinInfo.get(), LayerMetasPtr(), expectAggResult);
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 3});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:5;2:1";
        innerTestSeekAndJoin(
                query, "1,2,2,3", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 4});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:5;2:2";
        innerTestSeekAndJoin(
                query, "2,2,3,4", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:5;2:3";
        innerTestSeekAndJoin(
                query, "2,2,4,5", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({4, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "2:2";
        innerTestSeekAndJoin(
                query, "4,5", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    { //empty layer meta
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "";
        innerTestSeekAndJoin(query, "", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    { //invalid range
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({6, 10});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "";
        innerTestSeekAndJoin(
                query, "", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
    { // multi range
        LayerMetasPtr layerMetas(new LayerMetas(_pool));
        LayerMeta layerMeta(_pool);
        layerMeta.push_back({0, 2});
        layerMeta.push_back({4, 5});
        layerMeta.quota = std::numeric_limits<uint32_t>::max();
        layerMetas->push_back(layerMeta);
        expectAggResult = "1:5;2:2";
        innerTestSeekAndJoin(
                query, "2,2,4,5", hashJoinInfo.get(), layerMetas, expectAggResult);
    }
}

END_HA3_NAMESPACE(search);
