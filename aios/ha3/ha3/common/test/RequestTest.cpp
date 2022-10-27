#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/common/Request.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/test/test.h>
#include <ha3/common/Request.h>
#include <ha3/common/TermQuery.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <multi_call/common/ControllerParam.h>

using namespace std;
using namespace suez::turing;
using namespace autil;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class RequestTest : public TESTBASE {
public:
    RequestTest();
    ~RequestTest();
public:
    void setUp();
    void tearDown();
protected:
    void serializeRequest(const Request& request,
                          Request& request2);
protected:
    queryparser::RequestParser *_requestParser;
    autil::DataBuffer _buffer;

protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RequestTest);


RequestTest::RequestTest() { 
    _requestParser = NULL;
}

RequestTest::~RequestTest() { 
}

void RequestTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _requestParser = new RequestParser();
    _buffer.clear();
}

void RequestTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _requestParser;
}

TEST_F(RequestTest, testEmptyRequestSerialize) {
    Request request;
    Request request2;
    serializeRequest(request, request2);
    
    ASSERT_EQ(string(DOCID_PARTITION_MODE), 
                         request2.getPartitionMode());    
    ASSERT_TRUE(!request2.getQueryClause());
    ASSERT_TRUE(!request2.getSortClause());
    ASSERT_TRUE(!request2.getAggregateClause());
    ASSERT_TRUE(!request2.getRankClause());
    ASSERT_TRUE(!request2.getConfigClause());
    ASSERT_TRUE(!request2.getClusterClause());
    ASSERT_TRUE(!request2.getPKFilterClause());    
    ASSERT_TRUE(!request2.getHealthCheckClause());
    ASSERT_TRUE(!request2.getAttributeClause());
    ASSERT_TRUE(!request2.getVirtualAttributeClause());
    ASSERT_TRUE(!request2.getFinalSortClause());
    ASSERT_TRUE(!request2.getAuxQueryClause());
    ASSERT_TRUE(!request2.getAuxFilterClause());
}

TEST_F(RequestTest, testMultiSerialize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Request request;
    request.setOriginalString("config=query");
    request.setPartitionMode("usrid");

    //add query clause
    QueryClause *queryClause = new QueryClause;
    RequiredFields requiredFields;
    Term term("one", "index", requiredFields);
    TermQuery *termQuery = new TermQuery(term, "");
    queryClause->setRootQuery(termQuery);
    string queryStr = "index:书包";
    queryClause->setOriginalString(queryStr);
    request.setQueryClause(queryClause);

    //add filter clause
    SyntaxExpr *filterExpr
        = new AtomicSyntaxExpr("aaa", vt_int32, ATTRIBUTE_NAME);
    FilterClause *filterClause = new FilterClause(filterExpr);
    filterClause->setOriginalString("aaa");
    request.setFilterClause(filterClause);
    //add pkFilter clause
    PKFilterClause *pkFilterClause = new PKFilterClause;
    pkFilterClause->setOriginalString("aaa");
    request.setPKFilterClause(pkFilterClause); 
    //add sort clause
    SortDescription *sortDescription = new SortDescription("type");
    sortDescription->setExpressionType(SortDescription::RS_NORMAL);
    SyntaxExpr *sortExpr = new AtomicSyntaxExpr("id", vt_int32, ATTRIBUTE_NAME);
    sortDescription->setRootSyntaxExpr(sortExpr);
    SortClause *sortClause = new  SortClause;
    sortClause->addSortDescription(sortDescription);
    sortClause->setOriginalString("type");
    request.setSortClause(sortClause);

    //add distinct clause
    SyntaxExpr *distExpr = new AtomicSyntaxExpr("userid", vt_int32, ATTRIBUTE_NAME);
    DistinctClause *distinctClause = new DistinctClause;
    DistinctDescription *distDesc = new DistinctDescription(DEFAULT_DISTINCT_MODULE_NAME,
            "dist_key:userid,dist_count=1,dist_times=1,update_total_hit:true,max_item_count:200",
            1, 1, 200, false, true, distExpr, NULL);
    distinctClause->addDistinctDescription(distDesc);
    distinctClause->setOriginalString("dist_key:userid,dist_count=1,dist_times=1,update_total_hit:true,max_item_count:200");
    request.setDistinctClause(distinctClause);

    //add aggregation clause
    AggregateDescription *aggDescription 
        = new AggregateDescription("groupkey:type, max(price)");
    AggregateClause *aggClause = new AggregateClause();
    aggClause->addAggDescription(aggDescription);
    aggClause->setOriginalString("groupkey:type, max(price)");
    request.setAggregateClause(aggClause);

    //add rank clause
    RankClause *rankClause = new RankClause;
    rankClause->setRankProfileName("rank.conf");
    rankClause->setOriginalString("rank.conf");
    request.setRankClause(rankClause);

    //add config clause
    string requestString = "table:phrase, start:10, hit:100, rerank:20, format:xml";
    ConfigClause *requestConfig = new ConfigClause;
    requestConfig->setOriginalString(requestString);
    requestConfig->addClusterName("phrase");
    requestConfig->setStartOffset(10);
    requestConfig->setHitCount(100);
    requestConfig->setResultFormatSetting("xml");
    request.setConfigClause(requestConfig);

    //add cluster clause
    string clusterString = "cluster1:hash_field=user1|user2,cluster2:part_ids=1|3";
    ClusterClause * clusterClause = new ClusterClause;
    clusterClause->setOriginalString(clusterString);
    vector<pair<hashid_t, hashid_t> > partIds;
    partIds.push_back({1, 1});
    partIds.push_back({3, 3});
    clusterClause->addClusterPartIds(string("cluster1"), partIds);
    clusterClause->addClusterPartIds(string("cluster2"), partIds);
    request.setClusterClause(clusterClause);

    //add healthCheck clause
    string healthCheckString = "check:true,check_times=3,recover_time:3";
    HealthCheckClause *healthCheckClause = new HealthCheckClause;
    healthCheckClause->setOriginalString(healthCheckString);
    healthCheckClause->setCheck(true);
    healthCheckClause->setCheckTimes(3);
    healthCheckClause->setRecoverTime(3000000);
    request.setHealthCheckClause(healthCheckClause);
    
    //add attribute clause
    string attributeString = "attr1,attr2";
    AttributeClause *attributeClause = new AttributeClause;
    attributeClause->setOriginalString(attributeString);
    attributeClause->addAttribute("attr1");
    attributeClause->addAttribute("attr2");
    request.setAttributeClause(attributeClause);

    //add virtualAttribute clause
    string virtualAttributeString = "var1:attr1,var2:attr2";
    VirtualAttributeClause *virtualAttributeClause = new VirtualAttributeClause;
    virtualAttributeClause->setOriginalString(virtualAttributeString);
    request.setVirtualAttributeClause(virtualAttributeClause);
    
    //add finalSort clause
    string finalSortString = "sort_name:taobao_sort;sort_params:key1|value1,key2|value2";
    FinalSortClause *finalSortClause = new FinalSortClause;
    finalSortClause->setOriginalString(finalSortString);
    finalSortClause->setSortName("taobao_sort");
    finalSortClause->addParam("key1", "value1");
    finalSortClause->addParam("key2", "value2");
    request.setFinalSortClause(finalSortClause);

    // add searcherCache clause
    string searcherCacheClauseString = "use:yes";
    SearcherCacheClause *searcherCacheClause = new SearcherCacheClause;
    searcherCacheClause->setUse(true);
    searcherCacheClause->setOriginalString(searcherCacheClauseString);
    
    request.setSearcherCacheClause(searcherCacheClause);
    
    Request request2;
    serializeRequest(request, request2);

    string expectedString = "config=table:phrase, start:10, hit:100, rerank:20, format:xml"
                            "&&aggregate=groupkey:type, max(price)"
                            "&&distinct=dist_key:userid,dist_count=1,dist_times=1,update_total_hit:true,max_item_count:200"
                            "&&filter=aaa&&pkfilter=aaa&&query=index:书包"
                            "&&rank=rank.conf&&sort=type"
                            "&&cluster=cluster1:hash_field=user1|user2,cluster2:part_ids=1|3"
                            "&&healthcheck=check:true,check_times=3,recover_time:3"
                            "&&attribute=attr1,attr2"
                            "&&virtual_attribute=var1:attr1,var2:attr2"
                            "&&searcher_cache=use:yes"
                            "&&final_sort=sort_name:taobao_sort;sort_params:key1|value1,key2|value2";

    ASSERT_EQ(expectedString, request2.getOriginalString());
    ASSERT_EQ(string("usrid"), request2.getPartitionMode());

    //assert query clause
    QueryClause *queryClause2 = request2.getQueryClause();
    ASSERT_EQ(queryClause->getOriginalString(), 
                         queryClause2->getOriginalString());
    const Query *query = queryClause2->getRootQuery();
    ASSERT_EQ(*query, *(Query *)termQuery);

    //assert filter clause
    FilterClause *filterClause2 = request2.getFilterClause();
    ASSERT_TRUE(filterClause2);
    ASSERT_EQ(filterClause->getOriginalString(),
                         filterClause2->getOriginalString());
    ASSERT_TRUE(*filterExpr == filterClause2->getRootSyntaxExpr());

    //assert pkFilter clause
    PKFilterClause *pkFilterClause2 = request2.getPKFilterClause();
    ASSERT_TRUE(pkFilterClause2);
    ASSERT_EQ(pkFilterClause->getOriginalString(),
                         pkFilterClause2->getOriginalString());

    //assert sort clause
    SortClause *sortClause2 = request2.getSortClause();
    ASSERT_EQ(string("type"), sortClause2->getOriginalString());
    const vector<SortDescription*>& sortDescriptions = 
        sortClause2->getSortDescriptions();
    ASSERT_EQ((size_t)1, sortDescriptions.size());
    ASSERT_EQ(string("type"), sortDescriptions[0]->getOriginalString());
    ASSERT_EQ(false, sortDescriptions[0]->getSortAscendFlag());
    ASSERT_TRUE(*sortExpr == sortDescriptions[0]->getRootSyntaxExpr());

    //assert distinct clause
    DistinctClause *distinctClause2 = request2.getDistinctClause();
    ASSERT_TRUE(distinctClause2);
    ASSERT_EQ((uint32_t)1, 
                         distinctClause2->getDistinctDescriptionsCount());
    const DistinctDescription *distDesc2 = distinctClause2->getDistinctDescription(0);
    
    ASSERT_EQ(1, distDesc2->getDistinctCount());
    ASSERT_EQ(1, distDesc2->getDistinctTimes());
    ASSERT_EQ(200, distDesc2->getMaxItemCount());
    ASSERT_TRUE(!distDesc2->getReservedFlag());
    ASSERT_TRUE(distDesc2->getUpdateTotalHitFlag());
    ASSERT_EQ(string("dist_key:userid,dist_count=1,dist_times=1,update_total_hit:true,max_item_count:200"), 
                         distDesc2->getOriginalString());
    ASSERT_TRUE(*distExpr == distDesc2->getRootSyntaxExpr());

    //assert aggregate clause
    AggregateClause *aggClause2 = request2.getAggregateClause();
    ASSERT_EQ(string("groupkey:type, max(price)"), 
                         aggClause2->getOriginalString());
    const vector<AggregateDescription*>& aggDescriptions = 
        aggClause2->getAggDescriptions();
    ASSERT_EQ((size_t)1, aggDescriptions.size());
    ASSERT_EQ(string("groupkey:type, max(price)"), 
                         aggDescriptions[0]->getOriginalString());

    //assert rank clause
    RankClause *rankClause2 = request2.getRankClause();
    ASSERT_EQ(string("rank.conf"), rankClause2->getOriginalString());
    ASSERT_EQ(string("rank.conf"), rankClause2->getRankProfileName());

    //assert config clause
    ConfigClause *requestConfig2 = request2.getConfigClause();
    ASSERT_TRUE(requestConfig);
    ASSERT_EQ(requestString, requestConfig2->getOriginalString());
    ASSERT_EQ(string("phrase.default"), requestConfig2->getClusterName());
    ASSERT_EQ((uint32_t)10, requestConfig2->getStartOffset());
    ASSERT_EQ((uint32_t)100, requestConfig2->getHitCount());
    ASSERT_EQ(string("xml"), requestConfig2->getResultFormatSetting());

    //assert cluster clause
    ClusterClause *clusterClause2 = request2.getClusterClause();
    ASSERT_TRUE(clusterClause2);
    ASSERT_EQ(clusterString, clusterClause2->getOriginalString());
    const ClusterPartIdsMap& clusterPartIdsMap = 
        clusterClause2->getClusterPartIdsMap();    
    ASSERT_EQ((size_t)2, clusterPartIdsMap.size());
    vector<pair<hashid_t, hashid_t> > partIds2;
    bool ret = clusterClause2->getClusterPartIds(string("cluster1.default"), partIds2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)2, partIds2.size());
    ASSERT_EQ((hashid_t)1, partIds2[0].first);
    ASSERT_EQ((hashid_t)1, partIds2[0].second);
    ASSERT_EQ((hashid_t)3, partIds2[1].first);
    ASSERT_EQ((hashid_t)3, partIds2[1].second);
    ret = clusterClause2->getClusterPartIds(string("cluster2.default"), partIds2);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)2, partIds2.size());
    ASSERT_EQ((hashid_t)1, partIds2[0].first);
    ASSERT_EQ((hashid_t)1, partIds2[0].second);
    ASSERT_EQ((hashid_t)3, partIds2[1].first);
    ASSERT_EQ((hashid_t)3, partIds2[1].second);

    //assert healthCheck clause
    HealthCheckClause *healthCheckClause2 = request2.getHealthCheckClause();
    ASSERT_TRUE(healthCheckClause2);
    ASSERT_EQ(healthCheckString, healthCheckClause2->getOriginalString());
    ASSERT_TRUE(healthCheckClause2->isCheck());
    ASSERT_EQ((int32_t)3, healthCheckClause2->getCheckTimes());
    ASSERT_EQ((int64_t)3000000, healthCheckClause2->getRecoverTime());

    //assert attribute clause
    AttributeClause *attributeClause2 = request2.getAttributeClause();
    ASSERT_TRUE(attributeClause2);
    ASSERT_EQ(attributeString, attributeClause2->getOriginalString());
    ASSERT_EQ((size_t)2, attributeClause2->getAttributeNames().size());
    
    //assert virtualAttribute clause
    VirtualAttributeClause *virtualAttributeClause2 = request2.getVirtualAttributeClause();
    ASSERT_TRUE(virtualAttributeClause2);
    ASSERT_EQ(virtualAttributeString, virtualAttributeClause2->getOriginalString());

    //assert searcherCache clause
    SearcherCacheClause *searcherCacheClause2 = request2.getSearcherCacheClause();
    ASSERT_TRUE(searcherCacheClause2);
    ASSERT_EQ(searcherCacheClauseString, 
                         searcherCacheClause2->getOriginalString());
    ASSERT_TRUE(searcherCacheClause2->getUse());

    //assert finalsort clause
    FinalSortClause *finalSortClause2 = request2.getFinalSortClause();
    ASSERT_TRUE(finalSortClause2);
    ASSERT_EQ(finalSortString, 
                         finalSortClause2->getOriginalString());
    ASSERT_EQ(string("taobao_sort"), 
                     finalSortClause2->getSortName());
    map<string, string> sortParams = finalSortClause2->getParams();
    ASSERT_EQ(size_t(2), sortParams.size());
    ASSERT_EQ(string("value1"), sortParams["key1"]);
    ASSERT_EQ(string("value2"), sortParams["key2"]);
}

TEST_F(RequestTest, testSerializeRequestWithDocIdClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    Request request;
    DocIdClause *docIdClause = new DocIdClause;
    docIdClause->addGlobalId(GlobalIdentifier(5, 1));
    request.setDocIdClause(docIdClause);

    Request request2;
    serializeRequest(request, request2);

    DocIdClause *resultDocIdClause = request2.getDocIdClause();
    ASSERT_TRUE(resultDocIdClause);

    GlobalIdVector gids;
    proto::Range range;
    range.set_from(1);
    range.set_to(1);    
    resultDocIdClause->getGlobalIdVector(range, gids);
    ASSERT_EQ((size_t)1, gids.size());
    ASSERT_EQ((docid_t)5, gids[0].getDocId());
}

TEST_F(RequestTest, testParseAndSerializeAggClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    string originalString= "group_key:type, agg_fun:count()";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=" + 
                        originalString + 
                        "&&cluster=cluster1:part_ids=0";

    ClusterConfigInfo *clusterConfigInfo = new ClusterConfigInfo();
    clusterConfigInfo->_tableName = "table1";
    clusterConfigInfo->_hashMode._hashFields.push_back("any");
    clusterConfigInfo->_hashMode._hashFunction = "HASH";
    unique_ptr<ClusterConfigInfo> configInfoPtr(clusterConfigInfo);
    ASSERT_TRUE(clusterConfigInfo->initHashFunc());
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    clusterConfigMapPtr->insert(make_pair("cluster1.default", *clusterConfigInfo));

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, clusterConfigMapPtr);
    ASSERT_EQ(ERROR_NONE, _requestParser->getErrorResult().getErrorCode());
    ASSERT_TRUE(ret);

    ASSERT_TRUE(requestPtr->getConfigClause());
    ASSERT_TRUE(requestPtr->getAggregateClause());

    Request request2;
    serializeRequest(*requestPtr, request2);

    AggregateClause* aggClause = request2.getAggregateClause();
    ASSERT_TRUE(aggClause);
    const AggregateDescriptions& aggDes =
        aggClause->getAggDescriptions();

    ASSERT_EQ((size_t)1, aggDes.size());
    ASSERT_EQ(originalString, aggDes[0]->getOriginalString());

    ASSERT_TRUE(aggDes[0]->getGroupKeyExpr());
    AtomicSyntaxExpr groupKeyExpr("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(groupKeyExpr == aggDes[0]->getGroupKeyExpr());
}

TEST_F(RequestTest, testParseAndSerializeDistinctClauses) {
    HA3_LOG(DEBUG, "Begin Test!");
    string originalString= "dist_key:type1,dist_times:1,dist_count:1;"
                           "dist_key:type2,dist_times:2,dist_count:2";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&distinct=" + 
                        originalString + 
                        "&&cluster=cluster1:part_ids=0";

    ClusterConfigInfo *clusterConfigInfo = new ClusterConfigInfo();
    clusterConfigInfo->_tableName = "table1";
    clusterConfigInfo->_hashMode._hashFields.push_back("any");
    clusterConfigInfo->_hashMode._hashFunction = "HASH";
    unique_ptr<ClusterConfigInfo> configInfoPtr(clusterConfigInfo);
    ASSERT_TRUE(clusterConfigInfo->initHashFunc());

    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    clusterConfigMapPtr->insert(make_pair("cluster1.default", *clusterConfigInfo));

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, clusterConfigMapPtr);
    ASSERT_EQ(ERROR_NONE, _requestParser->getErrorResult().getErrorCode());
    ASSERT_TRUE(ret);

    ASSERT_TRUE(requestPtr->getConfigClause());
    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);
    ASSERT_EQ((uint32_t)2, distinctClause->getDistinctDescriptionsCount());

    Request request2;
    serializeRequest(*requestPtr, request2);

    distinctClause = request2.getDistinctClause();
    ASSERT_TRUE(distinctClause);
    const vector<DistinctDescription *> descriptions = 
        distinctClause->getDistinctDescriptions();
    ASSERT_EQ((size_t)2, descriptions.size());
    DistinctDescription *description = descriptions[0];
    ASSERT_TRUE(description);
    ASSERT_TRUE(description->getRootSyntaxExpr());
    AtomicSyntaxExpr rootSyntaxExpr1("type1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(rootSyntaxExpr1 == description->getRootSyntaxExpr());
    ASSERT_EQ((int32_t)1, description->getDistinctTimes());
    ASSERT_EQ((int32_t)1, description->getDistinctCount());
    
    description = descriptions[1];
    ASSERT_TRUE(description);
    ASSERT_TRUE(description->getRootSyntaxExpr());
    AtomicSyntaxExpr rootSyntaxExpr2("type2", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(rootSyntaxExpr2 == description->getRootSyntaxExpr());
    ASSERT_EQ((int32_t)2, description->getDistinctTimes());
    ASSERT_EQ((int32_t)2, description->getDistinctCount());
}

TEST_F(RequestTest, testParseDoubleDistincts) {
     HA3_LOG(DEBUG, "Begin Test!");
    string originalString= "distinct=dist_key:type1,dist_times:1,dist_count:1;&&"
                           "distinct=dist_key:type2,dist_times:2,dist_count:2";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&" + 
                        originalString + 
                        "&&cluster=cluster1:part_ids=0";

    ClusterConfigInfo *clusterConfigInfo = new ClusterConfigInfo();
    clusterConfigInfo->_tableName = "table1";
    clusterConfigInfo->_hashMode._hashFields.push_back("any");
    clusterConfigInfo->_hashMode._hashFunction = "HASH";
    unique_ptr<ClusterConfigInfo> configInfoPtr(clusterConfigInfo);
    ASSERT_TRUE(clusterConfigInfo->initHashFunc());

    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    clusterConfigMapPtr->insert(make_pair("cluster1.default", *clusterConfigInfo));

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, clusterConfigMapPtr);
    ASSERT_EQ(ERROR_NONE, _requestParser->getErrorResult().getErrorCode());
    ASSERT_TRUE(ret);

    ASSERT_TRUE(requestPtr->getConfigClause());
    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);
    ASSERT_EQ((uint32_t)1, distinctClause->getDistinctDescriptionsCount());
    
    const DistinctDescription *distDesc = distinctClause->getDistinctDescription(0);
    ASSERT_TRUE(distDesc);
    ASSERT_TRUE(distDesc->getRootSyntaxExpr());
    AtomicSyntaxExpr rootSyntaxExpr1("type2", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(rootSyntaxExpr1 == distDesc->getRootSyntaxExpr());
    ASSERT_EQ((int32_t)2, distDesc->getDistinctTimes());
    ASSERT_EQ((int32_t)2, distDesc->getDistinctCount());
}

void RequestTest::serializeRequest(const Request& request,
                                   Request& request2)
{
    autil::DataBuffer buffer;
    buffer.write(request);
    buffer.read(request2);
}

TEST_F(RequestTest, testSetAndGetOriginalString) {
    Request request;
    string originalString = "config = cluster:daogou && "
                            "query ='book' && "
                            "rank=rank_profile:mlr && "
                            "filter=rank_score>60 && "
                            "pkfilter=aaa && "
                            "cluster=daogou:part_ids=0 && "
                            "healthcheck=check:true,check_times:3 && "
                            "attribute=attr1,attr2 && "
                            "virtual_attribute=var1:attr1,var2:attr2 &&"
                            "fetch_summary=cluster1_3_3_5_6 &&"
                            "final_sort=sort_name:taobao_sort;"
                            "sort_params:key1|value1,key2|value2 &&"
                            "analyzer=index1:aliws;index2:singlews;index3:noTokenize &&"
	                    "aux_query=store_id:123 AND title:abc &&"
	                    "aux_filter=store_price>100"
	;

    request.setOriginalString(originalString);

    ASSERT_TRUE(request.getConfigClause());
    ASSERT_EQ(string("cluster:daogou"),
                         request.getConfigClause()->getOriginalString());

    ASSERT_TRUE(request.getQueryClause());
    ASSERT_EQ(string("'book'"), 
                         request.getQueryClause()->getOriginalString());

    ASSERT_TRUE(request.getFilterClause());
    ASSERT_EQ(string("rank_score>60"), 
                         request.getFilterClause()->getOriginalString());

    ASSERT_TRUE(request.getPKFilterClause());
    ASSERT_EQ(string("aaa"), 
                         request.getPKFilterClause()->getOriginalString());

    ASSERT_TRUE(request.getRankClause());
    ASSERT_EQ(string("rank_profile:mlr"), 
                         request.getRankClause()->getOriginalString());

    ASSERT_TRUE(request.getClusterClause());
    ASSERT_EQ(string("daogou:part_ids=0"), 
                         request.getClusterClause()->getOriginalString());

    ASSERT_TRUE(request.getHealthCheckClause());
    ASSERT_EQ(string("check:true,check_times:3"), 
                         request.getHealthCheckClause()->getOriginalString());

    ASSERT_TRUE(request.getAttributeClause());
    ASSERT_EQ(string("attr1,attr2"), 
                         request.getAttributeClause()->getOriginalString());

    ASSERT_TRUE(request.getVirtualAttributeClause());
    ASSERT_EQ(string("var1:attr1,var2:attr2"), 
                         request.getVirtualAttributeClause()->getOriginalString());

    ASSERT_TRUE(request.getFetchSummaryClause());
    ASSERT_EQ(string("cluster1_3_3_5_6"), 
                         request.getFetchSummaryClause()->getOriginalString());

    ASSERT_TRUE(request.getFinalSortClause());
    ASSERT_EQ(string("sort_name:taobao_sort;sort_params:key1|value1,key2|value2"), 
                         request.getFinalSortClause()->getOriginalString());
    ASSERT_TRUE(request.getAnalyzerClause());
    ASSERT_EQ(string("index1:aliws;index2:singlews;index3:noTokenize"), 
                         request.getAnalyzerClause()->getOriginalString());

    ASSERT_TRUE(!request.getSortClause());
    ASSERT_TRUE(!request.getDistinctClause());
    ASSERT_TRUE(!request.getAggregateClause());
    ASSERT_TRUE(!request.getDocIdClause());

    ASSERT_TRUE(request.getAuxQueryClause());
    ASSERT_EQ(string("store_id:123 AND title:abc"),
	request.getAuxQueryClause()->getOriginalString());

    ASSERT_TRUE(request.getAuxFilterClause());
    ASSERT_EQ(string("store_price>100"),
	request.getAuxFilterClause()->getOriginalString());
    
    string expectedString = "config=cluster:daogou&&filter=rank_score>60&&pkfilter=aaa&&"
                            "query='book'&&rank=rank_profile:mlr&&"
                            "cluster=daogou:part_ids=0&&healthcheck=check:true,check_times:3&&"
                            "attribute=attr1,attr2&&virtual_attribute=var1:attr1,var2:attr2&&"
                            "fetch_summary=cluster1_3_3_5_6&&"
                            "final_sort=sort_name:taobao_sort;sort_params:key1|value1,key2|value2&&"
                            "analyzer=index1:aliws;index2:singlews;index3:noTokenize&&"
	                    "aux_query=store_id:123 AND title:abc&&"
	                    "aux_filter=store_price>100"
	                    ;
    ASSERT_EQ(expectedString, request.getOriginalString());
}

TEST_F(RequestTest, testSetAndGetPKFilterOriginalString) {
    PKFilterClause *pkFilterClause1 = new PKFilterClause;
    pkFilterClause1->setOriginalString("aaa");
    Request request;
    request.setPKFilterClause(pkFilterClause1);

    PKFilterClause *pkFilterClause2 = new PKFilterClause;
    pkFilterClause2->setOriginalString("bbb");
    request.setPKFilterClause(pkFilterClause2);

    PKFilterClause *pkFilterClause3 = request.getPKFilterClause();
    ASSERT_TRUE(pkFilterClause3);
    ASSERT_EQ(string("bbb"),pkFilterClause3->getOriginalString());
}
TEST_F(RequestTest, testParseAndSerializePKFilterClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    string originalString= "pkfilter=aaa";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&" + 
                        originalString + 
                        "&&cluster=cluster1:part_ids=0";

    ClusterConfigInfo *clusterConfigInfo = new ClusterConfigInfo();
    clusterConfigInfo->_tableName = "table1";
    unique_ptr<ClusterConfigInfo> configInfoPtr(clusterConfigInfo);
    ClusterConfigMapPtr clusterConfigMapPtr(new ClusterConfigMap);
    clusterConfigMapPtr->insert(make_pair("cluster1", *clusterConfigInfo));

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(requestPtr->getPKFilterClause());

    Request request3;
    autil::DataBuffer buffer;
    buffer.write(*requestPtr);
    buffer.read(request3);
    PKFilterClause* pkFilterClause2 = request3.getPKFilterClause();
    ASSERT_TRUE(pkFilterClause2);
    string pk = pkFilterClause2->getOriginalString();

    ASSERT_EQ(string("aaa"), pk);
}

TEST_F(RequestTest, testSplitClauseWithTwoSortClause) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string originalString= "pkfilter=aaa";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&" + 
                        originalString + 
                        "&&cluster=cluster1:part_ids=0&&sort=id&&sort=+id";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
}

TEST_F(RequestTest, testSplitClauseWithAnalyzer) { 
    HA3_LOG(DEBUG, "Begin Test!");
    {
        string requestStr = "config=cluster:cluster1,start:0,hit:10";
        RequestPtr requestPtr(new Request);
        requestPtr->setOriginalString(requestStr);
        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(!analyzerClause);
    }
    {
        string originalString= "analyzer=specific_index_analyzer:index1#aliws;"
                               "index2#singlews";
        string requestStr = "config=cluster:cluster1,start:0,hit:10&&" + 
                            originalString + 
                            "&&cluster=cluster1:part_ids=0&&sort=id&&sort=+id";
        RequestPtr requestPtr(new Request);
        requestPtr->setOriginalString(requestStr);
        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_EQ(string("specific_index_analyzer:index1#aliws;"
                        "index2#singlews"), analyzerClause->getOriginalString());
    }
}


#define TEST_SET_AND_GET_AND_STEAL_CLAUSE(className)             \
    {                                                            \
        className *clause = new className();                     \
        requestPtr->set##className(clause);                      \
        className *getClause = requestPtr->get##className();     \
        ASSERT_TRUE(clause == getClause);                     \
        className *stealClause = requestPtr->steal##className(); \
        ASSERT_TRUE(clause == stealClause);                   \
        getClause = requestPtr->get##className();                \
        ASSERT_TRUE(NULL == getClause);                       \
        DELETE_AND_SET_NULL(stealClause);                        \
    }


TEST_F(RequestTest, testGetAndSetAndStealClause) {
    RequestPtr requestPtr(new Request);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(ConfigClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(QueryClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(FilterClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(PKFilterClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(SortClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(DistinctClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(AggregateClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(RankClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(DocIdClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(ClusterClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(HealthCheckClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(AttributeClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(VirtualAttributeClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(FetchSummaryClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(KVPairClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(QueryLayerClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(SearcherCacheClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(OptimizerClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(RankSortClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(FinalSortClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(LevelClause);
    TEST_SET_AND_GET_AND_STEAL_CLAUSE(AnalyzerClause);
}

TEST_F(RequestTest, testGetAndSetDegradeLevel) {
    RequestPtr requestPtr(new Request());
    EXPECT_EQ(multi_call::MIN_PERCENT, requestPtr->_degradeLevel);
    EXPECT_EQ(0, requestPtr->_rankSize);
    EXPECT_EQ(0, requestPtr->_rerankSize);

    {
        requestPtr->setDegradeLevel(0.3f, 100, 200);
        float level;
        uint32_t rankSize;
        uint32_t rerankSize;
        requestPtr->getDegradeLevel(level, rankSize, rerankSize);

        EXPECT_EQ(0.3f, level);
        EXPECT_EQ(100, rankSize);
        EXPECT_EQ(200, rerankSize);
    }
}

TEST_F(RequestTest, testSerializeWithoutAuxQuery) {
    Request request;
    string originalString = "config = cluster:daogou && "
                            "query ='book' && "
                            "rank=rank_profile:mlr && "
                            "filter=rank_score>60 && "
                            "pkfilter=aaa && "
                            "cluster=daogou:part_ids=0 && "
                            "healthcheck=check:true,check_times:3 && "
                            "attribute=attr1,attr2 && "
                            "virtual_attribute=var1:attr1,var2:attr2 &&"
                            "fetch_summary=cluster1_3_3_5_6 &&"
                            "final_sort=sort_name:taobao_sort;"
                            "sort_params:key1|value1,key2|value2 &&"
                            "analyzer=index1:aliws;index2:singlews;index3:noTokenize &&"
	                    "aux_query=store_id:123 AND title:abc &&"
	                    "aux_filter=store_price>100"
	;

    request.setOriginalString(originalString);
    DataBuffer dataBuffer;    
    request.serializeWithoutAuxQuery(dataBuffer);

    Request request2;
    request2.deserialize(dataBuffer);
    string expected = "config=cluster:daogou&&filter=rank_score>60&&pkfilter=aaa&&query='book'&&rank=rank_profile:mlr&&cluster=daogou:part_ids=0&&healthcheck=check:true,check_times:3&&attribute=attr1,attr2&&virtual_attribute=var1:attr1,var2:attr2&&fetch_summary=&&final_sort=sort_name:taobao_sort;sort_params:key1|value1,key2|value2";
    EXPECT_EQ(expected, request2.getOriginalString());
}

END_HA3_NAMESPACE(common);
